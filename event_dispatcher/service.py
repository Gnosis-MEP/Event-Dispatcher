import threading

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.event_driven import BaseEventDrivenCMDService
from event_service_utils.tracing.jaeger import init_tracer


class EventDispatcher(BaseEventDrivenCMDService):
    def __init__(self,
                 service_stream_key, service_cmd_key_list,
                 pub_event_list, service_details,
                 stream_factory,
                 logging_level,
                 tracer_configs):

        tracer = init_tracer(self.__class__.__name__, **tracer_configs)
        super(EventDispatcher, self).__init__(
            name=self.__class__.__name__,
            service_stream_key=service_stream_key,
            service_cmd_key_list=service_cmd_key_list,
            pub_event_list=pub_event_list,
            service_details=service_details,
            stream_factory=stream_factory,
            logging_level=logging_level,
            tracer=tracer,
        )
        del self.service_stream
        self.service_stream_key = service_stream_key
        self.events_consumer_group_name = f'cg-{service_stream_key}'
        # always have EVENT_DISPATCHER_STREAM_KEY as a input stream source
        # and also the namespace buffers key as inputs as well
        # self.stream_sources = set({service_stream_key})
        self.stream_to_publisher_id_map = {service_stream_key: None}
        self.publisher_id_to_control_flow_map = {}
        self.all_events_consumer_group = None
        self._update_all_events_consumer_group()
        self.cmd_validation_fields = ['id']
        self.data_validation_fields = ['id', 'publisher_id']

    def _update_all_events_consumer_group(self):
        if len(self.stream_to_publisher_id_map.keys()) == 0:
            self.all_events_consumer_group = None
            return self.all_events_consumer_group
        else:
            self.all_events_consumer_group = self.stream_factory.create(
                key=list(self.stream_to_publisher_id_map.keys()), stype='manyKeyConsumer')
            # block only for 1 ms, this way if a new stream is added to the group
            # it should wait at most for 1 ms before reading and considering this new stream
            self.all_events_consumer_group.block = 1
            return self.all_events_consumer_group

    def add_buffer_stream_key(self, key, publisher_id):
        self.stream_to_publisher_id_map[key] = publisher_id
        self._update_all_events_consumer_group()

    def del_buffer_stream_key(self, key):
        if key in self.stream_to_publisher_id_map:
            del self.stream_to_publisher_id_map[key]
        self._update_all_events_consumer_group()

    def update_publisher_service_chain(self, publisher_id, service_chain):

        # right now cant really handle well parallel processing
        # (so only consider that service chain) will be of a list of one service for each element
        self.publisher_id_to_control_flow_map[publisher_id] = [[s] for s in service_chain]

    def process_event_type(self, event_type, event_data, json_msg):
        if not super(EventDispatcher, self).process_event_type(event_type, event_data, json_msg):
            return False

        if event_type == 'QueryCreated':
            buffer_stream = event_data['buffer_stream']
            key = buffer_stream['buffer_stream_key']
            publisher_id = buffer_stream['publisher_id']
            service_chain = event_data['service_chain']
            self.add_buffer_stream_key(key, publisher_id)
            self.update_publisher_service_chain(publisher_id, service_chain)
        elif event_type == 'QueryRemoved':
            buffer_stream = event_data['buffer_stream']
            key = buffer_stream['buffer_stream_key']
            self.del_buffer_stream_key(key)

    def log_state(self):
        super(EventDispatcher, self).log_state()
        self._log_dict('Stream Sources to Publisher Ids', self.stream_to_publisher_id_map)
        self._log_dict('Publisher IDs control flow', self.publisher_id_to_control_flow_map)

    def log_dispatched_events(self, event_data, control_flow):
        self.logger.debug(f'Dispatching event | {event_data} | to => {control_flow}')

    def get_destination_streams(self, destination):
        return self.stream_factory.create(destination, stype='streamOnly')

    def dispatch(self, event_data):
        """
        Start by dispatch the event to the correct next destinations,
        and add the Event Dispatcher as the last service the event has passed through.
        If it has no data_flow, then sent it to the scheduler.
        """
        data_flow = event_data.get('data_flow', [])
        if data_flow is None or len(data_flow) == 0:
            scheduler_stream = self.get_destination_streams('sc-data')
            self.write_event_with_trace(event_data, scheduler_stream)
            return
        data_path = event_data.get('data_path', [])

        data_path.append(self.service_stream_key)
        next_data_flow_i = len(data_path)
        if next_data_flow_i >= len(data_flow):
            self.logger.info(f'Ignoring event without a next destination available: {event_data}')
            return

        next_destinations = data_flow[next_data_flow_i]
        for destination in next_destinations:
            destination_stream = self.get_destination_streams(destination)
            self.write_event_with_trace(event_data, destination_stream)

    def get_control_flow_for_stream_key(self, stream_key):
        """
        Should return a control step list,
        where each step is a list of the necessary destinations to be sent to in parallel.
        Eg:
            [
                ['dest1-stream'],  # first destinations is dest1-stream
                ['dest2-stream', 'dest3-stream'], # later on go through dest2-stream and dest3-stream in parallel.
            ]
        """
        publisher_id = self.stream_to_publisher_id_map.get(stream_key)
        return self.publisher_id_to_control_flow_map.get(publisher_id, [])

    @timer_logger
    def process_data_event(self, event_data, json_msg):
        if not super(EventDispatcher, self).process_data_event(event_data, json_msg):
            return False
        # stream_key = event_data['buffer_stream_key']
        # control_flow = self.get_control_flow_for_stream_key(stream_key)
        # hacky hack. Hardcoded the scheduler data stream.
        control_flow = event_data.get('data_flow')
        if control_flow is None:
            control_flow = ['sc-data']
        self.log_dispatched_events(event_data, control_flow)
        self.dispatch(event_data)

    def process_data(self):
        stream_sources_events = list(self.all_events_consumer_group.read_stream_events_list(count=1))
        if stream_sources_events:
            self.logger.debug(f'Processing DATA.. {stream_sources_events}')

        for stream_key_bytes, event_list in stream_sources_events:
            stream_key = stream_key_bytes
            if type(stream_key_bytes) == bytes:
                stream_key = stream_key_bytes.decode('utf-8')
            for event_tuple in event_list:
                event_id, json_msg = event_tuple
                try:
                    event_data = self.default_event_deserializer(json_msg)
                    if 'buffer_stream_key' not in event_data:
                        event_data.update({
                            'buffer_stream_key': stream_key
                        })
                    self.process_data_event_wrapper(event_data, json_msg)
                except Exception as e:
                    self.logger.error(f'Error processing {json_msg}:')
                    self.logger.exception(e)

    def run(self):
        super(EventDispatcher, self).run()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.data_thread = threading.Thread(target=self.run_forever, args=(self.process_data,))
        self.cmd_thread.start()
        self.data_thread.start()
        self.cmd_thread.join()
        self.data_thread.join()
