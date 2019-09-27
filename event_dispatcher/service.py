
import threading

from opentracing.ext import tags

from event_service_utils.services.tracer import BaseTracerService, EVENT_ID_TAG
from event_service_utils.tracing.jaeger import init_tracer


class EventDispatcher(BaseTracerService):
    def __init__(self,
                 service_stream_key, service_cmd_key,
                 stream_factory,
                 logging_level,
                 tracer_configs):

        tracer = init_tracer(self.__class__.__name__, **tracer_configs)
        super(EventDispatcher, self).__init__(
            name=self.__class__.__name__,
            service_stream_key=service_stream_key,
            service_cmd_key=service_cmd_key,
            stream_factory=stream_factory,
            logging_level=logging_level,
            tracer=tracer,
        )
        del self.service_stream
        self.events_consumer_group_name = f'cg-{service_stream_key}'
        # always have EVENT_DISPATCHER_STREAM_KEY as a input stream source
        # and also the namespace buffers key as inputs as well
        # self.stream_sources = set({service_stream_key})
        self.stream_to_publisher_id_map = {service_stream_key: None}
        self.publisher_id_to_control_flow_map = {}
        self.all_events_consumer_group = None
        self._update_all_events_consumer_group()

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

    def update_control_flow(self, control_flow):
        # control_flow = {
        #     'publisher1': [
        #         ['dest1', 'dest2'],
        #         ['dest3']
        #     ],
        #     'publisher2': [
        #         ['dest1', 'dest2'],
        #         ['dest3']
        #     ]
        # }
        for publisher_id, publisher_control_flow in control_flow.items():
            self.publisher_id_to_control_flow_map[publisher_id] = publisher_control_flow

    def add_buffer_stream_key(self, key, publisher_id):
        self.stream_to_publisher_id_map[key] = publisher_id
        self._update_all_events_consumer_group()

    def del_buffer_stream_key(self, key):
        if key in self.stream_to_publisher_id_map:
            del self.stream_to_publisher_id_map[key]
        self._update_all_events_consumer_group()

    def process_action(self, action, event_data, json_msg):
        super(EventDispatcher, self).process_action(action, event_data, json_msg)
        if action == 'updateControlFlow':
            control_flow = event_data['control_flow']
            self.update_control_flow(control_flow)
        elif action == 'addBufferStreamKey':
            key = event_data['buffer_stream_key']
            publisher_id = event_data['publisher_id']
            self.add_buffer_stream_key(key, publisher_id)
        elif action == 'delBufferStreamKey':
            key = event_data['buffer_stream_key']
            self.del_buffer_stream_key(key)

    def log_state(self):
        super(EventDispatcher, self).log_state()
        self._log_dict('Stream Sources to Publisher Ids', self.stream_to_publisher_id_map)
        self._log_dict('Publisher IDs control flow', self.publisher_id_to_control_flow_map)

    def log_dispatched_events(self, event_data, control_flow):
        self.logger.debug(f'Dispatching event | {event_data} | to => {control_flow}')

    def get_destination_streams(self, destination):
        return self.stream_factory.create(destination, stype='streamOnly')

    def dispatch(self, event_data, control_flow):
        """
        Create a event message with the informations about the data-flow (all destinations).
        that is, a data-flow field, with all the data-flow for this event
        And the path of the event (which should start empty,
            and be filled whenever it passes through an data-flow point)
        """
        if len(control_flow) == 0:
            return
        next_step = control_flow[0]
        data_flow = control_flow
        event_data.update({
            'data_flow': data_flow,
            'data_path': [],
        })

        for destination in next_step:
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

    def dispatch_with_tracer(self, event_data, control_flow):
        self.event_trace_for_method_with_event_data(
            method=self.dispatch,
            method_args=(),
            method_kwargs={
                'event_data': event_data,
                'control_flow': control_flow
            },
            get_event_tracer=True,
            tracer_tags={
                EVENT_ID_TAG: event_data['id'],
                tags.SPAN_KIND: tags.SPAN_KIND_CONSUMER,
            }
        )

    def process_data(self):
        stream_sources_events = list(self.all_events_consumer_group.read_stream_events_list(count=1))
        if stream_sources_events:
            self.logger.debug(f'Processing DATA.. {stream_sources_events}')

        for stream_key_bytes, event_list in stream_sources_events:
            stream_key = stream_key_bytes
            if type(stream_key_bytes) == bytes:
                stream_key = stream_key_bytes.decode('utf-8')
            control_flow = self.get_control_flow_for_stream_key(stream_key)
            for event_tuple in event_list:
                event_id, json_msg = event_tuple
                event_data = self.default_event_deserializer(json_msg)

                try:
                    assert 'id' in event_data, "'id' field should always be present in all events"
                except Exception as e:
                    self.logger.exception(e)
                    self.logger.info(f'Ignoring bad event data: {event_data}')
                else:
                    self.log_dispatched_events(event_data, control_flow)
                    self.dispatch_with_tracer(event_data, control_flow)

    def run(self):
        super(EventDispatcher, self).run()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.data_thread = threading.Thread(target=self.run_forever, args=(self.process_data,))
        self.cmd_thread.start()
        self.data_thread.start()
        self.cmd_thread.join()
        self.data_thread.join()
