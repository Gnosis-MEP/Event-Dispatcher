import threading

from event_service_utils.services.base import BaseService
from event_service_utils.schemas.internal_msgs import (
    BaseInternalMessage,
)
from event_dispatcher.schemas import EventDispatcherBaseEventMessage, DataFlowEventMessage


class EventDispatcher(BaseService):
    def __init__(self,
                 service_stream_key, service_cmd_key,
                 stream_factory,
                 logging_level):

        super(EventDispatcher, self).__init__(
            name=self.__class__.__name__,
            service_stream_key=service_stream_key,
            service_cmd_key=service_cmd_key,
            cmd_event_schema=BaseInternalMessage,
            stream_factory=stream_factory,
            logging_level=logging_level
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
        self._log_dict('Stream Sources and Publisher Ids', self.stream_to_publisher_id_map)

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
        next_step = control_flow[0]
        data_flow = control_flow
        schema = DataFlowEventMessage(
            id=event_data['id'],
            publisher_id=event_data['publisher_id'],
            source=event_data['source'],
            data_flow=data_flow,
            data_path=[],
            event_data=event_data,
        )
        json_msg = schema.json_msg_load_from_dict()
        for destination in next_step:
            self.get_destination_streams(destination).write_events(json_msg)

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
        publisher_id = self.stream_to_publisher_id_map[stream_key]
        return self.publisher_id_to_control_flow_map.get(publisher_id, [])

    def process_data(self):
        stream_sources_events = list(self.all_events_consumer_group.read_stream_events_list(count=1))
        if stream_sources_events:
            self.logger.debug(f'Processing DATA.. {stream_sources_events}')

        for stream_key, event_list in stream_sources_events:
            control_flow = self.get_control_flow_for_stream_key(stream_key)
            for event_tuple in event_list:
                event_id, json_msg = event_tuple
                event_schema = EventDispatcherBaseEventMessage(json_msg=json_msg)
                event_data = event_schema.object_load_from_msg()
                self.log_dispatched_events(event_data, control_flow)
                self.dispatch(event_data, control_flow)

    def run(self):
        super(EventDispatcher, self).run()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.data_thread = threading.Thread(target=self.run_forever, args=(self.process_data,))
        self.cmd_thread.start()
        self.data_thread.start()
        self.cmd_thread.join()
        self.data_thread.join()
