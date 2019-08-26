import threading

from event_service_utils.services.base import BaseService
from event_service_utils.schemas.internal_msgs import (
    BaseInternalMessage,
)
from event_service_utils.schemas.events import (
    BaseEventMessage,
)


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
        self.stream_sources = set({service_stream_key})
        self.all_events_consumer_group = None
        self._update_all_events_consumer_group()

    def _update_all_events_consumer_group(self):
        if len(self.stream_sources) == 0:
            self.all_events_consumer_group = None
        else:
            self.all_events_consumer_group = self.stream_factory.redis_db.consumer_group(
                self.events_consumer_group_name,
                list(self.stream_sources)
            )
            self.all_events_consumer_group.create()

        # block only for 10 ms, this way if a new stream is added to the group
        # it should wait at most for 10 ms before reading considering this new stream
        self.all_events_consumer_group.block = 10
        return self.all_events_consumer_group

    def add_buffer_stream_key(self, key):
        self.stream_sources.add(key)
        self._update_all_events_consumer_group()

    def del_buffer_stream_key(self, key):
        if key in self.stream_sources:
            self.stream_sources.remove(key)
        self._update_all_events_consumer_group()

    def process_action(self, action, event_data, json_msg):
        super(EventDispatcher, self).process_action(action, event_data, json_msg)
        if action == 'updateControlFlow':
            # do some action
            pass
        elif action == 'addBufferStreamKey':
            key = event_data['buffer_stream_key']
            self.add_bufferstream_key(key)
        elif action == 'delBufferStreamKey':
            key = event_data['buffer_stream_key']
            self.del_buffer_stream_key(key)

    def log_state(self):
        super(EventDispatcher, self).log_state()
        log_msg = '- Stream Sources:'
        for k in self.stream_sources:
            log_msg += f'\n-- {k}'
        self.logger.debug(log_msg)

    def log_dispatched_events(self, event_data, control_flow):
        self.logger.debug(f'Dispatching event | | to => ')

    def dispatch(self, event_data, control_flow):
        # return self.get_destination_stream(destination).write_events(event)
        pass

    def process_data(self):
        self.logger.debug('Processing DATA..')
        stream_sources_events = self.all_events_consumer_group.read(count=1)
        for stream_key, event_list in stream_sources_events:
            control_flow = self.get_control_flow_for_stream_key(stream_key)
            for event_tuple in event_list:
                event_id, json_msg = event_tuple
                event_schema = BaseEventMessage(json_msg=json_msg)
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
