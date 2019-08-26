import threading

from event_service_utils.services.base import BaseService
from event_service_utils.schemas.internal_msgs import (
    BaseInternalMessage,
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

    def process_data(self):
        self.logger.debug('Processing DATA..')
        if not self.service_stream:
            return
        event_list = self.service_stream.read_events(count=1)
        for event_tuple in event_list:
            event_id, json_msg = event_tuple
            self.logger.debug(f'Processing new data: {json_msg}')
            # do something with json_msg

    def process_action(self, action, event_data, json_msg):
        super(EventDispatcher, self).process_action(action, event_data, json_msg)
        if action == 'someAction':
            # do some action
            pass
        elif action == 'otherAction':
            # do some other action
            pass

    def log_state(self):
        super(EventDispatcher, self).log_state()
        self.logger.info(f'My service name is: {self.name}')

    def run(self):
        super(EventDispatcher, self).run()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.data_thread = threading.Thread(target=self.run_forever, args=(self.process_data,))
        self.cmd_thread.start()
        self.data_thread.start()
        self.cmd_thread.join()
        self.data_thread.join()
