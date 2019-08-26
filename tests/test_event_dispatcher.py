from unittest.mock import patch

from event_service_utils.tests.base_test_case import MockedServiceStreamTestCase
from event_service_utils.tests.json_msg_helper import prepare_event_msg_tuple

from event_dispatcher.service import EventDispatcher

from event_dispatcher.conf import (
    SERVICE_STREAM_KEY,
    SERVICE_CMD_KEY,
)


class TestEventDispatcher(MockedServiceStreamTestCase):
    GLOBAL_SERVICE_CONFIG = {
        'service_stream_key': SERVICE_STREAM_KEY,
        'service_cmd_key': SERVICE_CMD_KEY,
        'logging_level': 'ERROR'
    }
    SERVICE_CLS = EventDispatcher
    MOCKED_STREAMS_DICT = {
        SERVICE_STREAM_KEY: [],
        SERVICE_CMD_KEY: [],
    }

    @patch('event_dispatcher.service.EventDispatcher.process_action')
    def test_process_cmd_should_call_process_action(self, mocked_process_action):
        action = 'someAction'
        event_data = {
            'action': action,
            'some': 'stuff'
        }
        msg_tuple = prepare_event_msg_tuple(event_data)

        self.service.service_cmd.mocked_values = [msg_tuple]
        self.service.process_cmd()
        self.assertTrue(mocked_process_action.called)
        self.service.process_action.assert_called_once_with(action, event_data, msg_tuple[1])
