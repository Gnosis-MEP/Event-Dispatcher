from unittest.mock import patch

from event_service_utils.tests.base_test_case import MockedServiceStreamTestCase
from event_service_utils.tests.json_msg_helper import prepare_event_msg_tuple

from mocked_streams import ManyKeyConsumerMockedStreamFactory

from event_dispatcher.service import EventDispatcher

from event_dispatcher.schemas import (
    EventDispatcherBaseEventMessage,
    DataFlowEventMessage
)

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

    def _mocked_event_message(self, schema_cls, **kwargs):
        schema = schema_cls(**kwargs)
        return prepare_event_msg_tuple(schema.dict)

    def prepare_mocked_stream_factory(self, mocked_dict):
        self.stream_factory = ManyKeyConsumerMockedStreamFactory(mocked_dict=self.mocked_streams_dict)

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

    @patch('event_dispatcher.service.EventDispatcher.add_buffer_stream_key')
    def test_process_action_should_call_add_buffer_stream_key(self, mocked_add_buffer_stream_key):
        action = 'addBufferStreamKey'
        query_data = {
            'buffer_stream_key': 'unique-buffer-key',
        }
        event_data = query_data.copy()
        event_data.update({
            'action': action,
        })
        msg_tuple = prepare_event_msg_tuple(event_data)

        self.service.service_cmd.mocked_values = [msg_tuple]
        self.service.process_cmd()
        self.assertTrue(mocked_add_buffer_stream_key.called)
        mocked_add_buffer_stream_key.assert_called_once_with(
            query_data['buffer_stream_key'],
        )

    @patch('event_dispatcher.service.EventDispatcher.del_buffer_stream_key')
    def test_process_action_should_call_del_buffer_stream_key(self, mocked_del_buffer_stream_key):
        action = 'delBufferStreamKey'
        query_data = {
            'buffer_stream_key': 'unique-buffer-key',
        }
        event_data = query_data.copy()
        event_data.update({
            'action': action,
        })
        msg_tuple = prepare_event_msg_tuple(event_data)

        self.service.service_cmd.mocked_values = [msg_tuple]
        self.service.process_cmd()
        self.assertTrue(mocked_del_buffer_stream_key.called)
        mocked_del_buffer_stream_key.assert_called_once_with(
            query_data['buffer_stream_key'],
        )

    @patch('event_dispatcher.service.EventDispatcher._update_all_events_consumer_group')
    def test_add_buffer_stream_key_should_call_update_all_events_consumer_group(self, mocked_update):
        self.service.add_buffer_stream_key('unique-buffer-key')
        self.assertTrue(mocked_update.called)
        mocked_update.assert_called_once()

    def test_update_all_events_consumer_group_should_set_block_to_1ms(self):
        self.service._update_all_events_consumer_group()
        self.assertEqual(self.service.all_events_consumer_group.block, 1)

    def test_update_all_events_consumer_group_should_have_same_keys_as_stream_sources(self):
        stream_sources = set({SERVICE_STREAM_KEY, 'some-stream', 'another-stream'})
        self.service.stream_sources = stream_sources
        self.service._update_all_events_consumer_group()
        self.assertEqual(self.service.all_events_consumer_group.keys, list(stream_sources))

        self.service.stream_sources.remove('some-stream')
        self.service._update_all_events_consumer_group()
        self.assertEqual(self.service.all_events_consumer_group.keys, list(stream_sources))

        self.service.stream_sources.add('some-other-stream')
        self.service._update_all_events_consumer_group()
        self.assertEqual(self.service.all_events_consumer_group.keys, list(stream_sources))

    @patch('event_dispatcher.service.EventDispatcher.get_control_flow_for_stream_key')
    @patch('event_dispatcher.service.EventDispatcher.dispatch')
    def test_process_data_should_call_dispatch_events_and_ge_control_flow(self, mocked_dispatch, mocked_control_flow):
        mocked_stream_sources = {
            'buffer1': [
                self._mocked_event_message(
                    EventDispatcherBaseEventMessage, id='1', publisher_id='publisher_id1', source='source1'
                ),
                self._mocked_event_message(
                    EventDispatcherBaseEventMessage, id='2', publisher_id='publisher_id1', source='source1'
                ),
            ],
            'buffer2': [
                self._mocked_event_message(
                    EventDispatcherBaseEventMessage, id='3', publisher_id='publisher_id2', source='source1'
                ),
                self._mocked_event_message(
                    EventDispatcherBaseEventMessage, id='4', publisher_id='publisher_id2', source='source1'
                ),
                self._mocked_event_message(
                    EventDispatcherBaseEventMessage, id='5', publisher_id='publisher_id2', source='source1'
                ),
            ]
        }
        self.service.stream_sources = set(mocked_stream_sources.keys())
        self.service._update_all_events_consumer_group()
        self.service.all_events_consumer_group._update_mocked_values(mocked_stream_sources)

        mocked_control_flow.return_value = []
        self.service.process_data()
        self.assertTrue(mocked_dispatch.called)
        self.assertEqual(mocked_dispatch.call_count, 2)
        self.assertTrue(mocked_control_flow.called)
        self.assertEqual(mocked_dispatch.call_count, 2)

    def test_dispatch_should_write_events_to_next_step_streams_in_control_flow(self):
        input_event_schema = EventDispatcherBaseEventMessage(id='1', publisher_id='publisher_id1', source='source1')
        event_data = input_event_schema.dict.copy()
        self.stream_factory.mocked_dict['dest1'] = []
        self.stream_factory.mocked_dict['dest2'] = []
        # mocked_destination_streams.side_effect = lambda x: self.stream_factory.create(x)
        control_flow = [['dest1', 'dest2'], ['dest3'], ['dest4']]
        original_stream_keys = set(self.stream_factory.mocked_dict.keys())
        expected_stream_keys = original_stream_keys.copy()
        expected_stream_keys.add('dest1')
        expected_stream_keys.add('dest2')
        expected_event_msg = {
            'event': (
                '{"id": "1", "publisher_id": "publisher_id1", "source": "source1", '
                '"data_flow": [["dest1", "dest2"], ["dest3"], ["dest4"]], "data_path": []}'
            )
        }

        self.service.dispatch(event_data, control_flow)
        final_stream_keys = set(self.stream_factory.mocked_dict.keys())
        self.assertEqual(expected_stream_keys, final_stream_keys)
        self.assertListEqual(self.stream_factory.mocked_dict['dest1'], [expected_event_msg])
        self.assertListEqual(self.stream_factory.mocked_dict['dest2'], [expected_event_msg])

    @patch('event_dispatcher.service.EventDispatcher.update_controlflow')
    def test_process_action_should_call_update_controlflow(self, mocked_update_controlflow):
        action = 'updateControlFlow'
        query_data = {
            'control_flow': []
        }
        event_data = query_data.copy()
        event_data.update({
            'action': action,
        })
        msg_tuple = prepare_event_msg_tuple(event_data)

        self.service.service_cmd.mocked_values = [msg_tuple]
        self.service.process_cmd()
        self.assertTrue(mocked_update_controlflow.called)
        mocked_update_controlflow.assert_called_once_with(
            query_data['control_flow'],
        )
