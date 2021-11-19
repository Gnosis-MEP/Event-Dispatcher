from unittest.mock import patch, MagicMock

from event_service_utils.tests.base_test_case import MockedEventDrivenServiceStreamTestCase
from event_service_utils.tests.json_msg_helper import prepare_event_msg_tuple

from mocked_streams import ManyKeyConsumerMockedStreamFactory


from event_dispatcher.service import EventDispatcher

from event_dispatcher.conf import (
    SERVICE_STREAM_KEY,
    SERVICE_CMD_KEY_LIST,
    SERVICE_DETAILS,
    PUB_EVENT_LIST,
)


class TestEventDispatcher(MockedEventDrivenServiceStreamTestCase):
    GLOBAL_SERVICE_CONFIG = {
        'service_stream_key': SERVICE_STREAM_KEY,
        'service_cmd_key_list': SERVICE_CMD_KEY_LIST,
        'pub_event_list': PUB_EVENT_LIST,
        'service_details': SERVICE_DETAILS,
        'logging_level': 'ERROR',
        'tracer_configs': {'reporting_host': None, 'reporting_port': None},
    }
    SERVICE_CLS = EventDispatcher

    MOCKED_CG_STREAM_DICT = {

    }
    MOCKED_STREAMS_DICT = {
        SERVICE_STREAM_KEY: [],
        'cg-EventDispatcher': MOCKED_CG_STREAM_DICT,
    }

    def _mocked_event_message(self, schema_cls, **kwargs):
        schema = schema_cls(**kwargs)
        return prepare_event_msg_tuple(schema.dict)

    def prepare_mocked_stream_factory(self, mocked_dict):
        self.stream_factory = ManyKeyConsumerMockedStreamFactory(mocked_dict=self.mocked_streams_dict)

    @patch('event_dispatcher.service.EventDispatcher.process_event_type')
    def test_process_cmd_should_call_process_event_type(self, mocked_process_event_type):
        event_type = 'SomeEventType'
        unicode_event_type = event_type.encode('utf-8')
        event_data = {
            'id': 1,
            'action': event_type,
            'some': 'stuff'
        }
        msg_tuple = prepare_event_msg_tuple(event_data)
        mocked_process_event_type.__name__ = 'process_event_type'

        self.service.service_cmd.mocked_values_dict = {
            unicode_event_type: [msg_tuple]
        }
        self.service.process_cmd()
        self.assertTrue(mocked_process_event_type.called)
        self.service.process_event_type.assert_called_once_with(event_type=event_type, event_data=event_data, json_msg=msg_tuple[1])

    @patch('event_dispatcher.service.EventDispatcher.update_publisher_service_chain')
    @patch('event_dispatcher.service.EventDispatcher.add_buffer_stream_key')
    def test_process_event_type_should_call_necessary_methods_correctly(self, mocked_add_buffer_stream_key, mocked_update_pub_chain):
        event_data = {
            'id': 1,
            'subscriber_id': 'subscriber_id',
            'query_id': 'query_id',
            'parsed_query': {
                'from': ['publisher_id'],
                'content': ['ObjectDetection', 'ColorDetection'],
                'window': {
                    'window_type': 'TUMBLING_COUNT_WINDOW',
                    'args': [2]
                }
            },
            'buffer_stream': {
                'publisher_id': 'publisher_id',
                'buffer_stream_key': 'buffer_stream_key',
                'source': 'source',
                'resolution': "300x900",
                'fps': "100",
            },
            'service_chain': ['service_chain'],
        }
        event_type = 'QueryCreated'
        json_msg = prepare_event_msg_tuple(event_data)[1]
        self.service.process_event_type(event_type, event_data, json_msg)

        mocked_add_buffer_stream_key.assert_called_once_with(
            'buffer_stream_key',
            'publisher_id'
        )
        mocked_update_pub_chain.assert_called_once_with(
            'publisher_id',
            ['service_chain']
        )

    @patch('event_dispatcher.service.EventDispatcher.del_buffer_stream_key')
    def test_process_action_should_call_del_buffer_stream_key(self, mocked_del_buffer_stream_key):
        event_data = {
            'id': 1,
            'subscriber_id': 'subscriber_id',
            'query_id': 'query_id',
            'parsed_query': {
                'from': ['publisher_id'],
                'content': ['ObjectDetection', 'ColorDetection'],
                'window': {
                    'window_type': 'TUMBLING_COUNT_WINDOW',
                    'args': [2]
                }
            },
            'buffer_stream': {
                'publisher_id': 'publisher_id',
                'buffer_stream_key': 'buffer_stream_key',
                'source': 'source',
                'resolution': "300x900",
                'fps': "100",
            },
            'service_chain': ['service_chain'],
            'deleted': True,
        }
        event_type = 'QueryRemoved'
        json_msg = prepare_event_msg_tuple(event_data)[1]
        self.service.process_event_type(event_type, event_data, json_msg)

        mocked_del_buffer_stream_key.assert_called_once_with(
            'buffer_stream_key',
        )

    @patch('event_dispatcher.service.EventDispatcher._update_all_events_consumer_group')
    def test_add_buffer_stream_key_should_call_update_all_events_consumer_group(self, mocked_update):
        publisher_id = 'publisher1'
        self.service.add_buffer_stream_key('unique-buffer-key', publisher_id)
        self.assertTrue(mocked_update.called)
        mocked_update.assert_called_once()

    def test_update_all_events_consumer_group_should_set_block_to_1ms(self):
        self.service._update_all_events_consumer_group()
        self.assertEqual(self.service.all_events_consumer_group.block, 1)

    def test_update_all_events_consumer_group_should_have_same_keys_as_stream_to_publisher_id_map(self):
        stream_to_publisher_id_map = {
            SERVICE_STREAM_KEY: None,
            'some-stream': 'publisher1',
            'another-stream': 'publisher2'
        }
        self.service.stream_to_publisher_id_map = stream_to_publisher_id_map
        self.service._update_all_events_consumer_group()
        self.assertEqual(self.service.all_events_consumer_group.keys, list(stream_to_publisher_id_map.keys()))

        del self.service.stream_to_publisher_id_map['some-stream']
        self.service._update_all_events_consumer_group()
        self.assertEqual(self.service.all_events_consumer_group.keys, list(stream_to_publisher_id_map.keys()))

        self.service.stream_to_publisher_id_map['some-other-stream'] = 'publisher3'
        self.service._update_all_events_consumer_group()
        self.assertEqual(self.service.all_events_consumer_group.keys, list(stream_to_publisher_id_map.keys()))

    def test_update_publisher_service_chain_should_change_service_chain_for_the_given_query_publisher(self):
        self.service.publisher_id_to_control_flow_map = {
            'publisher-id-1': [
                ['model-a'],
                ['graph-builder']
            ],
            'publisher-id-2': [
                ['model-b'],
                ['graph-builder']
            ]
        }

        expected_dict = {
            'publisher-id-1': [
                ['model-a'],
                ['graph-builder']
            ],
            'publisher-id-2': [
                ['model-b'],
                ['graph-builder']
            ],
            'publisher-id-3': [
                ['model-a'],
                ['graph-builder']
            ]
        }
        service_chain = [
            'model-a',
            'graph-builder'
        ]
        publisher_id = 'publisher-id-3'
        self.service.update_publisher_service_chain(publisher_id, service_chain)
        self.assertDictEqual(expected_dict, self.service.publisher_id_to_control_flow_map)

    def test_get_control_flow_for_stream_key_should_return_correct_control_flow(self):
        stream_key = 'stream'
        publisher_id = 'publisher1'
        expected_control_flow = [['dest1'], ['dest2']]

        self.service.stream_to_publisher_id_map[stream_key] = publisher_id
        self.service.publisher_id_to_control_flow_map[publisher_id] = expected_control_flow
        control_flow = self.service.get_control_flow_for_stream_key(stream_key)
        self.assertListEqual(expected_control_flow, control_flow)
