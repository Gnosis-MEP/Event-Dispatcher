import copy
import unittest
from unittest.mock import patch

from event_service_utils.tests.base_test_case import MockedServiceStreamTestCase
from event_service_utils.tests.json_msg_helper import prepare_event_msg_tuple

from mocked_streams import ManyKeyConsumerMockedStreamFactory


class TestMockedStream(unittest.TestCase):
    def setUp(self):
        self.original_stream_sources = {
            'buffer1': [
                prepare_event_msg_tuple({'event_data': 'b1-e1'}),
                prepare_event_msg_tuple({'event_data': 'b1-e2'}),
            ],
            'buffer2': [
                prepare_event_msg_tuple({'event_data': 'b2-e1'}),
                prepare_event_msg_tuple({'event_data': 'b2-e2'}),
                prepare_event_msg_tuple({'event_data': 'b2-e3'})
            ]
        }
        self.stream_sources = copy.deepcopy(self.original_stream_sources)
        self.stream_factory = ManyKeyConsumerMockedStreamFactory(mocked_dict=self.stream_sources)

        self.consumer_group = self.stream_factory.create(key=list(self.stream_sources.keys()), stype='manyKeyConsumer')
        self.consumer_group._update_mocked_values(self.stream_sources)

    def test_mocked_manykeyconsumer_group_should_return_two_list_one_for_each_stream(self):
        stream_sources_events = list(self.consumer_group.read_stream_events_list(count=1))
        self.assertEqual(len(stream_sources_events), 2)
        self.assertEqual(stream_sources_events[0][0], 'buffer1')
        self.assertEqual(stream_sources_events[1][0], 'buffer2')

    def test_mocked_manykeyconsumer_group_should_return_only_one_key_of_each_stream_and_correct(self):
        stream_sources_events = list(self.consumer_group.read_stream_events_list(count=1))
        for stream_key, event_list in stream_sources_events:
            self.assertEqual(len(event_list), 1)
            self.assertEqual(event_list, [self.original_stream_sources[stream_key][0]])
