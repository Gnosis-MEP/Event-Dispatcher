#!/usr/bin/env python
from event_service_utils.streams.redis import RedisStreamFactory
from event_service_utils.schemas.internal_msgs import (
    BaseInternalMessage,
)
from event_service_utils.schemas.events import (
    BaseEventMessage,
)

from event_dispatcher.conf import (
    REDIS_ADDRESS,
    REDIS_PORT,
    SERVICE_STREAM_KEY,
    SERVICE_CMD_KEY,
)


def make_dict_key_bites(d):
    return {k.encode('utf-8'): v for k, v in d.items()}


def new_event_msg(event_data):
    schema = BaseEventMessage()
    schema.dict.update(event_data)
    return schema.json_msg_load_from_dict()

def new_action_msg(action, event_data):
    schema = BaseInternalMessage(action=action)
    schema.dict.update(event_data)
    return schema.json_msg_load_from_dict()


def send_cmds(service_cmd):
    msg_1 = new_action_msg(
        'addBufferStreamKey',
        {
            'buffer_stream_key': 'buffer1',
            'query_ids': ['query1', 'query2']
        }
    )
    msg_2 = new_action_msg(
        'addBufferStreamKey',
        {
            'buffer_stream_key': 'buffer2',
            'query_ids': ['query3']

        }
    )

    msg_3 = new_action_msg(
        'delBufferStreamKey',
        {
            'buffer_stream_key': 'buffer1',
        }
    )

    import ipdb; ipdb.set_trace()
    print(f'Sending msg {msg_1}')
    service_cmd.write_events(msg_1)
    print(f'Sending msg {msg_2}')
    service_cmd.write_events(msg_2)
    print(f'Sending msg {msg_3}')
    service_cmd.write_events(msg_3)


def send_msgs(service_stream):
    msg_1 = new_event_msg(
        {
            'event': f'new msg in {service_stream.key}',
        }
    )

    print(f'Sending msg {msg_1}')
    service_stream.write_events(msg_1)


def main():
    stream_factory = RedisStreamFactory(host=REDIS_ADDRESS, port=REDIS_PORT)
    service_cmd = stream_factory.create(SERVICE_CMD_KEY, stype='streamOnly')
    buffer_1 = stream_factory.create('buffer1', stype='streamOnly')
    buffer_2 = stream_factory.create('buffer2', stype='streamOnly')
    import ipdb; ipdb.set_trace()
    send_cmds(service_cmd)


if __name__ == '__main__':
    main()
