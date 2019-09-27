#!/usr/bin/env python
import json
import uuid
from event_service_utils.streams.redis import RedisStreamFactory

from event_dispatcher.conf import (
    REDIS_ADDRESS,
    REDIS_PORT,
    SERVICE_CMD_KEY,
)


def make_dict_key_bites(d):
    return {k.encode('utf-8'): v for k, v in d.items()}


def new_event_msg(event_data):
    event_data.update({'id': str(uuid.uuid4())})
    return {'event': json.dumps(event_data)}


def new_action_msg(action, event_data):
    event_data['action'] = action
    event_data.update({'id': str(uuid.uuid4())})
    return {'event': json.dumps(event_data)}


def send_cmds(service_cmd):
    msg_1 = new_action_msg(
        'updateControlFlow',
        {
            'control_flow': {
                '44d7985a-e41e-4d02-a772-a8f7c1c69124': [
                    ['person-detection-data'],
                    ['car-detection-data'],
                    ['dog-detection-data'],
                    ['gb-data']
                ],
                # 'publisher2': [
                #     ['dest1'],
                #     ['dest3'],
                #     ['graph-builder']
                # ]
            }
        }
    )
    msg_2 = new_action_msg(
        'addBufferStreamKey',
        {
            'buffer_stream_key': 'd9a090de77d717758c950aa987602fe4',
            'publisher_id': '44d7985a-e41e-4d02-a772-a8f7c1c69124'
        }
    )
    msg_3 = new_action_msg(
        'addBufferStreamKey',
        {
            'buffer_stream_key': 'buffer2',
            'publisher_id': 'publisher2'

        }
    )
    msg_4 = new_action_msg(
        'addBufferStreamKey',
        {
            'buffer_stream_key': 'buffer3',
            'publisher_id': 'publisher2'

        }
    )

    msg_5 = new_action_msg(
        'delBufferStreamKey',
        {
            'buffer_stream_key': 'buffer3',
        }
    )

    import ipdb; ipdb.set_trace()
    print(f'Sending msg {msg_1}')
    service_cmd.write_events(msg_1)
    print(f'Sending msg {msg_2}')
    service_cmd.write_events(msg_2)
    # print(f'Sending msg {msg_3}')
    # service_cmd.write_events(msg_3)
    # print(f'Sending msg {msg_4}')
    # service_cmd.write_events(msg_4)
    # print(f'Sending msg {msg_5}')
    # service_cmd.write_events(msg_5)


def send_msgs(service_stream, publisher_id):
    msg_1 = new_event_msg(
        {
            'publisher_id': publisher_id,
            'information': f'new msg in {service_stream.key}',
        }
    )

    print(f'Sending msg {msg_1}')
    service_stream.write_events(msg_1)


def main():
    stream_factory = RedisStreamFactory(host=REDIS_ADDRESS, port=REDIS_PORT)
    service_cmd = stream_factory.create(SERVICE_CMD_KEY, stype='streamOnly')
    buffer_1 = stream_factory.create('d9a090de77d717758c950aa987602fe4', stype='streamOnly')
    buffer_2 = stream_factory.create('buffer2', stype='streamOnly')
    send_cmds(service_cmd)
    import ipdb; ipdb.set_trace()
    send_msgs(buffer_1, '44d7985a-e41e-4d02-a772-a8f7c1c69124')
    send_msgs(buffer_2, 'publisher2')


if __name__ == '__main__':
    main()
