#!/usr/bin/env python
from event_service_utils.streams.redis import RedisStreamFactory

from event_dispatcher.service import EventDispatcher

from event_dispatcher.conf import (
    REDIS_ADDRESS,
    REDIS_PORT,
    SERVICE_STREAM_KEY,
    SERVICE_CMD_KEY,
    LOGGING_LEVEL
)


def run_service():
    stream_factory = RedisStreamFactory(host=REDIS_ADDRESS, port=REDIS_PORT)
    service = EventDispatcher(
        service_stream_key=SERVICE_STREAM_KEY,
        service_cmd_key=SERVICE_CMD_KEY,
        stream_factory=stream_factory,
        logging_level=LOGGING_LEVEL
    )
    service.run()


def main():
    try:
        run_service()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
