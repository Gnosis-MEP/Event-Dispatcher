import json


class EventDispatcherBaseEventMessage():

    def __init__(self, id=None, publisher_id=None, source=None, json_msg=None):
        self.dict = {
            'id': id,
            'publisher_id': publisher_id,
            'source': source,
        }
        self.json_serialized = json_msg

    def json_msg_load_from_dict(self):
        if self.dict['id'] is None:
            self.dict['id'] = ''

        self.json_serialized = {
            'event': json.dumps(self.dict)
        }
        return self.json_serialized

    def object_load_from_msg(self):
        event_json = self.json_serialized.get(b'event', '{}')
        self.dict = json.loads(event_json)
        if self.dict['id'] is None:
            self.dict['id'] = ''

        return self.dict


class OutputEventMessage():

    def __init__(self, id=None, publisher_id=None, source=None, destinations=None, json_msg=None):
        self.dict = {
            'id': id,
            'publisher_id': publisher_id,
            'source': source,
            'destinations': destinations
        }
        self.json_serialized = json_msg

    def json_msg_load_from_dict(self):
        if self.dict['id'] is None:
            self.dict['id'] = ''

        self.json_serialized = {
            'event': json.dumps(self.dict)
        }
        return self.json_serialized

    def object_load_from_msg(self):
        event_json = self.json_serialized.get(b'event', '{}')
        self.dict = json.loads(event_json)
        if self.dict['id'] is None:
            self.dict['id'] = ''

        return self.dict
