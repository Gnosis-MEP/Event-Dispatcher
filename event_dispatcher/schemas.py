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


class DataFlowEventMessage():

    def __init__(self, id=None, publisher_id=None, source=None, data_flow=None, data_path=None, event_data=None, json_msg=None):
        self.dict = {
            'id': id,
            'publisher_id': publisher_id,
            'source': source,
            'data_flow': data_flow,
            'data_path': data_path
        }
        if event_data:
            for key, value in event_data.items():
                if key not in self.dict:
                    self.dict[key] = value
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
