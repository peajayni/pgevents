class Event:
    def __init__(self, payload=None):
        self.payload = payload

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.payload == other.payload
