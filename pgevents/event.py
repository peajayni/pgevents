import logging

from pgevents import data_access

LOGGER = logging.getLogger(__name__)


class Event:
    PENDING = "PENDING"
    PROCESSED = "PROCESSED"

    def __init__(self, id, topic, payload, data_access=data_access):
        self.id = id
        self.topic = topic
        self.payload = payload
        self.data_access = data_access

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.id == other.id

    def mark_processed(self, cursor):
        self.data_access.mark_event_processed(cursor, self.id)
