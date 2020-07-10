import logging

from pgevents import data_access

LOGGER = logging.getLogger(__name__)


class EventStream:
    def __init__(self, connection, handlers, data_access=data_access):
        self.connection = connection
        self.handlers = handlers
        self.data_access = data_access

    @property
    def topics(self):
        return list(self.handlers.keys())

    def process(self):
        while self.process_next():
            pass

    def process_next(self):
        with self.connection:
            with self.connection.cursor() as cursor:
                event = self.get_next(cursor)
                if not event:
                    return False
                handler = self.handlers[event.topic]
                handler(event)
                event.mark_processed(cursor)
                return True

    def get_next(self, cursor):
        data = self.data_access.get_next_event(cursor, self.topics)
        if not data:
            LOGGER.info("No more events to process")
            return None
        return Event(data["id"], data["topic"], data["payload"])

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.connection == other.connection
            and self.handlers == other.handlers
        )


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
