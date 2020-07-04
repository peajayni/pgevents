import logging

from pgevents import data_access, events

LOGGER = logging.getLogger(__name__)


def always_continue(app):
    return True


class App:
    def __init__(self, dsn, channel):
        self.dsn = dsn
        self.channel = channel

        self.connection = None
        self.event_stream = None
        self.handlers = {}

    def run(self, should_continue=always_continue):
        self.setup()
        try:
            while should_continue(self):
                self.tick()
        finally:
            self.stop_listening()

    def tick(self):
        self.connection.poll()
        if self.connection.notifies:
            LOGGER.debug("Received notification")
            while self.connection.notifies:
                self.connection.notifies.pop()
            self.event_stream.process()

    def setup(self):
        self.connect()
        self.setup_event_stream()
        self.start_listening()

    def connect(self):
        self.connection = data_access.connect(self.dsn)

    def setup_event_stream(self):
        self.event_stream = events.EventStream(self.connection, self.handlers)

    def start_listening(self):
        LOGGER.debug("Starting to listen on channel: %s", self.channel)
        with data_access.cursor(self.connection) as cursor:
            data_access.listen(cursor, self.channel)

    def stop_listening(self):
        LOGGER.debug("Stopping listening on channel: %s", self.channel)
        with data_access.cursor(self.connection) as cursor:
            data_access.unlisten(cursor, self.channel)

    def register(self, topic):
        def decorator(func):
            LOGGER.debug("Registering %s on topic: %s", func, topic)
            self.handlers[topic] = func
            return func

        return decorator

    def unregister(self, topic, func):
        LOGGER.debug("Unregistering %s on topic: %s", func, topic)
        try:
            del self.handlers[topic]
        except KeyError:
            pass
