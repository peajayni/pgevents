import logging
from collections import defaultdict

import psycopg2

from pgevents.event import Event

LOGGER = logging.getLogger(__name__)


class App:
    def __init__(self, dsn):
        self.event_connection = psycopg2.connect(dsn)
        self.notification_connection = psycopg2.connect(dsn)
        self.notification_connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
        )
        self.registry: dict = defaultdict(set)

    def register(self, channel):
        def decorator(func):
            LOGGER.debug("Registering %s on channel: %s", func, channel)
            self._listen(channel)
            self._register(channel, func)
            return func

        return decorator

    def unregister(self, channel, func):
        LOGGER.debug("Unregistering %s on channel: %s", func, channel)
        if self._unregister(channel, func):
            self._unlisten(channel)

    def _listen(self, channel):
        if len(self.registry[channel]) != 0:
            return

        cursor = self.notification_connection.cursor()
        cursor.execute(f"LISTEN {channel}")

    def _unlisten(self, channel):
        if len(self.registry[channel]) != 0:
            return

        cursor = self.notification_connection.cursor()
        cursor.execute(f"UNLISTEN {channel}")

    def _register(self, channel, func):
        self.registry[channel].add(func)

    def _unregister(self, channel, func):
        try:
            self.registry[channel].remove(func)
            return True
        except KeyError:
            return False

    def run(self):
        while True:
            self._tick()

    def _tick(self):
        self.notification_connection.poll()
        while self.notification_connection.notifies:
            LOGGER.debug("Received notification")
            notification = self.notification_connection.notifies.pop(0)
            self._dispatch(notification)

    def _dispatch(self, notification):
        LOGGER.debug(
            "Dispatching notification: pid=%s, channel=%s, payload=%s",
            notification.pid,
            notification.channel,
            notification.payload,
        )
        for handler in self.registry[notification.channel]:
            LOGGER.debug("Dispatching to handler: %s", handler)
            event = Event(payload=notification.payload)
            handler(event)
