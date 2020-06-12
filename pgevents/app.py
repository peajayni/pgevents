from collections import defaultdict

import psycopg2


class App:
    def __init__(self, dsn):
        self.connection = psycopg2.connect(dsn)
        self.connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
        )
        self.registry: dict = defaultdict(set)

    def register(self, channel):
        def decorator(func):
            self._listen(channel)
            self._register(channel, func)
            return func

        return decorator

    def unregister(self, channel, func):
        if self._unregister(channel, func):
            self._unlisten(channel)

    def _listen(self, channel):
        if len(self.registry[channel]) != 0:
            return

        cursor = self.connection.cursor()
        cursor.execute(f"LISTEN {channel}")

    def _unlisten(self, channel):
        if len(self.registry[channel]) != 0:
            return

        cursor = self.connection.cursor()
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
        self.connection.poll()
        while self.connection.notifies:
            notification = self.connection.notifies.pop(0)
            self._dispatch(notification)

    def _dispatch(self, notification):
        for handler in self.registry[notification.channel]:
            handler(notification.payload)
