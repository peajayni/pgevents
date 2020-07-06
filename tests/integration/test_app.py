import time
from threading import Thread

from pgevents import data_access
from pgevents.app import App
from pgevents.events import Event
from tests.integration import DSN

CHANNEL = "test"
FOO_TOPIC = "foo"
BAR_TOPIC = "bar"

foo_event_id = None
bar_event_id = None


def send_notification():
    global foo_event_id, bar_event_id

    time.sleep(1)

    connection = data_access.connect(DSN)
    with data_access.cursor(connection) as cursor:
        foo_event_id = data_access.create_event(cursor, FOO_TOPIC)["id"]
        bar_event_id = data_access.create_event(cursor, BAR_TOPIC)["id"]
        data_access.notify(cursor, CHANNEL)


def test_app():
    app = App(DSN, CHANNEL)

    @app.register(FOO_TOPIC)
    def handler(event):
        pass

    thread = Thread(target=send_notification)
    thread.start()

    now = time.time()

    def continue_for_two_seconds(app):
        return time.time() < now + 2

    app.run(should_continue=continue_for_two_seconds)

    with data_access.cursor(app.connection) as cursor:
        assert data_access.get_event(cursor, foo_event_id)["status"] == Event.PROCESSED
        assert data_access.get_event(cursor, bar_event_id)["status"] == Event.PENDING
