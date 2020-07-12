import time
from threading import Thread

from pgevents import data_access
from pgevents.app import App
from pgevents.event import Event, PROCESSED, PENDING
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
        foo_event_id = data_access.create_event(cursor, Event(topic=FOO_TOPIC)).id
        bar_event_id = data_access.create_event(cursor, Event(topic=BAR_TOPIC)).id
        data_access.notify(cursor, CHANNEL)


def test_init_db(connection):
    with data_access.cursor(connection) as cursor:
        data_access.drop_table(cursor, "pgmigrations")
        data_access.drop_table(cursor, "events")
        data_access.drop_type(cursor, "event_status")
    app = App(DSN, None)
    app.init_db()
    event = Event(topic=FOO_TOPIC)
    with data_access.cursor(connection) as cursor:
        data_access.create_event(cursor, event)


def test_create_event(connection):
    app = App(DSN, None)
    event = Event(topic=FOO_TOPIC, payload=dict(hello="world"))
    created = app.create_event(event)

    with data_access.cursor(connection) as cursor:
        retrieved = data_access.get_event_by_id(cursor, created.id)

    assert created == retrieved


def test_run_processes_due_to_notification():
    app = App(DSN, CHANNEL, interval=5)

    @app.register(FOO_TOPIC)
    def handler(context):
        pass

    thread = Thread(target=send_notification)
    thread.start()

    now = time.time()

    def continue_for_two_seconds(app):
        return time.time() < now + 2

    app.run(should_continue=continue_for_two_seconds)

    with data_access.cursor(app.connection) as cursor:
        assert data_access.get_event_by_id(cursor, foo_event_id).status == PROCESSED
        assert data_access.get_event_by_id(cursor, bar_event_id).status == PENDING


def test_run_processes_due_to_interval():
    global foo_event_id, bar_event_id

    connection = data_access.connect(DSN)
    with data_access.cursor(connection) as cursor:
        foo_event_id = data_access.create_event(cursor, Event(topic=FOO_TOPIC)).id
        bar_event_id = data_access.create_event(cursor, Event(topic=BAR_TOPIC)).id

    app = App(DSN, CHANNEL, interval=1)

    @app.register(FOO_TOPIC)
    def handler(context):
        pass

    now = time.time()

    def continue_for_two_seconds(app):
        return time.time() < now + 2

    app.run(should_continue=continue_for_two_seconds)

    with data_access.cursor(app.connection) as cursor:
        assert data_access.get_event_by_id(cursor, foo_event_id).status == PROCESSED
        assert data_access.get_event_by_id(cursor, bar_event_id).status == PENDING
