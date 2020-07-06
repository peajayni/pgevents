import time
from threading import Thread

import pytest

from pgevents import data_access
from pgevents.events import Event
from tests.integration import DSN


@pytest.fixture(autouse=True)
def clear_events():
    connection = data_access.connect(DSN)
    with data_access.cursor(connection) as cursor:
        data_access.truncate_events(cursor)


def test_listen_notify_unlisten():
    connection = data_access.connect(DSN)
    channel = "foo"
    with data_access.cursor(connection) as cursor:
        data_access.listen(cursor, channel)

    with data_access.cursor(connection) as cursor:
        data_access.notify(cursor, channel)

    connection.poll()
    assert connection.notifies

    while connection.notifies:
        connection.notifies.pop()

    with data_access.cursor(connection) as cursor:
        data_access.unlisten(cursor, channel)

    with data_access.cursor(connection) as cursor:
        data_access.notify(cursor, channel)

    connection.poll()
    assert not connection.notifies


def test_create_and_get_event_without_payload():
    connection = data_access.connect(DSN)
    topic = "foo"
    with data_access.cursor(connection) as cursor:
        created_id = data_access.create_event(cursor, topic)["id"]

    with data_access.cursor(connection) as cursor:
        retrieved = data_access.get_event(cursor, created_id)

    assert retrieved["id"] == created_id
    assert retrieved["topic"] == topic
    assert retrieved["status"] == Event.PENDING
    assert retrieved["payload"] is None


def test_create_and_get_event_with_payload():
    connection = data_access.connect(DSN)
    topic = "foo"
    payload = [dict(foo="bar"), dict(hello=[0, 1])]
    with data_access.cursor(connection) as cursor:
        created_id = data_access.create_event(cursor, topic, payload=payload)["id"]

    with data_access.cursor(connection) as cursor:
        retrieved = data_access.get_event(cursor, created_id)

    assert retrieved["id"] == created_id
    assert retrieved["topic"] == topic
    assert retrieved["status"] == Event.PENDING
    assert retrieved["payload"] == payload


def test_get_next_event():
    connection = data_access.connect(DSN)
    topic = "foo"
    with data_access.cursor(connection) as cursor:
        first_id = data_access.create_event(cursor, topic)["id"]
        second_id = data_access.create_event(cursor, topic)["id"]

    def slow_running():
        local_connection = data_access.connect(DSN)
        with data_access.cursor(local_connection) as cursor:
            event = data_access.get_next_event(cursor, [topic])
            time.sleep(2)
            data_access.mark_event_processed(cursor, event["id"])

    def fast_running():
        local_connection = data_access.connect(DSN)
        with data_access.cursor(local_connection) as cursor:
            event = data_access.get_next_event(cursor, [topic])
            data_access.mark_event_processed(cursor, event["id"])

    slow_thread = Thread(target=slow_running)
    slow_thread.start()

    time.sleep(0.5)

    fast_thread = Thread(target=fast_running)
    fast_thread.start()

    slow_thread.join()
    fast_thread.join()

    with data_access.cursor(connection) as cursor:
        retrieved_first = data_access.get_event(cursor, first_id)

    assert retrieved_first["status"] == Event.PROCESSED

    with data_access.cursor(connection) as cursor:
        retrieved_second = data_access.get_event(cursor, second_id)

    assert retrieved_second["status"] == Event.PROCESSED


def test_mark_event_processed():
    connection = data_access.connect(DSN)
    topic = "foo"
    with data_access.cursor(connection) as cursor:
        created_id = data_access.create_event(cursor, topic)["id"]

    with data_access.cursor(connection) as cursor:
        retrieved = data_access.get_event(cursor, created_id)
        assert retrieved["status"] == Event.PENDING

    with data_access.cursor(connection) as cursor:
        data_access.mark_event_processed(cursor, created_id)

    with data_access.cursor(connection) as cursor:
        retrieved = data_access.get_event(cursor, created_id)
        assert retrieved["status"] == Event.PROCESSED