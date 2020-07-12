import time
from datetime import timedelta
from threading import Thread

import pytest

from pgevents import data_access
from pgevents.event import Event, PENDING, PROCESSED
from pgevents.utils import timestamps
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
    event = Event(topic="foo")
    with data_access.cursor(connection) as cursor:
        created = data_access.create_event(cursor, event)

    with data_access.cursor(connection) as cursor:
        retrieved = data_access.get_event_by_id(cursor, created.id)

    assert created == retrieved
    assert created.status == PENDING


def test_create_and_get_event_with_payload():
    connection = data_access.connect(DSN)
    event = Event(topic="foo", payload=[dict(foo="bar"), dict(hello=[0, 1])])
    with data_access.cursor(connection) as cursor:
        created = data_access.create_event(cursor, event)

    with data_access.cursor(connection) as cursor:
        retrieved = data_access.get_event_by_id(cursor, created.id)

    assert retrieved.payload == event.payload


def test_create_and_get_event_with_process_after():
    connection = data_access.connect(DSN)
    event = Event(topic="foo", process_after=timestamps.now() + timedelta(seconds=10))
    with data_access.cursor(connection) as cursor:
        created = data_access.create_event(cursor, event)

    with data_access.cursor(connection) as cursor:
        retrieved = data_access.get_event_by_id(cursor, created.id)

    assert retrieved.process_after == event.process_after


@pytest.mark.parametrize(
    [
        "first_process_after",
        "second_process_after",
        "expected_first_status",
        "expected_second_status",
    ],
    [
        [None, None, PROCESSED, PROCESSED,],
        [timestamps.now() + timedelta(seconds=10), None, PENDING, PROCESSED,],
    ],
)
def test_get_next_event(
    first_process_after,
    second_process_after,
    expected_first_status,
    expected_second_status,
):
    connection = data_access.connect(DSN)
    topic = "foo"
    with data_access.cursor(connection) as cursor:
        first = data_access.create_event(
            cursor, Event(topic=topic, process_after=first_process_after)
        )
        second = data_access.create_event(
            cursor, Event(topic=topic, process_after=second_process_after)
        )

    time.sleep(0.1)

    def slow_running():
        local_connection = data_access.connect(DSN)
        with data_access.cursor(local_connection) as cursor:
            event = data_access.get_next_event(cursor, [topic])
            time.sleep(0.5)
            if event:
                data_access.mark_event_processed(cursor, event.id)

    def fast_running():
        local_connection = data_access.connect(DSN)
        with data_access.cursor(local_connection) as cursor:
            event = data_access.get_next_event(cursor, [topic])
            if event:
                data_access.mark_event_processed(cursor, event.id)

    slow_thread = Thread(target=slow_running)
    slow_thread.start()

    time.sleep(0.1)

    fast_thread = Thread(target=fast_running)
    fast_thread.start()

    slow_thread.join()
    fast_thread.join()

    with data_access.cursor(connection) as cursor:
        retrieved_first = data_access.get_event_by_id(cursor, first.id)

    assert retrieved_first.status == expected_first_status

    with data_access.cursor(connection) as cursor:
        retrieved_second = data_access.get_event_by_id(cursor, second.id)

    assert retrieved_second.status == expected_second_status


def test_mark_event_processed():
    connection = data_access.connect(DSN)
    event = Event(topic="foo")
    with data_access.cursor(connection) as cursor:
        created = data_access.create_event(cursor, event)

    with data_access.cursor(connection) as cursor:
        retrieved = data_access.get_event_by_id(cursor, created.id)
        assert retrieved.status == PENDING

    with data_access.cursor(connection) as cursor:
        data_access.mark_event_processed(cursor, created.id)

    with data_access.cursor(connection) as cursor:
        retrieved = data_access.get_event_by_id(cursor, created.id)
        assert retrieved.status == PROCESSED
