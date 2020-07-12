import queue
import time
from concurrent.futures.thread import ThreadPoolExecutor

import pytest

from pgevents import data_access
from pgevents.event import Event
from pgevents.event_stream import EventStream

QUEUE = queue.Queue()
HANDLER_SLEEP_TIME = 2


def process_next_assert_true(stream):
    assert stream.process_next()


def process_next_asset_false(stream):
    assert not stream.process_next()


@pytest.fixture()
def empty_queue():
    while not QUEUE.empty():
        QUEUE.get()


def create_event_stream(handler_error=False):
    dsn = "dbname=test user=test password=test host=localhost"
    connection = data_access.connect(dsn)

    def foo(event):
        if handler_error:
            raise Exception("Deliberately thrown from handler")
        QUEUE.put("item")
        time.sleep(HANDLER_SLEEP_TIME)

    handlers = dict(test=foo)

    return EventStream(connection, handlers)


def create_event(connection):
    with data_access.cursor(connection) as cursor:
        data = data_access.create_event(cursor, "test")
        return Event(data["id"], data["topic"], data["payload"])


def test_process_next_when_next(empty_queue):
    """
    Ensure that events get processed in parallel.
    """
    stream0 = create_event_stream()
    stream1 = create_event_stream()

    event0 = create_event(stream0.connection)
    event1 = create_event(stream0.connection)

    start_time = time.time()

    executor = ThreadPoolExecutor(max_workers=2)
    result0 = executor.submit(process_next_assert_true, stream0)
    result1 = executor.submit(process_next_assert_true, stream1)

    result0.result()
    result1.result()

    total_time = time.time() - start_time

    assert total_time < 2 * HANDLER_SLEEP_TIME

    assert QUEUE.qsize() == 2

    with data_access.cursor(stream0.connection) as cursor:
        assert data_access.get_event(cursor, event0.id)["status"] == Event.PROCESSED
        assert data_access.get_event(cursor, event1.id)["status"] == Event.PROCESSED


def test_process_next_when_not_next(empty_queue):
    """
    Ensure that the same event does not get processed twice.
    """
    stream0 = create_event_stream()
    stream1 = create_event_stream()

    event0 = create_event(stream0.connection)

    executor = ThreadPoolExecutor(max_workers=2)
    result0 = executor.submit(process_next_assert_true, stream0)
    time.sleep(1)
    result1 = executor.submit(process_next_asset_false, stream1)

    result0.result()
    result1.result()

    assert QUEUE.qsize() == 1

    with data_access.cursor(stream0.connection) as cursor:
        assert data_access.get_event(cursor, event0.id)["status"] == Event.PROCESSED


def test_process_next_error(empty_queue):
    """
    Ensure that if an error occurs when processing an event
    that it will be picked up again on the next iteration.
    """
    stream0 = create_event_stream(handler_error=True)
    stream1 = create_event_stream()

    event0 = create_event(stream0.connection)

    with pytest.raises(Exception):
        stream0.process_next()

    with data_access.cursor(stream0.connection) as cursor:
        assert data_access.get_event(cursor, event0.id)["status"] == Event.PENDING

    assert stream1.process_next()

    with data_access.cursor(stream0.connection) as cursor:
        assert data_access.get_event(cursor, event0.id)["status"] == Event.PROCESSED

    assert QUEUE.qsize() == 1
