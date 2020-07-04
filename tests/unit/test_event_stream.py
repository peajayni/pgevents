from unittest.mock import sentinel, MagicMock

from pgevents.events import EventStream


def test_process():
    stream = EventStream(sentinel.connection, sentinel.handlers)
    stream.process_next = MagicMock(side_effect=[True, False])

    stream.process()

    assert stream.process_next.call_count == 2


def test_process_next_when_next():
    connection = MagicMock()
    event = MagicMock()
    handler = MagicMock()
    handlers = {event.topic: handler}
    stream = EventStream(connection, handlers)
    stream.get_next = MagicMock(return_value=event)

    assert stream.process_next()

    handler.assert_called_once_with(event)
    event.mark_processed.assert_called_once()


def test_process_next_when_not_next():
    connection = MagicMock()
    stream = EventStream(connection, sentinel.handlers)
    stream.get_next = MagicMock(return_value=None)

    assert not stream.process_next()


def test_get_next_when_not_next():
    data_access = MagicMock()
    data_access.get_next_event.return_value = None
    stream = EventStream(
        sentinel.connection, sentinel.handlers, data_access=data_access
    )

    assert stream.get_next(sentinel.cursor) is None

    data_access.get_next_event.assert_called_once_with(sentinel.cursor)
