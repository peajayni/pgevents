from unittest.mock import Mock, sentinel

from pgevents.events import Event


def test_equality_when_equal():
    event0 = Event(sentinel.id, sentinel.topic, sentinel.payload0)
    event1 = Event(sentinel.id, sentinel.topic, sentinel.payload1)

    assert event0 == event1


def test_equality_when_not_equal():
    event0 = Event(sentinel.id0, sentinel.topic, sentinel.payload)
    event1 = Event(sentinel.id1, sentinel.topic, sentinel.payload)

    assert event0 != event1


def test_mark_processed():
    data_access = Mock()
    event = Event(
        sentinel.id, sentinel.topic, sentinel.payload, data_access=data_access
    )

    event.mark_processed(sentinel.cursor)

    data_access.mark_event_processed.assert_called_once_with(
        sentinel.cursor, sentinel.id
    )
