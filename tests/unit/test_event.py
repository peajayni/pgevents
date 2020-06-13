from unittest.mock import Mock

from pgevents.event import Event


def test_equality_when_equal():
    payload = Mock()
    event0 = Event(payload=payload)
    event1 = Event(payload=payload)

    assert event0 == event1


def test_equality_when_not_equal():
    event0 = Event(payload=Mock())
    event1 = Event(payload=Mock())

    assert event0 != event1
