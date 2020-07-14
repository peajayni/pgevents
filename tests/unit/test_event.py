from unittest.mock import sentinel, patch, Mock

import pytest

from pgevents.event import Event


@pytest.fixture
def data_access():
    with patch("pgevents.event.data_access") as data_access:
        yield data_access


def test_instantiate_just_topic():
    event = Event(topic=sentinel.topic)
    assert event.topic == sentinel.topic


def test_mark_processed(data_access):
    event = Event(id=sentinel.id, topic=sentinel.topic, payload=sentinel.payload)

    event.mark_processed(sentinel.cursor)

    data_access.mark_event_processed.assert_called_once_with(
        sentinel.cursor, sentinel.id
    )


@pytest.mark.parametrize(
    ["payload", "expected_string_value"],
    [["hello", "hello"], ["a" * 100, "a" * 50 + "..."]],
)
def test_repr_field(payload, expected_string_value):
    event = Event(id=sentinel.id, topic=sentinel.topic, payload=payload)
    field = Mock()
    field.name = "payload"
    assert event.repr_field(field) == f"payload={expected_string_value}"
