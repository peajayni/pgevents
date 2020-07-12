from unittest.mock import sentinel, patch

import pytest

from pgevents.context import Context


@pytest.fixture
def data_access():
    with patch("pgevents.context.data_access") as data_access:
        yield data_access


@pytest.fixture
def context():
    return Context(sentinel.event, sentinel.cursor)


def test_init(context):
    assert context.event == sentinel.event
    assert context.cursor == sentinel.cursor


def test_create_event(context, data_access):
    context.create_event(sentinel.event)
    data_access.create_event.assert_called_once_with(
        sentinel.cursor, sentinel.event,
    )
