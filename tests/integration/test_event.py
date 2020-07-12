import pytest

from pgevents import data_access, constants
from pgevents.event import Event


@pytest.fixture()
def event(connection):
    with data_access.cursor(connection) as cursor:
        return data_access.create_event(cursor, Event(topic="test"))


def test_mark_processed(connection, event):
    with data_access.cursor(connection) as cursor:
        event.mark_processed(cursor)

    with data_access.cursor(connection) as cursor:
        retrieved = data_access.get_event_by_id(cursor, event.id)
        assert retrieved.status == constants.PROCESSED
