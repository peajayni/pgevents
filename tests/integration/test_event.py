import pytest

from pgevents import data_access
from pgevents.events import Event


@pytest.fixture()
def event(connection):
    with data_access.cursor(connection) as cursor:
        data = data_access.create_event(cursor, "test")
        return Event(data["id"], data["topic"], data["payload"])


def test_mark_processed(connection, event):
    with data_access.cursor(connection) as cursor:
        event.mark_processed(cursor)

    with data_access.cursor(connection) as cursor:
        event_data = data_access.get_event(cursor, event.id)
        assert event_data["status"] == Event.PROCESSED
