from unittest.mock import Mock

from pgevents import data_access
from pgevents.context import Context
from pgevents.event import Event


def test_create_event(connection):
    with data_access.cursor(connection) as cursor:
        event = Mock()
        new_event = Event(topic="hello")
        context = Context(event, cursor)
        created = context.create_event(new_event)

    with data_access.cursor(connection) as cursor:
        retrieved = data_access.get_event_by_id(cursor, created.id)

    assert created == retrieved
