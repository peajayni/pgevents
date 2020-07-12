from unittest.mock import Mock

from pgevents import data_access
from pgevents.context import Context


def test_create_event(connection):
    with data_access.cursor(connection) as cursor:
        event = Mock()
        topic = "hello"
        context = Context(event, cursor)
        created_id = context.create_event(topic,)["id"]

    with data_access.cursor(connection) as cursor:
        retrieved_id = data_access.get_event(cursor, created_id)["id"]

    assert created_id == retrieved_id
