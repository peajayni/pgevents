import json
import pathlib


from pgevents import data_access, App, cli
from pgevents.event import Event
from tests.integration import DSN

BASE_DIRECTORY = pathlib.Path(__file__).parent.absolute()

app = App(DSN, None)


def test_init_db(connection):

    with data_access.cursor(connection) as cursor:
        data_access.drop_table(cursor, "pgmigrations")
        data_access.drop_table(cursor, "events")
        data_access.drop_type(cursor, "event_status")

    cli.init_db("tests.integration.test_cli")

    event = Event(topic="hello")
    with data_access.cursor(connection) as cursor:
        data_access.create_event(cursor, event)


def test_create_event(connection):
    topic = "hello"
    payload = json.dumps(dict(hello="world"))
    created = cli.create_event("tests.integration.test_cli", topic, payload)

    with data_access.cursor(connection) as cursor:
        retrieved = data_access.get_event_by_id(cursor, created.id)

    assert retrieved.topic == topic
    assert retrieved.payload == json.loads(payload)
