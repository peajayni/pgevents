import pytest

from pgevents import data_access
from tests.integration import DSN


@pytest.fixture()
def connection():
    connection = data_access.connect(DSN)
    yield connection
    connection.close()


@pytest.fixture(autouse=True)
def truncate_events(connection):
    with data_access.cursor(connection) as cursor:
        data_access.truncate_events(cursor)
