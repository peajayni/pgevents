import pytest

from pgevents import data_access


@pytest.fixture()
def connection():
    dsn = "dbname=test user=test password=test host=localhost"
    connection = data_access.connect(dsn)
    yield connection
    connection.close()


@pytest.fixture(autouse=True)
def truncate_events(connection):
    with data_access.cursor(connection) as cursor:
        data_access.truncate_events(cursor)
