import pgmigrations
import pytest

from pgevents import data_access, constants
from tests.integration import DSN


@pytest.fixture()
def connection():
    connection = data_access.connect(DSN)
    yield connection
    connection.close()


@pytest.fixture(autouse=True)
def apply_migrations():
    migrations = pgmigrations.Migrations(locations=[constants.CORE_MIGRATIONS_LOCATION])
    migrations.apply(DSN)


@pytest.fixture(autouse=True)
def truncate_events(connection):
    with data_access.cursor(connection) as cursor:
        data_access.truncate_events(cursor)
