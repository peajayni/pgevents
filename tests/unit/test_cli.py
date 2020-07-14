from unittest.mock import Mock, sentinel, MagicMock

import pytest

from pgevents import cli
from pgevents.event import Event


@pytest.fixture
def app():
    return Mock()


@pytest.fixture
def app_loader(monkeypatch, app):
    app_loader = Mock()
    app_loader.load.return_value = app
    monkeypatch.setattr("pgevents.cli.app_loader", app_loader)
    return app_loader


@pytest.fixture
def json(monkeypatch):
    json = Mock()
    monkeypatch.setattr("pgevents.cli.json", json)
    return json


@pytest.fixture
def pgmigrations(monkeypatch):
    pgmigrations = Mock()
    monkeypatch.setattr("pgevents.cli.pgmigrations", pgmigrations)
    return pgmigrations


@pytest.fixture
def data_access(monkeypatch):
    data_access = MagicMock()
    monkeypatch.setattr("pgevents.cli.data_access", data_access)
    return data_access


def test_init_db(app_loader, app, pgmigrations):
    cli.init_db(sentinel.path)

    app_loader.load.assert_called_once_with(sentinel.path)

    pgmigrations.Migrations.assert_called_once_with(locations=app.migration_locations)

    migrations = pgmigrations.Migrations.return_value

    migrations.apply.assert_called_once_with(app.dsn)


def test_run(app_loader, app):
    cli.run(sentinel.path)

    app_loader.load.assert_called_once_with(sentinel.path)
    app.run.assert_called_once()


def test_create_event(app_loader, app, json):
    cli.create_event(sentinel.path, sentinel.topic, sentinel.payload)
    event = Event(topic=sentinel.topic, payload=json.loads(sentinel.payload))

    app_loader.load.assert_called_once_with(sentinel.path)
    app.create_event.assert_called_once_with(sentinel.topic, event)


def test_create_event(app_loader, app, json, data_access):
    event = Event(topic=sentinel.topic, payload=json.loads(sentinel.payload))
    connection = data_access.connect.return_value
    cursor = data_access.cursor.return_value.__enter__.return_value

    cli.create_event(sentinel.path, sentinel.topic, sentinel.payload)

    app_loader.load.assert_called_once_with(sentinel.path)
    data_access.connect.assert_called_once_with(app.dsn)
    data_access.cursor.assert_called_once_with(connection)
    data_access.create_event.assert_called_once_with(cursor, event)
