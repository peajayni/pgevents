from datetime import datetime, timezone
from unittest.mock import Mock, sentinel, patch

import pytest
from freezegun import freeze_time

from pgevents.utils import timestamps
from pgevents.app import App, always_continue, MIGRATIONS_DIRECTORY
from pgevents.event_stream import EventStream


@pytest.fixture
def data_access():
    with patch("pgevents.app.data_access") as data_access:
        yield data_access


@pytest.fixture
def app():
    return App(dsn=sentinel.dsn, channel=sentinel.channel)


@pytest.fixture
def pgmigrations(monkeypatch):
    pgmigrations = Mock()
    monkeypatch.setattr("pgevents.app.pgmigrations", pgmigrations)
    return pgmigrations


def test_always_continue():
    assert always_continue(sentinel.app)


def test_last_processed(app):
    assert app.last_processed == timestamps.EPOCH


def test_init_db(app, pgmigrations):
    app.init_db()

    pgmigrations.Migrations.assert_called_once_with(
        app.dsn, base_directory=MIGRATIONS_DIRECTORY
    )

    migrations = pgmigrations.Migrations.return_value

    migrations.init.assert_called_once()
    migrations.apply.assert_called_once()


def test_create_event(app, data_access):
    app.connection = sentinel.connection
    app.connect = Mock()
    cursor = data_access.cursor.return_value.__enter__.return_value

    app.create_event(sentinel.topic, sentinel.payload)

    app.connect.assert_called_once()
    data_access.cursor.assert_called_once_with(sentinel.connection)
    data_access.create_event.assert_called_once_with(
        cursor, sentinel.topic, sentinel.payload
    )


def test_run(app):
    app.connect = Mock()
    app.setup_event_stream = Mock()
    app.start_listening = Mock()
    app.tick = Mock()
    app.stop_listening = Mock()

    should_continue = Mock(side_effect=[True, False])

    app.run(should_continue=should_continue)

    app.connect.assert_called_once()
    app.setup_event_stream.assert_called_once()
    app.start_listening.assert_called_once()
    app.tick.assert_called_once()
    app.stop_listening.assert_called_once()


def test_tick_when_should_process_events(app):
    app.should_process_events = Mock(return_value=True)
    app.event_stream = Mock()

    app.tick()

    app.event_stream.process.assert_called_once()


def test_tick_when_should_not_process_events(app):
    app.should_process_events = Mock(return_value=False)
    app.event_stream = Mock()

    app.tick()

    app.event_stream.process.assert_not_called()


@pytest.mark.parametrize(
    ["has_received_notification", "has_exceeded_interval", "expected"],
    [
        [True, True, True],
        [True, False, True],
        [False, True, True],
        [False, False, False],
    ],
)
def test_should_process_events(
    app, has_received_notification, has_exceeded_interval, expected
):
    app.has_received_notification = Mock(return_value=has_received_notification)
    app.has_exceeded_interval = Mock(return_value=has_exceeded_interval)

    assert app.should_process_events() == expected


@freeze_time("2020-07-10 22:00")
def test_process_events(app):
    app.event_stream = Mock()
    app.process_events()

    app.event_stream.process.assert_called_once()
    assert app.last_processed == timestamps.now()


def test_has_received_notification_when_notification(app):
    app.connection = Mock()
    app.connection.notifies = [sentinel.notification]

    assert app.has_received_notification()

    app.connection.poll.assert_called_once()
    assert app.connection.notifies == []


def test_has_received_notification_when_no_notification(app):
    app.connection = Mock()
    app.connection.notifies = []

    assert not app.has_received_notification()

    app.connection.poll.assert_called_once()
    assert app.connection.notifies == []


@pytest.mark.parametrize(
    ["seconds_since_last_processed", "expected"], [[11, True], [10, False],],
)
@freeze_time("2020-07-10 22:00")
def test_has_exceeded_interval(app, seconds_since_last_processed, expected):
    app.interval = 10
    app.calculate_seconds_since_last_processed = Mock(
        return_value=seconds_since_last_processed
    )

    assert app.has_exceeded_interval() == expected


@freeze_time("2020-07-10 22:00")
def test_calculate_seconds_since_last_processed(app):
    app.last_processed = datetime(2020, 7, 10, 21, 59, tzinfo=timezone.utc)
    assert app.calculate_seconds_since_last_processed() == 60


def test_connect(app, data_access):
    app.connect()

    data_access.connect.assert_called_once_with(sentinel.dsn)
    assert app.connection == data_access.connect.return_value


def test_setup_event_stream(app):
    app.connection = sentinel.connection
    app.handlers = sentinel.handlers

    app.setup_event_stream()

    assert app.event_stream == EventStream(sentinel.connection, sentinel.handlers)


def test_start_listening(app, data_access):
    app.connection = sentinel.connection
    cursor = data_access.cursor.return_value.__enter__.return_value

    app.start_listening()

    data_access.cursor.assert_called_once_with(sentinel.connection)
    data_access.listen.assert_called_once_with(cursor, sentinel.channel)


def test_stop_listening(app, data_access):
    app.connection = sentinel.connection
    cursor = data_access.cursor.return_value.__enter__.return_value

    app.stop_listening()

    data_access.cursor.assert_called_once_with(sentinel.connection)
    data_access.unlisten.assert_called_once_with(cursor, sentinel.channel)


def test_register(app):
    func = Mock()
    func.__name__ = sentinel.name

    decorator = app.register(sentinel.topic)

    assert decorator(func) == func
    assert app.handlers[sentinel.topic] == func


def test_unregister_when_registered(app):
    func = Mock()
    func.__name__ = sentinel.name

    app.handlers = {sentinel.topic: func}

    app.unregister(sentinel.topic, func)

    with pytest.raises(KeyError):
        app.handlers[sentinel.topic]


def test_unregister_when_not_registered(app):
    func = Mock()
    func.__name__ = sentinel.name

    app.handlers = {}

    app.unregister(sentinel.topic, func)

    assert app.handlers == {}
