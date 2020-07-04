from unittest.mock import Mock, sentinel, patch

import pytest

from pgevents.app import App
from pgevents.events import EventStream


@pytest.fixture
def data_access():
    with patch("pgevents.app.data_access") as data_access:
        yield data_access


@pytest.fixture
def app():
    return App(dsn=sentinel.dsn, channel=sentinel.channel)


def test_run(app):
    app.setup = Mock()
    app.tick = Mock()
    app.stop_listening = Mock()

    should_continue = Mock(side_effect=[True, False])

    app.run(should_continue=should_continue)

    app.setup.assert_called_once()
    app.tick.assert_called_once()
    app.stop_listening.assert_called_once()


def test_tick_when_notification(app):
    app.connection = Mock()
    app.connection.notifies = [sentinel.notification]
    app.event_stream = Mock()

    app.tick()

    app.connection.poll.assert_called_once()
    assert app.connection.notifies == []
    app.event_stream.process.assert_called_once()


def test_tick_when_no_notification(app):
    app.connection = Mock()
    app.connection.notifies = []
    app.event_stream = Mock()

    app.tick()

    app.connection.poll.assert_called_once()
    assert app.connection.notifies == []
    app.event_stream.process.assert_not_called()


def test_setup(app):
    app.connect = Mock()
    app.setup_event_stream = Mock()
    app.start_listening = Mock()

    app.setup()

    app.connect.assert_called_once()
    app.setup_event_stream.assert_called_once()
    app.start_listening.assert_called_once()


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

    decorator = app.register(sentinel.topic)

    assert decorator(func) == func
    assert app.handlers[sentinel.topic] == func


def test_unregister(app):
    func = Mock()
    app.handlers[sentinel.topic] = func

    app.unregister(sentinel.topic, func)

    with pytest.raises(KeyError):
        app.handlers[sentinel.topic]
