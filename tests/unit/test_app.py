from unittest.mock import Mock, sentinel, call

import psycopg2
import pytest

from pgevents.app import App
from pgevents.event import Event


@pytest.fixture
def psycopg2_connect(monkeypatch):
    connect = Mock()
    monkeypatch.setattr(psycopg2, "connect", connect)
    return connect


def test_app_makes_connection(psycopg2_connect):
    dsn = sentinel

    event_connection = Mock()
    notification_connection = Mock()

    psycopg2_connect.side_effect = [event_connection, notification_connection]

    app = App(dsn)

    assert app.event_connection == event_connection
    assert app.notification_connection == notification_connection

    app.notification_connection.set_isolation_level.assert_called_once_with(
        psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
    )


def test_app_register(psycopg2_connect):
    dsn = sentinel
    app = App(dsn)
    channel = "foo"

    @app.register(channel)
    def bar():
        pass

    expected = dict(foo=set([bar]))

    assert app.registry == expected
    app.notification_connection.cursor().execute.assert_called_once_with(f"LISTEN {channel}")


def test_app_duplicated_register(psycopg2_connect):
    dsn = sentinel
    app = App(dsn)
    channel = "foo"

    def bar():
        pass

    app.register(channel)(bar)
    app.register(channel)(bar)

    expected = dict(foo=set([bar]))

    assert app.registry == expected
    app.notification_connection.cursor().execute.assert_called_once_with(f"LISTEN {channel}")


def test_app_multiple_register(psycopg2_connect):
    dsn = sentinel
    app = App(dsn)
    channel = "foo"

    @app.register(channel)
    def bar0():
        pass

    @app.register(channel)
    def bar1():
        pass

    expected = dict(foo=set([bar0, bar1]))

    assert app.registry == expected
    app.notification_connection.cursor().execute.assert_called_once_with(f"LISTEN {channel}")


def test_app_unregister_when_registered(psycopg2_connect):
    dsn = sentinel
    app = App(dsn)
    channel = "foo"

    def bar():
        pass

    app.registry[channel].add(bar)

    app.unregister(channel, bar)

    assert app.registry[channel] == set()
    app.notification_connection.cursor().execute.assert_called_once_with(f"UNLISTEN {channel}")


def test_app_unregister_when_multiple_registered(psycopg2_connect):
    dsn = sentinel
    app = App(dsn)
    channel = "foo"

    def bar0():
        pass

    def bar1():
        pass

    app.registry[channel].add(bar0)
    app.registry[channel].add(bar1)

    app.unregister(channel, bar1)

    assert app.registry[channel] == set([bar0])
    app.notification_connection.cursor().execute.assert_not_called()


def test_app_unregister_when_not_registered(psycopg2_connect):
    dsn = sentinel
    app = App(dsn)
    channel = "foo"

    def bar():
        pass

    app.unregister(channel, bar)

    assert app.registry[channel] == set()
    app.notification_connection.cursor().execute.assert_not_called()


def test_run(psycopg2_connect):
    dsn = sentinel
    app = App(dsn)
    channel = "foo"

    handler = Mock()
    notification0 = Mock()
    notification0.channel = channel

    notification1 = Mock()
    notification1.channel = channel

    # Should be ignored as different channel
    notification2 = Mock()
    notification2.channel = f"{channel}-different"

    app.register(channel)(handler)

    # Raise exception to break infinite loop
    app.notification_connection.poll.side_effect = [None, StopIteration]
    app.notification_connection.notifies = [notification0, notification1]

    with pytest.raises(StopIteration):
        app.run()

    assert handler.call_args_list == [
        call(Event(payload=notification0.payload)),
        call(Event(payload=notification1.payload)),
    ]
