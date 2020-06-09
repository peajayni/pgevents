from unittest.mock import Mock, sentinel

import psycopg2
import pytest

from pgevents.app import App


@pytest.fixture
def psycopg2_connect(monkeypatch):
    connect = Mock()
    monkeypatch.setattr(psycopg2, "connect", connect)
    return connect


def test_app_makes_connection(psycopg2_connect):
    dsn = sentinel
    App(dsn)

    psycopg2_connect.assert_called_once_with(dsn)
