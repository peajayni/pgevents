import os
import pathlib
from unittest.mock import sentinel

import pytest

from pgevents.utils import app_loader

BASE_DIRECTORY = pathlib.Path(__file__).parent.absolute()

app = sentinel.app
foo_app = sentinel.foo_app


@pytest.fixture
def work_dir():
    original = os.getcwd()
    os.chdir(BASE_DIRECTORY)
    yield
    os.chdir(original)


@pytest.mark.parametrize(
    ["path", "expected"],
    [["test_app_loader", app], ["test_app_loader:foo_app", foo_app]],
)
def test_load_succcess(work_dir, path, expected):
    actual = app_loader.load(path)
    assert actual == expected


@pytest.mark.parametrize(
    ["path", "expected_exception"],
    [
        ["test_app_loader:foo:app", ValueError],
        ["test_app_loader:bar_app", AttributeError],
    ],
)
def test_load_fail(work_dir, path, expected_exception):
    with pytest.raises(expected_exception):
        app_loader.load(path)
