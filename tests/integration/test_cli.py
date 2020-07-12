import json
import os
import pathlib
import tempfile

import pytest
from click.testing import CliRunner

from pgevents.cli import init_db, run, create_event
from tests.integration.fixtures.cli_mock_app import app

BASE_DIRECTORY = pathlib.Path(__file__).parent.absolute()


@pytest.fixture
def workspace():
    original = os.getcwd()
    with tempfile.TemporaryDirectory() as workspace:
        workspace = pathlib.Path(workspace)
        copy_app_script_to_workspace(workspace)
        os.chdir(workspace)
        yield workspace
        os.chdir(original)


def copy_app_script_to_workspace(workspace):
    source_script = BASE_DIRECTORY / "fixtures" / "cli_mock_app.py"
    target_script = workspace / "app.py"
    target_script.write_text(source_script.read_text())


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def app_path():
    return "app:app"


def test_cli_init_db(workspace, runner, app_path):
    cli_args = [app_path]
    result = runner.invoke(init_db, cli_args)
    assert result.exit_code == 0

    app.assert_called(app.init_db)


def test_cli_run(workspace, runner, app_path):
    cli_args = [app_path]
    result = runner.invoke(run, cli_args)
    assert result.exit_code == 0

    app.assert_called(app.run)


def test_cli_create_event(workspace, runner, app_path):
    cli_args = [app_path, "topic", "--payload", json.dumps("hello")]
    result = runner.invoke(create_event, cli_args)
    assert result.exit_code == 0

    app.assert_called(app.create_event)
