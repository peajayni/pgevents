import json
import logging

import click
import pgmigrations

from pgevents import data_access
from pgevents.event import Event
from pgevents.utils import app_loader

LOGGER = logging.getLogger(__name__)


@click.group()
def cli():
    pass  # coverage: ignore


@cli.command(name="init_db")
@click.argument("path")
def init_db_handler(path):
    init_db(path)


@cli.command(name="run")
@click.argument("path")
def run_handler(path):
    run(path)


@cli.command(name="create_event")
@click.argument("path")
@click.argument("topic")
@click.option("--payload", default=None)
def create_event_handler(path, topic, payload):
    create_event(path, topic, payload)


def init_db(path):
    LOGGER.info("Initialising database for app: %s", path)
    app = app_loader.load(path)

    migrations = pgmigrations.Migrations(app.dsn, locations=app.migration_locations)
    migrations.apply()

    LOGGER.info("Initialised database for app: %s", path)


def run(path):
    LOGGER.info("Running app: %s", path)
    app = app_loader.load(path)
    app.run()


def create_event(path, topic, payload):
    app = app_loader.load(path)
    event = Event(topic=topic, payload=json.loads(payload))
    connection = data_access.connect(app.dsn)
    with data_access.cursor(connection) as cursor:
        return data_access.create_event(cursor, event)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    cli()
