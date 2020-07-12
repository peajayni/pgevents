import json
import logging

import click

from pgevents.utils import app_loader

LOGGER = logging.getLogger(__name__)


@click.group()
def cli():
    pass  # coverage: ignore


@cli.command()
@click.argument("path")
def init_db(path):
    LOGGER.info("Initialising database for app: %s", path)
    app = app_loader.load(path)
    app.init_db()
    LOGGER.info("Initialised database for app: %s", path)


@cli.command()
@click.argument("path")
def run(path):
    LOGGER.info("Running app: %s", path)
    app = app_loader.load(path)
    app.run()


@cli.command()
@click.argument("path")
@click.argument("topic")
@click.option("--payload", default=None)
def create_event(path, topic, payload):
    app = app_loader.load(path)
    parsed_payload = json.loads(payload)
    app.create_event(topic, parsed_payload)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    cli()
