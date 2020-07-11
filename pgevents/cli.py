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


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    cli()
