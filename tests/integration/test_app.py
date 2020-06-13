from threading import Thread

import psycopg2

from pgevents.app import App

DSN = "dbname=postgres user=postgres password=postgres host=integration_db"
CHANNEL = "foo"


class EndTest(Exception):
    pass


def send_notification():
    connection = psycopg2.connect(DSN)
    connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()
    cursor.execute(f"NOTIFY {CHANNEL}")


def test_app():
    app = App(DSN)

    @app.register(CHANNEL)
    def handler(payload):
        raise EndTest

    thread = Thread(target=send_notification)
    thread.start()

    try:
        app.run()
    except EndTest:
        pass
