import psycopg2
from psycopg2.extensions import connection


class App:
    def __init__(self, dsn: str):
        self._connection: connection = psycopg2.connect(dsn)
