from contextlib import contextmanager

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor


def connect(dsn):
    return psycopg2.connect(dsn, cursor_factory=RealDictCursor)


@contextmanager
def cursor(connection):
    with connection:
        with connection.cursor() as cursor:
            yield cursor


def listen(cursor, channel):
    cursor.execute(f"LISTEN {channel}")


def unlisten(cursor, channel):
    cursor.execute(f"UNLISTEN {channel}")


def notify(cursor, channel):
    cursor.execute(f"NOTIFY {channel}")


def create_event(cursor, topic, payload=None):
    cursor.execute(
        """
        INSERT INTO events (topic, payload)
        VALUES (%s, %s)
        RETURNING *
        """,
        [topic, payload],
    )
    return cursor.fetchone()


def get_event(cursor, id):
    cursor.execute(
        """
        SELECT *
        FROM events
        WHERE id=%s
        """,
        [id],
    )
    return cursor.fetchone()


def get_next_event(cursor, topics):
    query = sql.SQL(
        """
        SELECT id, topic, payload
        FROM events
        WHERE status='PENDING'
        AND topic in ({})
        ORDER BY id
        FOR UPDATE SKIP LOCKED
        LIMIT 1
        """
    ).format(sql.SQL(", ").join(sql.Literal(topic) for topic in topics))
    cursor.execute(query)
    return cursor.fetchone()


def mark_event_processed(cursor, event_id):
    cursor.execute(
        """
        UPDATE events
        SET status='PROCESSED'
        WHERE id=%s
        """,
        [event_id],
    )


def truncate_events(cursor):
    cursor.execute("TRUNCATE events")
