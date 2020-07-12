from contextlib import contextmanager
from datetime import timezone

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import register_adapter
from psycopg2.extras import RealDictCursor, Json

from pgevents.event import Event

register_adapter(dict, Json)
register_adapter(list, Json)


class RealDictTimezoneAwareCursor(RealDictCursor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tzinfo_factory = lambda _: timezone.utc


def connect(dsn):
    return psycopg2.connect(dsn, cursor_factory=RealDictTimezoneAwareCursor)


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


def create_event(cursor, event):
    if event.process_after:
        cursor.execute(
            """
            INSERT INTO events (topic, payload, process_after)
            VALUES (%s, %s, %s)
            RETURNING *
            """,
            [event.topic, event.payload, event.process_after],
        )
    else:
        cursor.execute(
            """
            INSERT INTO events (topic, payload)
            VALUES (%s, %s)
            RETURNING *
            """,
            [event.topic, event.payload],
        )
    return Event.from_dict(cursor.fetchone())


def get_event_by_id(cursor, event_id):
    cursor.execute(
        """
        SELECT *
        FROM events
        WHERE id=%s
        """,
        [event_id],
    )
    return Event.from_dict(cursor.fetchone())


def get_next_event(cursor, topics):
    query = sql.SQL(
        """
        SELECT *
        FROM events
        WHERE status='PENDING'
        AND process_after < now()
        AND topic in ({})
        ORDER BY id
        FOR UPDATE SKIP LOCKED
        LIMIT 1
        """
    ).format(sql.SQL(", ").join(sql.Literal(topic) for topic in topics))
    cursor.execute(query)
    data = cursor.fetchone()
    if not data:
        return None
    return Event.from_dict(data)


def mark_event_processed(cursor, event_id):
    cursor.execute(
        """
        UPDATE events
        SET status='PROCESSED',
        processed_at=now()
        WHERE id=%s
        """,
        [event_id],
    )


def truncate_events(cursor):
    cursor.execute("TRUNCATE events")


def drop_table(cursor, name):
    cursor.execute(f"DROP TABLE IF EXISTS {name}")


def drop_type(cursor, name):
    cursor.execute(f"DROP TYPE IF EXISTS {name}")
