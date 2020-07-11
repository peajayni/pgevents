from datetime import datetime, timezone

EPOCH = datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc)


def now():
    return datetime.now(timezone.utc)
