import dataclasses
import logging
from datetime import datetime
from typing import Optional, Any

from pgevents import data_access, constants

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class Event:
    topic: str
    status: str = constants.PENDING
    id: Optional[int] = None
    payload: Optional[Any] = None
    created_at: Optional[datetime] = None
    process_after: Optional[datetime] = None
    processed_at: Optional[datetime] = None

    @classmethod
    def from_dict(cls, data):
        return cls(
            topic=data["topic"],
            status=data["status"],
            id=data["id"],
            payload=data["payload"]["payload"],
            created_at=data["created_at"],
            process_after=data["process_after"],
            processed_at=data["processed_at"],
        )

    def mark_processed(self, cursor):
        assert self.id is not None, "Cannot mark processed when id is not set"
        data_access.mark_event_processed(cursor, self.id)

    def __repr__(self):
        name = self.__class__.__qualname__
        fields = dataclasses.fields(self)
        fields_string = f", ".join([self.repr_field(field) for field in fields])
        return f"{name}({fields_string})"

    def repr_field(self, field):
        value = getattr(self, field.name)
        return f"{field.name}={_to_truncated_string(value)}"


def _to_truncated_string(value, max_lenth=50):
    full = str(value)
    return full if len(full) < max_lenth else (full[:max_lenth] + "...")
