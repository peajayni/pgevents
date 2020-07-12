import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Any

from pgevents import data_access, constants

LOGGER = logging.getLogger(__name__)


@dataclass
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
            payload=data["payload"],
            created_at=data["created_at"],
            process_after=data["process_after"],
            processed_at=data["processed_at"],
        )

    def mark_processed(self, cursor):
        assert self.id is not None, "Cannot mark processed when id is not set"
        data_access.mark_event_processed(cursor, self.id)
