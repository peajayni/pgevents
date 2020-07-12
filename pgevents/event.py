import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Any

from pgevents import data_access

LOGGER = logging.getLogger(__name__)

PENDING = "PENDING"
PROCESSED = "PROCESSED"


@dataclass
class Event:
    topic: str
    status: str = PENDING
    id: Optional[int] = None
    payload: Optional[Any] = None
    created_at: Optional[datetime] = None
    process_after: Optional[datetime] = None
    processed_at: Optional[datetime] = None

    def mark_processed(self, cursor):
        assert self.id is not None, "Cannot mark processed when id is not set"
        data_access.mark_event_processed(cursor, self.id)
