from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional


@dataclass(frozen=True)
class CalendarEvent:
    source: str
    source_entity_key: Optional[str]
    external_id: Optional[str]
    title: str
    description: Optional[str]
    location: Optional[str]
    starts_at: Optional[datetime]
    ends_at: Optional[datetime]
    all_day: bool
    start_date: Optional[date]
    end_date: Optional[date]
