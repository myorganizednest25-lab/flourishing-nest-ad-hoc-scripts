import argparse
import hashlib
import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Optional

import psycopg
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

from calendar_models import CalendarEvent


def _parse_time(value: str) -> tuple[int, int]:
    text = value.strip().lower()
    if not text.endswith(("am", "pm")):
        raise ValueError(f"Unsupported time format: {value!r}")

    meridiem = text[-2:]
    time_part = text[:-2].strip()
    if ":" in time_part:
        hour_text, minute_text = time_part.split(":", 1)
    else:
        hour_text, minute_text = time_part, "0"

    hour = int(hour_text)
    minute = int(minute_text)
    if hour < 1 or hour > 12 or minute < 0 or minute > 59:
        raise ValueError(f"Unsupported time format: {value!r}")

    if meridiem == "am":
        hour = 0 if hour == 12 else hour
    else:
        hour = 12 if hour == 12 else hour + 12

    return hour, minute


def _infer_all_day(item: dict[str, Any]) -> bool:
    if "all_day" in item:
        return bool(item["all_day"])
    if item.get("starts_at") or item.get("ends_at"):
        return False
    return True


def _hash_external_id(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _build_event(
    item: dict[str, Any],
    source: str,
    source_entity_key: Optional[str],
    tz: ZoneInfo,
) -> CalendarEvent:
    if "start_date" not in item:
        raise ValueError("Missing start_date")

    start_date = date.fromisoformat(item["start_date"])
    end_date = date.fromisoformat(item["end_date"]) if item.get("end_date") else None
    title = (item.get("title") or "").strip()
    if not title:
        raise ValueError("Missing title")

    description = (item.get("description") or "").strip() or None
    location = (item.get("location") or "").strip() or None

    all_day = _infer_all_day(item)

    if all_day:
        if item.get("starts_at") or item.get("ends_at"):
            raise ValueError("all_day event cannot include starts_at/ends_at")
        start_date_value = start_date
        end_date_value = end_date
        starts_at = None
        ends_at = None
    else:
        if not item.get("starts_at"):
            raise ValueError("Non-all-day event missing starts_at")
        hour, minute = _parse_time(item["starts_at"])
        starts_at = datetime(
            start_date.year,
            start_date.month,
            start_date.day,
            hour,
            minute,
            tzinfo=tz,
        )
        ends_at = None
        if item.get("ends_at"):
            end_hour, end_minute = _parse_time(item["ends_at"])
            ends_at = datetime(
                start_date.year,
                start_date.month,
                start_date.day,
                end_hour,
                end_minute,
                tzinfo=tz,
            )
        start_date_value = None
        end_date_value = None

    external_id_payload = {
        "start_date": item.get("start_date"),
        "end_date": item.get("end_date"),
        "title": item.get("title"),
        "description": item.get("description"),
        "location": item.get("location"),
        "starts_at": item.get("starts_at"),
        "ends_at": item.get("ends_at"),
        "all_day": all_day,
    }

    return CalendarEvent(
        source=source,
        source_entity_key=source_entity_key,
        external_id=_hash_external_id(external_id_payload),
        title=title,
        description=description,
        location=location,
        starts_at=starts_at,
        ends_at=ends_at,
        all_day=all_day,
        start_date=start_date_value,
        end_date=end_date_value,
    )


def _load_events(
    input_path: Path,
    source: str,
    source_entity_key: Optional[str],
    tz: ZoneInfo,
) -> list[CalendarEvent]:
    data = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("calendar.json must be a list")

    events: list[CalendarEvent] = []
    for idx, item in enumerate(data, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Calendar entry #{idx} is not an object")
        events.append(_build_event(item, source, source_entity_key, tz))
    return events


def _insert_events(conn: psycopg.Connection, events: Iterable[CalendarEvent]) -> int:
    rows = [
        (
            event.source,
            event.source_entity_key,
            event.external_id,
            event.title,
            event.description,
            event.location,
            event.starts_at,
            event.ends_at,
            event.all_day,
            event.start_date,
            event.end_date,
        )
        for event in events
    ]
    if not rows:
        return 0

    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into public.calendar_events (
              source,
              source_entity_key,
              external_id,
              title,
              description,
              location,
              starts_at,
              ends_at,
              all_day,
              start_date,
              end_date
            )
            values (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            on conflict (source, external_id) do update set
              source_entity_key = excluded.source_entity_key,
              title = excluded.title,
              description = excluded.description,
              location = excluded.location,
              starts_at = excluded.starts_at,
              ends_at = excluded.ends_at,
              all_day = excluded.all_day,
              start_date = excluded.start_date,
              end_date = excluded.end_date
            """,
            rows,
        )
    return len(rows)


def _require_value(value: Optional[str], name: str) -> str:
    if value:
        return value
    raise SystemExit(f"Missing required value: {name}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload calendar.json to calendar_events.")
    parser.add_argument("--input", default="calendar.json", help="Path to calendar.json")
    parser.add_argument("--db-url", default=None, help="Postgres connection string")
    parser.add_argument("--source", default=None, help="Source label, e.g. jcps")
    parser.add_argument("--source-entity-key", default=None, help="Optional source entity key")
    parser.add_argument("--timezone", default="America/New_York", help="IANA time zone")
    parser.add_argument("--dry-run", action="store_true", help="Validate and report only")

    args = parser.parse_args()

    load_dotenv()

    db_url = args.db_url or os.environ.get("DATABASE_URL")
    source = args.source or os.environ.get("CALENDAR_SOURCE")
    source_entity_key = args.source_entity_key or os.environ.get("CALENDAR_SOURCE_ENTITY_KEY")

    db_url = _require_value(db_url, "DATABASE_URL or --db-url")
    source = _require_value(source, "CALENDAR_SOURCE or --source")

    tz = ZoneInfo(args.timezone)
    events = _load_events(Path(args.input), source, source_entity_key, tz)

    if args.dry_run:
        print(f"Validated {len(events)} events from {args.input}.")
        return 0

    with psycopg.connect(db_url) as conn:
        inserted = _insert_events(conn, events)
        conn.commit()

    print(f"Upserted {inserted} events into public.calendar_events.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
