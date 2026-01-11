import argparse
import json
import os
import logging
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import psycopg
from dotenv import load_dotenv
from openai import OpenAI

from calendar_models import CalendarEvent

logger = logging.getLogger(__name__)


def _require_env(value: Optional[str], name: str) -> str:
    if value:
        return value
    raise SystemExit(f"Missing required env var: {name}")


def _format_date_range(start: date, end: Optional[date]) -> str:
    if not end or end == start:
        return start.strftime("%a, %b %d")
    return f"{start.strftime('%a, %b %d')}–{end.strftime('%a, %b %d')}"


def _format_time_range(start: datetime, end: Optional[datetime], tz: ZoneInfo) -> str:
    local_start = start.astimezone(tz)
    if not end:
        return local_start.strftime("%a, %b %d, %-I:%M %p")
    local_end = end.astimezone(tz)
    if local_start.date() == local_end.date():
        return f"{local_start.strftime('%a, %b %d, %-I:%M %p')}–{local_end.strftime('%-I:%M %p')}"
    return f"{local_start.strftime('%a, %b %d, %-I:%M %p')}–{local_end.strftime('%a, %b %d, %-I:%M %p')}"


def _load_events(
    conn: psycopg.Connection,
    source: str,
    start_date: date,
    end_date: date,
    start_dt: datetime,
    end_dt: datetime,
) -> list[CalendarEvent]:
    query = """
        select
          title,
          description,
          location,
          all_day,
          starts_at,
          ends_at,
          start_date,
          end_date
        from public.calendar_events
        where source = %s
          and (
            (all_day and start_date >= %s and start_date < %s) 
            or
            (all_day and start_date < %s and (end_date >= %s))
            or
            (not all_day and starts_at < %s and (ends_at is null or ends_at >= %s))
          )
        order by coalesce(start_date, starts_at), title
    """
    with conn.cursor() as cur:
        cur.execute(query, (source, start_date, end_date, start_date, start_date, end_dt, start_dt))
        rows = cur.fetchall()

    return [
        CalendarEvent(
            source=source,
            source_entity_key=None,
            external_id=None,
            title=row[0],
            description=row[1],
            location=row[2],
            starts_at=row[4],
            ends_at=row[5],
            all_day=row[3],
            start_date=row[6],
            end_date=row[7],
        )
        for row in rows
    ]


def _render_events_for_prompt(
    events: list[CalendarEvent],
    tz: ZoneInfo,
    window_start: date,
    window_start_dt: datetime,
) -> list[dict[str, str]]:
    rendered = []
    for event in events:
        is_ongoing = False
        if event.all_day and event.start_date:
            timing = _format_date_range(event.start_date, event.end_date)
            is_ongoing = event.start_date < window_start
        elif event.starts_at:
            timing = _format_time_range(event.starts_at, event.ends_at, tz)
            is_ongoing = event.starts_at < window_start_dt
        else:
            timing = "Date/time TBD"
        rendered.append(
            {
                "title": event.title,
                "timing": timing,
                "location": event.location or "Not specified",
                "description": event.description or "",
                "status": "ongoing" if is_ongoing else "new",
            }
        )
    return rendered


def _log_events(events: list[CalendarEvent], tz: ZoneInfo) -> None:
    if not events:
        logger.info("No calendar events found in the requested window.")
        return
    logger.info("Retrieved %s calendar events:", len(events))
    for event in events:
        if event.all_day and event.start_date:
            timing = _format_date_range(event.start_date, event.end_date)
        elif event.starts_at:
            timing = _format_time_range(event.starts_at, event.ends_at, tz)
        else:
            timing = "Date/time TBD"
        logger.info("- %s | %s | %s", timing, event.title, event.location or "Not specified")


def _build_prompt(events: list[dict[str, str]], start_date: date, end_date: date) -> list[dict[str, str]]:
    window_label = f"{start_date.strftime('%b %d, %Y')}–{(end_date - timedelta(days=1)).strftime('%b %d, %Y')}"
    system = (
        "You are an admin at an TheFlourishingNest, an organization that is created to help parents with logistics. "
        "You understand jersey city school system. For example, for public schools, CASPER is an after-care "
        "program and Morning stars is a before care program. "
        "Your job now is to craft concise weekly emails for families. "
        "Separate events into two buckets: Important (closures, schedule changes, urgent items) "
        "and Good to Know (events that don't change the school day). "
        "Use friendly, clear language and short bullet lists. "
        "Return a subject line and email body."
    )
    user = {
        "message_date": start_date.isoformat(),
        "window": window_label,
        "events": events,
        "instructions": (
            "Include 'Here is a look at the week ahead.' in every email "
            "before mentioning the events. "
            "If there are no calendar events at all (Good to Know or Important) in the upcoming week, "
            "add a short line: "
            "'This week, we want to let you know that there are no upcoming events.' "
            "Don't add filler text under any category. It's ok to remove a category "
            "from the email if there is nothing to report under that category. "
            "Keep the email under 250 words. "
            "Do not add any other reassurance or filler text. "
            "Rewrite event details in natural language; do not preserve the JSON keys "
            "or structured field labels in the output. "
            "Use the event details to infer what the event means for families "
            "(e.g., closures, schedule changes, or informational events) "
            "and convey that meaning clearly. "
            "If an event has status 'ongoing', mention it in natural wording (e.g., "
            "'Continuing from last week: Holiday and Winter Recess') "
            "without listing a Status field or label. "
            "Do not invent events. Greeting is ok. If there are any relevant tips "
            "for parents, for example needing to plan childcare for a school closure, "
            " or early pickup days, remind parents about it under a Helpful Tips section."
        ),
    }
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(user, ensure_ascii=True)},
    ]


def _generate_email(client: OpenAI, messages: list[dict[str, str]]) -> str:
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        temperature=0.3,
    )
    return response.choices[0].message.content.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate weekly parent email from calendar_events.")
    parser.add_argument("--date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--source", default="jersey city public schools", help="Calendar source")
    parser.add_argument("--days", type=int, default=7, help="Window length in days")
    parser.add_argument("--timezone", default="America/New_York", help="IANA time zone")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    load_dotenv()

    db_url = _require_env(os.environ.get("DATABASE_URL"), "DATABASE_URL")
    api_key = _require_env(os.environ.get("OPENAI_API_KEY"), "OPENAI_API_KEY")

    start_date = date.fromisoformat(args.date)
    end_date = start_date + timedelta(days=args.days)
    tz = ZoneInfo(args.timezone)
    start_dt = datetime.combine(start_date, datetime.min.time(), tzinfo=tz)
    end_dt = datetime.combine(end_date, datetime.min.time(), tzinfo=tz)

    with psycopg.connect(db_url) as conn:
        events = _load_events(conn, args.source, start_date, end_date, start_dt, end_dt)

    _log_events(events, tz)

    prompt_events = _render_events_for_prompt(events, tz, start_date, start_dt)
    messages = _build_prompt(prompt_events, start_date, end_date)

    client = OpenAI(api_key=api_key)
    email = _generate_email(client, messages)
    print(email)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
