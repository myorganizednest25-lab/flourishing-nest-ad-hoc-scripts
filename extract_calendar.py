#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

import pdfplumber

MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

MONTH_RE = re.compile(
    r"\b("
    r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|"
    r"dec(?:ember)?)\b",
    re.IGNORECASE,
)
YEAR_RE = re.compile(r"\b(20\d{2})\b")
NUMERIC_DATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?\b")
MONTH_NAME_DATE_RE = re.compile(
    r"\b("
    r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|"
    r"dec(?:ember)?)\b"
    r"\s+(\d{1,2})(?:[,\s]+(20\d{2}))?\b",
    re.IGNORECASE,
)
DAY_MONTH_NAME_RE = re.compile(
    r"\b(\d{1,2})\s+("
    r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|"
    r"dec(?:ember)?)\b",
    re.IGNORECASE,
)
RANGE_RE = re.compile(r"\b(\d{1,2})\s*-\s*(\d{1,2})\b")


@dataclass
class Event:
    date: dt.date
    description: str
    page: int
    source_line: str


def _clean_line(line: str) -> str:
    return re.sub(r"\s+", " ", line).strip()


def _month_from_token(token: str) -> Optional[int]:
    if not token:
        return None
    return MONTHS.get(token.lower())


def _coerce_year(raw_year: Optional[str], default_year: Optional[int]) -> Optional[int]:
    if raw_year:
        year = int(raw_year)
        if year < 100:
            return 2000 + year
        return year
    return default_year


def _safe_date(year: int, month: int, day: int) -> Optional[dt.date]:
    try:
        return dt.date(year, month, day)
    except ValueError:
        return None


def _expand_range(
    start_day: int, end_day: int, month: int, year: int
) -> List[dt.date]:
    if end_day < start_day:
        return []
    dates: List[dt.date] = []
    for day in range(start_day, end_day + 1):
        date = _safe_date(year, month, day)
        if date:
            dates.append(date)
    return dates


def _extract_dates_from_line(
    line: str, current_month: Optional[int], current_year: Optional[int]
) -> List[dt.date]:
    dates: List[dt.date] = []

    for match in MONTH_NAME_DATE_RE.finditer(line):
        month = _month_from_token(match.group(1))
        day = int(match.group(2))
        year = _coerce_year(match.group(3), current_year)
        if month and year:
            date = _safe_date(year, month, day)
            if date:
                dates.append(date)

        range_match = RANGE_RE.search(line[match.end() :])
        if range_match and month and year:
            dates.extend(
                _expand_range(int(range_match.group(1)), int(range_match.group(2)), month, year)
            )

    for match in DAY_MONTH_NAME_RE.finditer(line):
        day = int(match.group(1))
        month = _month_from_token(match.group(2))
        year = current_year
        if month and year:
            date = _safe_date(year, month, day)
            if date:
                dates.append(date)

    for match in NUMERIC_DATE_RE.finditer(line):
        month = int(match.group(1))
        day = int(match.group(2))
        year = _coerce_year(match.group(3), current_year)
        if not year and current_year:
            year = current_year
        if year:
            date = _safe_date(year, month, day)
            if date:
                dates.append(date)

        range_match = RANGE_RE.search(line[match.end() :])
        if range_match and year:
            dates.extend(_expand_range(int(range_match.group(1)), int(range_match.group(2)), month, year))

    if current_month and current_year:
        for match in RANGE_RE.finditer(line):
            dates.extend(
                _expand_range(int(match.group(1)), int(match.group(2)), current_month, current_year)
            )

    seen = set()
    unique_dates = []
    for date in dates:
        if date not in seen:
            seen.add(date)
            unique_dates.append(date)
    return unique_dates


def _extract_month_year(line: str) -> Tuple[Optional[int], Optional[int]]:
    month_match = MONTH_RE.search(line)
    year_match = YEAR_RE.search(line)
    month = _month_from_token(month_match.group(1)) if month_match else None
    year = int(year_match.group(1)) if year_match else None
    return month, year


def _lines_from_tables(tables: Sequence[Sequence[Sequence[Optional[str]]]]) -> List[str]:
    lines: List[str] = []
    for table in tables:
        for row in table:
            cells = [cell for cell in row if cell and cell.strip()]
            if cells:
                lines.append(" ".join(cell.strip() for cell in cells))
    return lines


def parse_events_from_text(text: str, page_num: int) -> List[Event]:
    events: List[Event] = []
    current_month: Optional[int] = None
    current_year: Optional[int] = None

    for raw_line in text.splitlines():
        line = _clean_line(raw_line)
        if not line:
            continue

        month, year = _extract_month_year(line)
        if month:
            current_month = month
        if year:
            current_year = year

        dates = _extract_dates_from_line(line, current_month, current_year)
        if not dates:
            continue

        description = line
        for date in dates:
            events.append(
                Event(date=date, description=description, page=page_num, source_line=line)
            )

    return events


def extract_events(pdf_path: Path) -> List[Event]:
    events: List[Event] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page_index, page in enumerate(pdf.pages, start=1):
            page_text = page.extract_text() or ""
            page_tables = page.extract_tables()
            lines = []
            if page_text:
                lines.append(page_text)
            if page_tables:
                lines.append("\n".join(_lines_from_tables(page_tables)))
            combined_text = "\n".join(lines)
            events.extend(parse_events_from_text(combined_text, page_index))
    return events


def write_csv(events: Iterable[Event], output_path: Path) -> None:
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["date", "description", "page", "source_line"])
        for event in events:
            writer.writerow(
                [
                    event.date.isoformat(),
                    event.description,
                    event.page,
                    event.source_line,
                ]
            )


def write_json(events: Iterable[Event], output_path: Path) -> None:
    payload = [
        {
            "date": event.date.isoformat(),
            "description": event.description,
            "page": event.page,
            "source_line": event.source_line,
        }
        for event in events
    ]
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract calendar events from a PDF into CSV or JSON."
    )
    parser.add_argument(
        "pdf",
        nargs="?",
        default="input.pdf",
        help="Path to the PDF file (default: input.pdf).",
    )
    parser.add_argument(
        "--csv",
        default="calendar.csv",
        help="Output CSV path (default: calendar.csv).",
    )
    parser.add_argument(
        "--json",
        default="",
        help="Optional JSON output path.",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    pdf_path = Path(args.pdf)
    if not pdf_path.exists():
        raise SystemExit(f"PDF not found: {pdf_path}")

    events = extract_events(pdf_path)
    write_csv(events, Path(args.csv))

    if args.json:
        write_json(events, Path(args.json))

    print(f"Wrote {len(events)} events to {args.csv}")


if __name__ == "__main__":
    main()
