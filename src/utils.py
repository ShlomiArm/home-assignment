from collections import defaultdict
from datetime import date, timedelta
from typing import Iterator, Tuple


def infer_schema(rows):
    schema = defaultdict(str)
    type_map = {
        "int": "bigint",
        "str": "varchar",
    }

    for row in rows:
        for k, v in row.items():
            schema[k] = type_map.get(type(v).__name__, "varchar")

    return schema


def date_ranges(
    start: str, total_days: int, step_days: int
) -> Iterator[Tuple[str, str]]:
    """
    Yield inclusive, non-overlapping date ranges.

    Each range is [start, end], and the next range starts at end + 1 day.
    The final range always ends exactly on start + total_days.
    """

    final_day = date.fromisoformat(start) + timedelta(days=total_days)
    current = date.fromisoformat(start)

    while current <= final_day:
        range_end = min(current + timedelta(days=step_days - 1), final_day)
        yield current.isoformat(), range_end.isoformat()
        current = range_end + timedelta(days=1)
