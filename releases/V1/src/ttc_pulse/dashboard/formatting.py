"""Formatting helpers shared across dashboard pages."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd

DAY_NAME_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def fmt_int(value: Any) -> str:
    if _is_missing(value):
        return "-"
    return f"{int(value):,}"


def fmt_float(value: Any, digits: int = 2) -> str:
    if _is_missing(value):
        return "-"
    return f"{float(value):,.{digits}f}"


def fmt_pct(value: Any, digits: int = 1) -> str:
    """Format ratio values (0-1) as percentages."""
    if _is_missing(value):
        return "-"
    return f"{float(value) * 100:.{digits}f}%"


def fmt_date(value: Any) -> str:
    if _is_missing(value):
        return "-"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def sort_day_name(frame: pd.DataFrame, column: str = "day_name") -> pd.DataFrame:
    """Apply a stable weekday order when the column exists."""
    if frame.empty or column not in frame.columns:
        return frame
    output = frame.copy()
    output[column] = pd.Categorical(output[column], categories=DAY_NAME_ORDER, ordered=True)
    return output.sort_values([column, "hour_bin"] if "hour_bin" in output.columns else [column])


def status_label(status: str) -> str:
    mapping = {
        "ok": "Ready",
        "empty": "Empty",
        "missing": "Missing",
        "error": "Error",
    }
    return mapping.get(status, status.title())


__all__ = [
    "DAY_NAME_ORDER",
    "fmt_date",
    "fmt_float",
    "fmt_int",
    "fmt_pct",
    "sort_day_name",
    "status_label",
]
