"""Reusable Altair chart builders for Streamlit pages."""

from __future__ import annotations

from typing import Sequence

import altair as alt
import pandas as pd


def _auto_bar_height(frame: pd.DataFrame, minimum: int, per_row: int = 24, maximum: int = 1400) -> int:
    row_count = max(1, len(frame.index))
    return max(minimum, min(maximum, row_count * per_row + 40))


def line_chart(
    frame: pd.DataFrame,
    x: str,
    y: str,
    color: str | None = None,
    tooltip: Sequence[str] | None = None,
    title: str | None = None,
    height: int = 320,
) -> alt.Chart | None:
    """Build a line chart when data is available."""
    if frame.empty:
        return None

    chart = alt.Chart(frame).mark_line(point=True).encode(
        x=alt.X(x),
        y=alt.Y(y),
        tooltip=list(tooltip or []),
    )
    if color is not None:
        chart = chart.encode(color=alt.Color(color))
    return chart.properties(title=title, height=height).interactive()


def stacked_bar_chart(
    frame: pd.DataFrame,
    x: str,
    y: str,
    color: str,
    tooltip: Sequence[str] | None = None,
    title: str | None = None,
    height: int = 320,
) -> alt.Chart | None:
    """Build a stacked bar chart."""
    if frame.empty:
        return None

    chart = (
        alt.Chart(frame)
        .mark_bar()
        .encode(
            x=alt.X(x),
            y=alt.Y(y),
            color=alt.Color(color),
            tooltip=list(tooltip or []),
        )
    )
    resolved_height = _auto_bar_height(frame, minimum=height) if ":N" in y or ":O" in y else height
    return chart.properties(title=title, height=resolved_height)


def horizontal_bar_chart(
    frame: pd.DataFrame,
    x: str,
    y: str,
    tooltip: Sequence[str] | None = None,
    color: str | None = None,
    title: str | None = None,
    height: int = 360,
) -> alt.Chart | None:
    """Build a horizontal bar chart."""
    if frame.empty:
        return None

    chart = alt.Chart(frame).mark_bar().encode(
        x=alt.X(x),
        y=alt.Y(y, sort="-x"),
        tooltip=list(tooltip or []),
    )
    if color is not None:
        chart = chart.encode(color=alt.Color(color))
    return chart.properties(title=title, height=_auto_bar_height(frame, minimum=height))


def heatmap_chart(
    frame: pd.DataFrame,
    x: str,
    y: str,
    color: str,
    tooltip: Sequence[str] | None = None,
    title: str | None = None,
    height: int = 380,
) -> alt.Chart | None:
    """Build a heatmap chart."""
    if frame.empty:
        return None

    return (
        alt.Chart(frame)
        .mark_rect()
        .encode(
            x=alt.X(x),
            y=alt.Y(y),
            color=alt.Color(color, scale=alt.Scale(scheme="tealblues")),
            tooltip=list(tooltip or []),
        )
        .properties(title=title, height=height)
    )


__all__ = [
    "heatmap_chart",
    "horizontal_bar_chart",
    "line_chart",
    "stacked_bar_chart",
]
