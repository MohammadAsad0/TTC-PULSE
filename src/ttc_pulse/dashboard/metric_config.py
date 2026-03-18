"""Shared metric selection helpers for TTC Pulse dashboard pages."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import pandas as pd

METRIC_OPTIONS: list[str] = [
    "Composite Score",
    "Frequency",
    "Severity",
    "Regularity",
    "Cause Mix",
]

METRIC_COLUMN_MAP: dict[str, str] = {
    "Composite Score": "composite_score",
    "Frequency": "frequency",
    "Severity": "severity_p90",
    "Regularity": "regularity_p90",
    "Cause Mix": "cause_mix_score",
}

METRIC_FALLBACK_ORDER: dict[str, list[str]] = {
    "Composite Score": ["composite_score", "frequency", "severity_p90", "regularity_p90", "cause_mix_score"],
    "Frequency": ["frequency", "composite_score", "severity_p90", "regularity_p90", "cause_mix_score"],
    "Severity": ["severity_p90", "frequency", "composite_score", "regularity_p90", "cause_mix_score"],
    "Regularity": ["regularity_p90", "frequency", "composite_score", "severity_p90", "cause_mix_score"],
    "Cause Mix": ["cause_mix_score", "frequency", "composite_score", "severity_p90", "regularity_p90"],
}

METRIC_LABEL_BY_COLUMN: dict[str, str] = {column: label for label, column in METRIC_COLUMN_MAP.items()}


@dataclass(frozen=True)
class MetricResolution:
    """Resolved metric selection for a frame-backed view."""

    requested_label: str
    requested_column: str
    resolved_label: str
    resolved_column: str
    fallback_used: bool
    message: str | None = None


def metric_axis_title(metric_label: str) -> str:
    """Return a readable axis label for a selected metric."""
    return metric_label


def metric_chart_title(subject: str, metric_label: str, composite_phrase: str = "Composite Reliability Risk Score") -> str:
    """Build a chart title that preserves the legacy composite phrasing."""
    if metric_label == "Composite Score":
        return f"{subject} by {composite_phrase}"
    return f"{subject} by {metric_label}"


def metric_column(metric_label: str) -> str:
    """Return the canonical column name for a selected metric."""
    return METRIC_COLUMN_MAP[metric_label]


def metric_fallback_candidates(metric_label: str, extra_candidates: Sequence[str] | None = None) -> list[str]:
    """Return ordered fallback columns for a selected metric."""
    candidates = [metric_column(metric_label)]
    candidates.extend(METRIC_FALLBACK_ORDER.get(metric_label, []))
    if extra_candidates:
        candidates.extend(list(extra_candidates))

    ordered: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate not in seen:
            ordered.append(candidate)
            seen.add(candidate)
    return ordered


def _column_has_values(frame: pd.DataFrame, column: str) -> bool:
    if column not in frame.columns:
        return False
    values = pd.to_numeric(frame[column], errors="coerce")
    return bool(values.notna().any())


def resolve_metric_choice(
    frame: pd.DataFrame,
    metric_label: str,
    extra_candidates: Sequence[str] | None = None,
) -> MetricResolution:
    """Resolve a requested metric to an available column, with fallback if needed."""
    requested_column = metric_column(metric_label)
    candidates = metric_fallback_candidates(metric_label, extra_candidates=extra_candidates)

    for candidate in candidates:
        if _column_has_values(frame, candidate):
            resolved_label = METRIC_LABEL_BY_COLUMN.get(candidate, candidate.replace("_", " ").title())
            fallback_used = candidate != requested_column
            message = None
            if fallback_used:
                message = (
                    f"Selected metric '{metric_label}' is unavailable for this slice. "
                    f"Falling back to '{resolved_label}'."
                )
            return MetricResolution(
                requested_label=metric_label,
                requested_column=requested_column,
                resolved_label=resolved_label,
                resolved_column=candidate,
                fallback_used=fallback_used,
                message=message,
            )

    return MetricResolution(
        requested_label=metric_label,
        requested_column=requested_column,
        resolved_label=metric_label,
        resolved_column=requested_column,
        fallback_used=False,
        message=None,
    )


__all__ = [
    "METRIC_COLUMN_MAP",
    "METRIC_FALLBACK_ORDER",
    "METRIC_LABEL_BY_COLUMN",
    "METRIC_OPTIONS",
    "MetricResolution",
    "metric_axis_title",
    "metric_chart_title",
    "metric_column",
    "metric_fallback_candidates",
    "resolve_metric_choice",
]
