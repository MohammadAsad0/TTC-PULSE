"""Composite reliability scoring helpers for Gold marts."""

from __future__ import annotations

from dataclasses import dataclass
from math import isclose
from typing import Iterable


@dataclass(frozen=True)
class ScoreWeights:
    """Weights for composite scoring: S = w1*z(freq)+w2*z(sev90)+w3*z(reg90)+w4*cause_mix."""

    frequency: float = 0.35
    severity_p90: float = 0.30
    regularity_p90: float = 0.20
    cause_mix: float = 0.15

    def as_dict(self) -> dict[str, float]:
        return {
            "frequency": self.frequency,
            "severity_p90": self.severity_p90,
            "regularity_p90": self.regularity_p90,
            "cause_mix": self.cause_mix,
        }

    def total(self) -> float:
        return float(self.frequency + self.severity_p90 + self.regularity_p90 + self.cause_mix)


DEFAULT_SCORE_WEIGHTS = ScoreWeights()


def validate_weights(weights: ScoreWeights = DEFAULT_SCORE_WEIGHTS) -> None:
    """Validate score weights are non-negative and sum to 1.0."""
    values = [weights.frequency, weights.severity_p90, weights.regularity_p90, weights.cause_mix]
    if any(value < 0 for value in values):
        raise ValueError(f"Score weights must be non-negative, got {weights}")
    if not isclose(weights.total(), 1.0, rel_tol=1e-9, abs_tol=1e-9):
        raise ValueError(f"Score weights must sum to 1.0, got {weights.total():.6f}")


def _window_clause(partition_columns: Iterable[str] | None) -> str:
    partition_cols = [column.strip() for column in (partition_columns or []) if column.strip()]
    if not partition_cols:
        return ""
    return "PARTITION BY " + ", ".join(partition_cols)


def _zscore_sql(metric_expr: str, window_clause: str) -> str:
    over = "OVER ()" if not window_clause else f"OVER ({window_clause})"
    avg_expr = f"AVG({metric_expr}) {over}"
    stddev_expr = f"STDDEV_SAMP({metric_expr}) {over}"
    return f"COALESCE(({metric_expr} - {avg_expr}) / NULLIF({stddev_expr}, 0), 0.0)"


def composite_score_sql(
    frequency_expr: str = "frequency",
    severity_p90_expr: str = "severity_p90",
    regularity_p90_expr: str = "regularity_p90",
    cause_mix_expr: str = "cause_mix_score",
    partition_columns: Iterable[str] | None = None,
    weights: ScoreWeights = DEFAULT_SCORE_WEIGHTS,
) -> str:
    """Build SQL expression for composite score with z-normalized components."""
    validate_weights(weights)
    window = _window_clause(partition_columns)

    z_freq = _zscore_sql(frequency_expr, window)
    z_severity = _zscore_sql(severity_p90_expr, window)
    z_regularity = _zscore_sql(regularity_p90_expr, window)
    cause_mix = f"COALESCE({cause_mix_expr}, 0.0)"

    return (
        f"({weights.frequency} * {z_freq}) + "
        f"({weights.severity_p90} * {z_severity}) + "
        f"({weights.regularity_p90} * {z_regularity}) + "
        f"({weights.cause_mix} * {cause_mix})"
    )


__all__ = [
    "DEFAULT_SCORE_WEIGHTS",
    "ScoreWeights",
    "composite_score_sql",
    "validate_weights",
]
