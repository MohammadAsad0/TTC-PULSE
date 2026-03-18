"""Polling entry points for GTFS-RT alerts."""

from datetime import datetime
from typing import Any, Optional


def poll_gtfsrt_alerts(
    as_of: Optional[datetime] = None,
    timeout_seconds: float = 10.0,
) -> dict[str, Any]:
    """Stub for TTC GTFS-RT alerts polling."""
    raise NotImplementedError("Implement GTFS-RT alerts polling in a follow-up task.")
