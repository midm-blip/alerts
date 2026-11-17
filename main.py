"""Cloud Function entry points for Kepware log processing."""

from typing import Any, Dict

from functions.kepware_alerts import (
    ingest_kepware_log_events as _ingest_kepware_log_events,
    process_kepware_log_alerts as _process_kepware_log_alerts,
)


def ingest_kepware_log_events(event: Dict[str, Any], context: Any = None) -> str:
    """Ingest matching Kepware log events into BigQuery.

    Args:
        event: Pub/Sub event or payload containing Kepware log records.
        context: Cloud Function event context.

    Returns:
        The ingestion result returned by the processing implementation.
    """
    return _ingest_kepware_log_events(event, context)


def process_kepware_log_alerts(data: Dict[str, Any], context: Any = None) -> str:
    """Cloud Function entry point for scheduled Kepware alert processing.

    Args:
        data: Cloud Function event payload.
        context: Cloud Function event context.

    Returns:
        The alert-processing result.
    """
    return _process_kepware_log_alerts(data, context)
