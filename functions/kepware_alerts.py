"""Kepware log ingestion and alerting workflow implementation.

Architecture:
    Stage 1: Kepware app/device logs are collected and forwarded to GCP via a
        Pub/Sub or equivalent ingestion endpoint. A Cloud Function ingests the
        raw logs and stores them in the BigQuery staging table.
    Stage 2: Recent staged logs are scanned for known error patterns (0x4, 0x1,
        and "Device Not Responding"). Matching records emit a structured alert
        to Cloud Logging, which then triggers the configured alert policy and
        email notifications.

Important design distinction:
    - BigQuery stores the collected logs in the staging table.
    - Cloud Logging is used only for the alerting mechanism.
"""

from __future__ import annotations

import base64
import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from google.cloud import bigquery
from google.cloud import logging as cloud_logging


STAGE_TABLE = os.getenv(
    "KEPWARE_STAGE_TABLE",
    "fxg_pdm_us_de_stg.tbl_stg_kepware_log_records",
)
DEDUP_TABLE = os.getenv(
    "KEPWARE_DEDUP_TABLE",
    "fxg_pdm_us_de_logs.tbl_kepware_log_alert_sent",
)
CONFIG_TABLE = os.getenv(
    "KEPWARE_CONFIG_TABLE",
    "fxg_pdm_us_de_logs.tbl_update_flag_shm_cloud_logger",
)
LOGGER_NAME = os.getenv("KEPWARE_LOGGER_NAME", "ALERT_KEPWARE_LOG_ERRORS")

ERROR_PATTERNS: Dict[str, re.Pattern[str]] = {
    "OX4": re.compile(r"(?i)\b0x0*4\b|\box4\b"),
    "OX1": re.compile(r"(?i)\b0x0*1\b|\box1\b"),
    "DEVICE_NOT_RESPONDING": re.compile(r"(?i)device\s+not\s+responding"),
}


def _utc_now() -> datetime:
    """Return the current UTC time.

    Returns:
        The current time as a timezone-aware datetime in UTC.
    """
    return datetime.now(timezone.utc)


def _safe_int(value: Any, default: int) -> int:
    """Convert a value to a positive integer or return the fallback.

    Args:
        value: Value to convert to an integer.
        default: Value to return when conversion fails or is non-positive.

    Returns:
        The converted positive integer, or ``default``.
    """
    try:
        parsed = int(value)
        return parsed if parsed > 0 else default
    except (TypeError, ValueError):
        return default


def _parse_timestamp(raw_value: Any) -> Optional[datetime]:
    """Parse common timestamp representations into a UTC datetime.

    Args:
        raw_value: A datetime, Unix timestamp, or supported timestamp string.

    Returns:
        A timezone-aware UTC datetime, or ``None`` if parsing fails.
    """
    if raw_value is None:
        return None

    if isinstance(raw_value, datetime):
        if raw_value.tzinfo is None:
            return raw_value.replace(tzinfo=timezone.utc)
        return raw_value.astimezone(timezone.utc)

    if isinstance(raw_value, (int, float)):
        try:
            return datetime.fromtimestamp(float(raw_value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None

    if isinstance(raw_value, str):
        value = raw_value.strip()
        if not value:
            return None

        candidates = [
            value,
            value.replace("Z", "+00:00"),
            value.replace(" ", "T").replace(" ", "T"),
        ]
        for candidate in candidates:
            try:
                if candidate.endswith("Z"):
                    candidate = candidate[:-1] + "+00:00"
                parsed = datetime.fromisoformat(candidate)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc)
            except ValueError:
                continue

        # Common SQL/BigQuery style timestamps
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
        ):
            try:
                parsed = datetime.strptime(value, fmt)
                return parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                continue

    return None


def detect_error_patterns(log_message: Any) -> List[str]:
    """Find normalized Kepware error codes in a log message.

    Args:
        log_message: Log text to scan.

    Returns:
        Matching error-code names in the order they are configured.
    """
    text = str(log_message or "").strip()
    if not text:
        return []

    matches: List[str] = []
    for name, pattern in ERROR_PATTERNS.items():
        if pattern.search(text):
            matches.append(name)

    return matches


def _get_site_device(raw_record: Dict[str, Any]) -> Tuple[str, str]:
    """Extract site and device identifiers from a raw record.

    Args:
        raw_record: Log record containing optional site and device fields.

    Returns:
        A pair of non-empty site and device identifiers, using ``UNKNOWN``
        when either identifier is unavailable.
    """
    site_id = str(raw_record.get("site_id") or raw_record.get("site") or "UNKNOWN").strip()
    device_name = str(
        raw_record.get("device_name")
        or raw_record.get("device")
        or raw_record.get("asset_id")
        or raw_record.get("point_name")
        or "UNKNOWN"
    ).strip()
    return site_id or "UNKNOWN", device_name or "UNKNOWN"


def normalize_log_record(raw_record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize a raw log payload into a BigQuery staging record.

    Args:
        raw_record: Raw record containing log details and optional metadata.

    Returns:
        A normalized staging record if it contains a recognized error;
        otherwise, ``None``.
    """
    if not isinstance(raw_record, dict):
        return None

    message = (
        raw_record.get("message")
        or raw_record.get("log_message")
        or raw_record.get("details")
        or raw_record.get("raw_log")
        or ""
    )
    raw_message = str(message or "").strip()
    if not raw_message:
        return None

    error_matches = detect_error_patterns(raw_message)
    if not error_matches:
        return None

    site_id, device_name = _get_site_device(raw_record)
    timestamp = _parse_timestamp(
        raw_record.get("log_timestamp_utc")
        or raw_record.get("timestamp")
        or raw_record.get("event_time")
        or raw_record.get("created_at")
        or raw_record.get("time")
    ) or _utc_now()

    normalized = {
        "source_system": str(raw_record.get("source_system") or raw_record.get("system") or "kepware").strip(),
        "log_source": str(raw_record.get("log_source") or raw_record.get("source") or "kepware").strip(),
        "site_id": site_id,
        "asset_id": str(raw_record.get("asset_id") or raw_record.get("device_id") or "UNKNOWN").strip(),
        "device_name": device_name,
        "log_level": str(raw_record.get("severity") or raw_record.get("log_level") or raw_record.get("level") or "INFO").strip(),
        "log_message": raw_message[:15000],
        "error_codes": error_matches,
        "error_count": len(error_matches),
        "log_timestamp_utc": timestamp,
        "created_at": _utc_now(),
    }

    return normalized


def decode_pubsub_message(event: Dict[str, Any]) -> Dict[str, Any]:
    """Decode a Pub/Sub event into a dictionary payload.

    Args:
        event: Pub/Sub event with an optional base64-encoded ``data`` field.

    Returns:
        The decoded JSON object or a dictionary containing the decoded text.
        Events that cannot be decoded are returned as message text.
    """
    if not isinstance(event, dict):
        return {"message": str(event)}

    if "data" in event:
        data_value = event["data"]
        if isinstance(data_value, str):
            try:
                decoded = base64.b64decode(data_value)
                text = decoded.decode("utf-8")
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    return {"message": text}
            except Exception:
                return {"message": data_value}

        if isinstance(data_value, (bytes, bytearray)):
            try:
                text = bytes(data_value).decode("utf-8")
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    return {"message": text}
            except Exception:
                return {"message": str(data_value)}

    if "message" in event:
        return event

    return event


def ingest_kepware_log_events(event: Dict[str, Any], context: Any = None) -> str:
    """Normalize and store matching Kepware logs in BigQuery.

    Args:
        event: Pub/Sub event or payload containing one or more log records.
        context: Cloud Function event context; accepted for handler
            compatibility and otherwise unused.

    Returns:
        A success message describing whether records were ingested.

    Raises:
        RuntimeError: If BigQuery reports errors while inserting records.
    """
    client_bq = bigquery.Client()

    payload = decode_pubsub_message(event)
    records: List[Dict[str, Any]] = []

    if isinstance(payload, list):
        candidate_records = payload
    elif isinstance(payload, dict) and "records" in payload and isinstance(payload["records"], list):
        candidate_records = payload["records"]
    else:
        candidate_records = [payload]

    for item in candidate_records:
        if not isinstance(item, dict):
            continue
        normalized = normalize_log_record(item)
        if normalized is not None:
            records.append(normalized)

    if not records:
        return "success: no matching Kepware error records to ingest"

    errors = client_bq.insert_rows_json(STAGE_TABLE, records)
    if errors:
        raise RuntimeError(f"BigQuery insert failed for Kepware stage data: {errors}")

    return f"success: ingested {len(records)} Kepware log record(s)"


def _get_alert_window() -> Tuple[int, str]:
    """Read and validate the alert detection window from environment variables.

    Returns:
        A tuple containing a positive time interval and either ``MINUTE`` or
        ``HOUR``.
    """
    time_parameter_raw = os.getenv("time_parameter", "15")
    time_pattern_raw = os.getenv("time_pattern", "MINUTE")

    try:
        time_parameter = int(time_parameter_raw)
        if time_parameter <= 0:
            time_parameter = 15
    except (TypeError, ValueError):
        time_parameter = 15

    time_pattern = str(time_pattern_raw).upper().strip()
    if time_pattern not in ("MINUTE", "HOUR"):
        time_pattern = "MINUTE"

    return time_parameter, time_pattern


def _read_flag_value(client_bq: bigquery.Client) -> str:
    """Read the Cloud Logging enablement flag from the configuration table.

    Args:
        client_bq: BigQuery client used to execute the configuration query.

    Returns:
        The normalized flag value, or ``N`` if the query fails or has no rows.
    """
    query = f"""
        SELECT update_flag_cloud_logger
        FROM `{CONFIG_TABLE}`
        WHERE LOWER(log_name) = 'alert_kepware_log_errors'
    """
    try:
        rows = list(client_bq.query(query).result())
    except Exception:
        return "N"

    if not rows:
        return "N"

    value = rows[0].get("update_flag_cloud_logger")
    return str(value or "N").strip().upper()


def _read_recent_stage_rows(client_bq: bigquery.Client, time_parameter: int, time_pattern: str) -> List[Dict[str, Any]]:
    """Read recent staged logs and retain records with recognized errors.

    Args:
        client_bq: BigQuery client used to query staged logs.
        time_parameter: Positive size of the detection window.
        time_pattern: Supported BigQuery interval unit (``MINUTE`` or
            ``HOUR``).

    Returns:
        Matching staged records with normalized error codes and identifiers.
    """
    query = f"""
        SELECT
            site_id,
            asset_id,
            device_name,
            log_source,
            log_level,
            log_message,
            log_timestamp_utc,
            error_codes,
            error_count,
            created_at
        FROM `{STAGE_TABLE}`
        WHERE log_timestamp_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {time_parameter} {time_pattern})
        ORDER BY log_timestamp_utc DESC
    """
    results = []
    for row in client_bq.query(query).result():
        message = row.get("log_message") or ""
        matches = detect_error_patterns(message)
        if not matches:
            continue
        results.append(
            {
                "site_id": row.get("site_id") or "UNKNOWN",
                "asset_id": row.get("asset_id") or "UNKNOWN",
                "device_name": row.get("device_name") or "UNKNOWN",
                "log_source": row.get("log_source") or "kepware",
                "log_level": row.get("log_level") or "INFO",
                "log_message": message,
                "log_timestamp_utc": row.get("log_timestamp_utc"),
                "error_codes": matches,
                "error_count": len(matches),
            }
        )
    return results


def _read_alerted_sites(client_bq: bigquery.Client) -> set[Tuple[str, str]]:
    """Read site/device pairs already alerted on the current UTC date.

    Args:
        client_bq: BigQuery client used to query the deduplication table.

    Returns:
        A set of site/device pairs. An empty set is returned if the query
        fails.
    """
    dedup_query = f"""
        SELECT DISTINCT site_id, device_name
        FROM `{DEDUP_TABLE}`
        WHERE alert_date = CURRENT_DATE('UTC')
    """
    alerted = set()
    try:
        for row in client_bq.query(dedup_query).result():
            site_id = str(row.get("site_id") or "UNKNOWN").strip()
            device_name = str(row.get("device_name") or "UNKNOWN").strip()
            alerted.add((site_id, device_name))
    except Exception:
        return set()
    return alerted


def detect_kepware_alerts(data: Dict[str, Any], context: Any = None) -> str:
    """Emit Cloud Logging alerts for recent Kepware errors.

    The function groups matching records by site and device, skips pairs
    already alerted on the current UTC date, and writes deduplication records
    for alerts emitted when Cloud Logging is enabled.

    Args:
        data: Cloud Function event payload; accepted for handler compatibility.
        context: Cloud Function event context; accepted for handler
            compatibility and otherwise unused.

    Returns:
        ``success`` after processing the current detection window.
    """
    client_bq = bigquery.Client()
    client_log = cloud_logging.Client()
    time_parameter, time_pattern = _get_alert_window()
    current_timestamp_utc = _utc_now().strftime("%Y-%m-%d-%H:%M:%S")
    current_date_utc = _utc_now().strftime("%Y-%m-%d")

    update_flag = _read_flag_value(client_bq)
    print(f"update_flag_cloud_logger={update_flag}")

    recent_rows = _read_recent_stage_rows(client_bq, time_parameter, time_pattern)
    if not recent_rows:
        print("No Kepware error patterns detected in the current scheduler window.")
        return "success"

    alerted_today = _read_alerted_sites(client_bq)
    site_device_groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

    for row in recent_rows:
        site_id = str(row.get("site_id") or "UNKNOWN").strip()
        device_name = str(row.get("device_name") or "UNKNOWN").strip()
        key = (site_id, device_name)
        if key not in site_device_groups:
            site_device_groups[key] = []
        site_device_groups[key].append(row)

    to_alert = []
    for key, rows in site_device_groups.items():
        if key in alerted_today:
            continue
        to_alert.append((key, rows))

    if not to_alert:
        print("All affected site/device combinations were already alerted today. No new cloud log alert written.")
        return "success"

    alert_emitted = False
    dedup_rows: List[Dict[str, Any]] = []

    if update_flag == "Y":
        logger = client_log.logger(LOGGER_NAME)

        for (site_id, device_name), rows in to_alert:
            unique_codes = sorted({code for row in rows for code in row["error_codes"]})
            detail_lines = []
            for row in rows:
                timestamp = row.get("log_timestamp_utc")
                timestamp_value = timestamp.strftime("%Y-%m-%d %H:%M:%S %Z") if hasattr(timestamp, "strftime") else str(timestamp)
                detail_lines.append(
                    f"{timestamp_value};{row.get('log_level')};{row.get('log_source')};{row.get('log_message')}"
                )

            log_payload = {
                "key": "ALERT_KEPWARE_LOG_ERRORS",
                "site_id": site_id,
                "device_name": device_name,
                "error_codes": unique_codes,
                "error_count": len(rows),
                "detection_window": f"Last {time_parameter} {time_pattern}",
                "details": "||".join(detail_lines[:50]),
                "alert_date": current_date_utc,
                "log_created_timestamp": current_timestamp_utc,
            }
            logger.log_struct(log_payload, severity="WARNING")
            print(f"Cloud logging alert emitted for site={site_id}, device={device_name}, codes={unique_codes}")

            dedup_rows.append(
                {
                    "site_id": site_id,
                    "device_name": device_name,
                    "alert_date": current_date_utc,
                    "sent_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "error_count": len(rows),
                }
            )
            alert_emitted = True

        print(f"Custom log message(s) successfully loaded into Cloud Logger: {len(to_alert)}")
    else:
        print("Cloud logger update skipped because update_flag_cloud_logger is not set to 'Y'.")

    if alert_emitted:
        try:
            errors = client_bq.insert_rows_json(DEDUP_TABLE, dedup_rows)
            if errors:
                print(f"WARNING: Error writing dedup records: {errors}")
            else:
                print(f"Dedup records written successfully for {len(dedup_rows)} site/device alert(s).")
        except Exception as exc:  # pragma: no cover - defensive path
            print(f"WARNING: Failed to write dedup rows: {exc}")

    return "success"


# Cloud Function entrypoint for Pub/Sub-triggered Kepware log alert processing.
def process_kepware_log_alerts(data: Dict[str, Any], context: Any = None) -> str:
    """Cloud Function entry point for scheduled Kepware alert processing.

    Args:
        data: Cloud Function event payload.
        context: Cloud Function event context.

    Returns:
        The result returned by :func:`detect_kepware_alerts`.
    """
    return detect_kepware_alerts(data, context)
