# """Lambda entry point for the NYC taxi pipeline.

# Triggered by an EventBridge rule when a trip file lands in raw/yellow_tripdata/.
# All AWS I/O lives here; the data logic lives in rules.py.

# Steps (docs/technical_design.pdf, section 6):
#   1. Read bucket and key from the event; parse year and month from the file name
#   2. Read the trip file and the zone lookup; validate columns
#   3-6. Apply rules, split, join zones, build summary
#   7. Write curated/, quarantine/, summary/ (overwrite this month only) and update the Glue Catalog
#   8. Count check: raw == curated + quarantined, read back from the written files; else raise
#   9. Write the manifest (only after the check passes)

# Configuration comes from environment variables (BUCKET, GLUE_DATABASE, LOG_LEVEL).
# No credentials in code: the function runs with its IAM execution role.
# """

# from __future__ import annotations

# import json
# import logging
# import os
# import time
# from datetime import datetime, timezone
# from typing import Any

# import awswrangler as wr
# import boto3
# import pandas as pd

# from transform import rules

# ZONE_LOOKUP_KEY = "raw/taxi_zone_lookup/taxi_zone_lookup.csv"
# COUNT_COLUMN = rules.PICKUP  # a column present in curated and quarantine; read back to count rows

# DATASETS = {
#     "curated": ("curated/yellow_trips", "yellow_trips_curated"),
#     "quarantine": ("quarantine/yellow_trips", "yellow_trips_quarantine"),
#     "summary": ("summary/daily_borough", "daily_borough_summary"),
# }

# logger = logging.getLogger()
# logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# _s3 = boto3.client("s3")


# def _log(step: str, **fields: Any) -> None:
#     """Structured JSON log line. Only names, counts and timings; never row-level data."""
#     logger.info(json.dumps({"step": step, **fields}, default=str))


# def _get_config() -> tuple[str, str]:
#     """Read required configuration from the environment; fail loudly if missing."""
#     try:
#         return os.environ["BUCKET"], os.environ["GLUE_DATABASE"]
#     except KeyError as missing:
#         raise RuntimeError(f"Missing required environment variable: {missing}") from None


# def _parse_event(event: dict[str, Any], expected_bucket: str) -> str:
#     """Return the object key from an EventBridge S3 'Object Created' event.

#     Raises:
#         ValueError: if the event is not in the expected shape or is for another bucket.
#     """
#     try:
#         bucket = event["detail"]["bucket"]["name"]
#         key = event["detail"]["object"]["key"]
#     except (KeyError, TypeError):
#         raise ValueError("Event is not an EventBridge S3 'Object Created' event") from None
#     if bucket != expected_bucket:
#         raise ValueError(f"Event is for bucket {bucket!r}, expected {expected_bucket!r}")
#     return key


# def _write_dataset(
#     df: pd.DataFrame, name: str, bucket: str, database: str, year: str, month: str
# ) -> list[str]:
#     """Write one dataset for one month, replacing only that month's partition.

#     Returns the S3 paths written in this run (empty if there were no rows).
#     """
#     folder, table = DATASETS[name]
#     base_path = f"s3://{bucket}/{folder}/"

#     if df.empty:
#         # awswrangler refuses to write an empty DataFrame, so clear the month explicitly.
#         # Otherwise files from an earlier run of this month would remain (and be double counted).
#         wr.s3.delete_objects(f"{base_path}year={year}/month={month}/")
#         _log("write_skipped_empty", dataset=name, year=year, month=month)
#         return []

#     result = wr.s3.to_parquet(
#         df=df.assign(year=year, month=month),
#         path=base_path,
#         dataset=True,
#         partition_cols=["year", "month"],
#         mode="overwrite_partitions",   # replace this month only; other months untouched
#         database=database,
#         table=table,
#         compression="snappy",
#     )
#     paths = result["paths"]
#     _log("write_done", dataset=name, table=table, files=len(paths), rows=len(df))
#     return paths


# def _count_written_rows(paths: list[str]) -> int:
#     """Count rows in the files actually written, by reading back one column."""
#     if not paths:
#         return 0
#     return len(wr.s3.read_parquet(path=paths, columns=[COUNT_COLUMN]))


# def _write_manifest(bucket: str, year: str, month: str, manifest: dict[str, Any]) -> str:
#     key = f"manifests/yellow_tripdata_{year}-{month}/_manifest.json"
#     _s3.put_object(
#         Bucket=bucket,
#         Key=key,
#         Body=json.dumps(manifest, indent=2).encode("utf-8"),
#         ContentType="application/json",
#     )
#     return key


# def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
#     """Process one monthly trip file end to end. Raises on any failure so the alarm fires."""
#     started = time.monotonic()
#     bucket, database = _get_config()

#     # 1. Which file and which month
#     key = _parse_event(event, bucket)
#     year, month = rules.parse_month_from_key(key)
#     month_start, _ = rules.month_bounds(year, month)
#     _log("start", key=key, year=year, month=month)

#     # 2. Read inputs
#     if not wr.s3.does_object_exist(f"s3://{bucket}/{ZONE_LOOKUP_KEY}"):
#         raise FileNotFoundError(f"Zone lookup not found at s3://{bucket}/{ZONE_LOOKUP_KEY}; upload it first")
#     # keep_default_na=False: the lookup uses the literal text "N/A" (zones 264/265); pandas would
#     # otherwise turn it into a missing value and the join would relabel it "Unknown".
#     zones = wr.s3.read_csv(f"s3://{bucket}/{ZONE_LOOKUP_KEY}", keep_default_na=False, na_values=[""])
#     rules.validate_columns(zones, rules.REQUIRED_ZONE_COLUMNS, "Zone lookup")

#     trips = wr.s3.read_parquet(path=f"s3://{bucket}/{key}")
#     rules.validate_columns(trips, rules.REQUIRED_TRIP_COLUMNS, "Trip file")
#     raw_rows = len(trips)
#     _log("read_done", raw_rows=raw_rows, zone_rows=len(zones))

#     # 3-6. Clean, split, enrich, summarise
#     flagged = rules.assign_reject_reason(trips, month_start)
#     del trips  # free memory before the copies made by split()
#     curated, quarantine = rules.split(flagged)
#     del flagged
#     reasons = rules.reason_counts(quarantine)
#     curated = rules.add_zones(curated, zones)
#     summary = rules.build_summary(curated)
#     _log("transform_done", curated_rows=len(curated), quarantined_rows=len(quarantine), reasons=reasons)

#     # 7. Write outputs (this month only) and update the Glue Catalog
#     curated_paths = _write_dataset(curated, "curated", bucket, database, year, month)
#     quarantine_paths = _write_dataset(quarantine, "quarantine", bucket, database, year, month)
#     _write_dataset(summary, "summary", bucket, database, year, month)

#     # 8. Count check against what was actually written
#     curated_rows = _count_written_rows(curated_paths)
#     quarantined_rows = _count_written_rows(quarantine_paths)
#     rules.check_counts(raw_rows, curated_rows, quarantined_rows)
#     _log("count_check_passed", raw_rows=raw_rows, curated_rows=curated_rows, quarantined_rows=quarantined_rows)

#     # 9. Manifest: written only on success
#     manifest = {
#         "source_key": key,
#         "run_id": getattr(context, "aws_request_id", None),
#         "finished_at": datetime.now(timezone.utc).isoformat(),
#         "raw_rows": raw_rows,
#         "curated_rows": curated_rows,
#         "quarantined_rows": quarantined_rows,
#         "count_check": "passed",
#         "reject_reasons": reasons,
#         "duration_seconds": round(time.monotonic() - started, 1),
#     }
#     manifest_key = _write_manifest(bucket, year, month, manifest)
#     _log("done", manifest_key=manifest_key, duration_seconds=manifest["duration_seconds"])
#     return manifest
"""Lambda entry point for the NYC taxi pipeline.

Triggered by an EventBridge rule when a trip file lands in
raw/yellow_tripdata/.

All AWS I/O lives here; the data logic lives in rules.py.

Processing steps:
  1. Read bucket and key from the EventBridge event.
  2. Parse year and month from the source filename.
  3. Read the trip file and zone lookup.
  4. Apply validation and quarantine rules.
  5. Write quarantine and release it from memory.
  6. Enrich curated rows with zone information.
  7. Build and write the daily borough summary.
  8. Write curated data.
  9. Read back written files and verify row counts.
 10. Write the manifest only after successful validation.

Configuration comes from environment variables:
  BUCKET
  GLUE_DATABASE
  LOG_LEVEL

The function uses its IAM execution role.
No AWS credentials are stored in code.
"""

from __future__ import annotations

import gc
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import awswrangler as wr
import boto3
import pandas as pd

from transform import rules


ZONE_LOOKUP_KEY = "raw/taxi_zone_lookup/taxi_zone_lookup.csv"

COUNT_COLUMN = rules.PICKUP

DATASETS = {
    "curated": (
        "curated/yellow_trips",
        "yellow_trips_curated",
    ),
    "quarantine": (
        "quarantine/yellow_trips",
        "yellow_trips_quarantine",
    ),
    "summary": (
        "summary/daily_borough",
        "daily_borough_summary",
    ),
}


logger = logging.getLogger()

logger.setLevel(
    os.environ.get(
        "LOG_LEVEL",
        "INFO",
    )
)

_s3 = boto3.client("s3")


def _log(
    step: str,
    **fields: Any,
) -> None:
    """Write one structured JSON log entry."""

    logger.info(
        json.dumps(
            {
                "step": step,
                **fields,
            },
            default=str,
        )
    )


def _get_config() -> tuple[str, str]:
    """Read required configuration from environment variables."""

    try:
        bucket = os.environ["BUCKET"]
        database = os.environ["GLUE_DATABASE"]

    except KeyError as missing:
        raise RuntimeError(
            f"Missing required environment variable: {missing}"
        ) from None

    return bucket, database


def _parse_event(
    event: dict[str, Any],
    expected_bucket: str,
) -> str:
    """Return the object key from an EventBridge S3 Object Created event."""

    try:
        bucket = event["detail"]["bucket"]["name"]
        key = event["detail"]["object"]["key"]

    except (KeyError, TypeError):
        raise ValueError(
            "Event is not an EventBridge S3 'Object Created' event"
        ) from None

    if bucket != expected_bucket:
        raise ValueError(
            f"Event is for bucket {bucket!r}, "
            f"expected {expected_bucket!r}"
        )

    return key


def _write_dataset(
    df: pd.DataFrame,
    name: str,
    bucket: str,
    database: str,
    year: str,
    month: str,
) -> list[str]:
    """Write one monthly dataset and return the written S3 paths.

    Partition columns are added directly to the existing DataFrame
    instead of using df.assign(), which would create another complete
    copy of a potentially multi-million-row DataFrame.
    """

    folder, table = DATASETS[name]

    base_path = f"s3://{bucket}/{folder}/"

    if df.empty:
        wr.s3.delete_objects(
            f"{base_path}year={year}/month={month}/"
        )

        _log(
            "write_skipped_empty",
            dataset=name,
            year=year,
            month=month,
        )

        return []

    row_count = len(df)

    # Add partition columns in place.
    # Avoid df.assign(), which creates another DataFrame.
    df["year"] = year
    df["month"] = month

    result = wr.s3.to_parquet(
        df=df,
        path=base_path,
        dataset=True,
        partition_cols=[
            "year",
            "month",
        ],
        mode="overwrite_partitions",
        database=database,
        table=table,
        compression="snappy",

        # Lower peak memory usage.
        use_threads=False,
    )

    paths = result["paths"]

    _log(
        "write_done",
        dataset=name,
        table=table,
        files=len(paths),
        rows=row_count,
    )

    return paths


def _count_written_rows(
    paths: list[str],
) -> int:
    """Read back one column and count the rows actually written."""

    if not paths:
        return 0

    written = wr.s3.read_parquet(
        path=paths,
        columns=[COUNT_COLUMN],
        use_threads=False,
    )

    row_count = len(written)

    del written

    gc.collect()

    return row_count


def _write_manifest(
    bucket: str,
    year: str,
    month: str,
    manifest: dict[str, Any],
) -> str:
    """Write the success manifest to S3."""

    key = (
        f"manifests/"
        f"yellow_tripdata_{year}-{month}/"
        f"_manifest.json"
    )

    _s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(
            manifest,
            indent=2,
        ).encode("utf-8"),
        ContentType="application/json",
    )

    return key


def handler(
    event: dict[str, Any],
    context: Any,
) -> dict[str, Any]:
    """Process one monthly taxi trip file end to end."""

    started = time.monotonic()

    bucket, database = _get_config()

    # ---------------------------------------------------------
    # 1. Identify source file
    # ---------------------------------------------------------

    key = _parse_event(
        event,
        bucket,
    )

    year, month = rules.parse_month_from_key(
        key
    )

    month_start, _ = rules.month_bounds(
        year,
        month,
    )

    _log(
        "start",
        key=key,
        year=year,
        month=month,
    )

    # ---------------------------------------------------------
    # 2. Read zone lookup
    # ---------------------------------------------------------

    zone_lookup_path = (
        f"s3://{bucket}/{ZONE_LOOKUP_KEY}"
    )

    if not wr.s3.does_object_exist(
        zone_lookup_path
    ):
        raise FileNotFoundError(
            "Zone lookup not found at "
            f"{zone_lookup_path}; upload it first"
        )

    # keep_default_na=False keeps literal values such as "N/A".
    zones = wr.s3.read_csv(
        path=zone_lookup_path,
        keep_default_na=False,
        na_values=[""],
        use_threads=False,
    )

    rules.validate_columns(
        zones,
        rules.REQUIRED_ZONE_COLUMNS,
        "Zone lookup",
    )

    zone_rows = len(zones)

    # ---------------------------------------------------------
    # 3. Read source Parquet
    # ---------------------------------------------------------

    trips = wr.s3.read_parquet(
        path=f"s3://{bucket}/{key}",

        # Reduce simultaneous PyArrow buffers.
        use_threads=False,
    )

    rules.validate_columns(
        trips,
        rules.REQUIRED_TRIP_COLUMNS,
        "Trip file",
    )

    raw_rows = len(trips)

    _log(
        "read_done",
        raw_rows=raw_rows,
        zone_rows=zone_rows,
    )

    # ---------------------------------------------------------
    # 4. Apply rejection rules
    # ---------------------------------------------------------

    # Memory-optimized rules.py modifies trips directly.
    flagged = rules.assign_reject_reason(
        trips,
        month_start,
    )

    # trips and flagged currently reference the same DataFrame.
    del trips

    # ---------------------------------------------------------
    # 5. Split curated and quarantine
    # ---------------------------------------------------------

    curated, quarantine = rules.split(
        flagged
    )

    del flagged

    gc.collect()

    expected_curated_rows = len(curated)
    expected_quarantined_rows = len(quarantine)

    reasons = rules.reason_counts(
        quarantine
    )

    _log(
        "split_done",
        curated_rows=expected_curated_rows,
        quarantined_rows=expected_quarantined_rows,
        reasons=reasons,
    )

    # ---------------------------------------------------------
    # 6. Write quarantine first
    #
    # Quarantine is much smaller than curated.
    # Release it before working with the large curated set.
    # ---------------------------------------------------------

    quarantine_paths = _write_dataset(
        df=quarantine,
        name="quarantine",
        bucket=bucket,
        database=database,
        year=year,
        month=month,
    )

    del quarantine

    gc.collect()

    # ---------------------------------------------------------
    # 7. Add zone information
    # ---------------------------------------------------------

    curated = rules.add_zones(
        curated,
        zones,
    )

    del zones

    gc.collect()

    # ---------------------------------------------------------
    # 8. Build and write summary
    # ---------------------------------------------------------

    summary = rules.build_summary(
        curated
    )

    summary_rows = len(summary)

    _write_dataset(
        df=summary,
        name="summary",
        bucket=bucket,
        database=database,
        year=year,
        month=month,
    )

    del summary

    gc.collect()

    # ---------------------------------------------------------
    # 9. Write curated data last
    #
    # At this point quarantine, zones and summary have already
    # been released, leaving more memory for Parquet writing.
    # ---------------------------------------------------------

    curated_paths = _write_dataset(
        df=curated,
        name="curated",
        bucket=bucket,
        database=database,
        year=year,
        month=month,
    )

    del curated

    gc.collect()

    _log(
        "write_phase_done",
        expected_curated_rows=expected_curated_rows,
        expected_quarantined_rows=expected_quarantined_rows,
        summary_rows=summary_rows,
    )

    # ---------------------------------------------------------
    # 10. Count written quarantine rows
    # ---------------------------------------------------------

    quarantined_rows = _count_written_rows(
        quarantine_paths
    )

    del quarantine_paths

    gc.collect()

    # ---------------------------------------------------------
    # 11. Count written curated rows
    # ---------------------------------------------------------

    curated_rows = _count_written_rows(
        curated_paths
    )

    del curated_paths

    gc.collect()

    # ---------------------------------------------------------
    # 12. Validate counts
    # ---------------------------------------------------------

    rules.check_counts(
        raw_rows,
        curated_rows,
        quarantined_rows,
    )

    if curated_rows != expected_curated_rows:
        raise rules.RowCountMismatchError(
            "Curated write count mismatch: "
            f"expected={expected_curated_rows}, "
            f"written={curated_rows}"
        )

    if quarantined_rows != expected_quarantined_rows:
        raise rules.RowCountMismatchError(
            "Quarantine write count mismatch: "
            f"expected={expected_quarantined_rows}, "
            f"written={quarantined_rows}"
        )

    _log(
        "count_check_passed",
        raw_rows=raw_rows,
        curated_rows=curated_rows,
        quarantined_rows=quarantined_rows,
    )

    # ---------------------------------------------------------
    # 13. Write manifest only after successful validation
    # ---------------------------------------------------------

    duration_seconds = round(
        time.monotonic() - started,
        1,
    )

    manifest = {
        "source_key": key,
        "run_id": getattr(
            context,
            "aws_request_id",
            None,
        ),
        "finished_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "raw_rows": raw_rows,
        "curated_rows": curated_rows,
        "quarantined_rows": quarantined_rows,
        "summary_rows": summary_rows,
        "count_check": "passed",
        "reject_reasons": reasons,
        "duration_seconds": duration_seconds,
    }

    manifest_key = _write_manifest(
        bucket=bucket,
        year=year,
        month=month,
        manifest=manifest,
    )

    _log(
        "done",
        manifest_key=manifest_key,
        duration_seconds=duration_seconds,
    )

    return manifest