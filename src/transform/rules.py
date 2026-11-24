# """Cleaning rules, zone join and daily summary for the NYC taxi pipeline.

# Pure functions only: no AWS calls and no I/O, so everything here can be unit-tested
# locally (see tests/test_rules.py) and later ported to Spark if volumes grow.

# Design reference: docs/technical_design.pdf, sections 4-6.
# """

# from __future__ import annotations

# import re

# import numpy as np
# import pandas as pd

# PICKUP = "tpep_pickup_datetime"
# DROPOFF = "tpep_dropoff_datetime"
# FARE = "fare_amount"

# #: Columns the transform relies on. Every other source column is carried through unchanged.
# REQUIRED_TRIP_COLUMNS: tuple[str, ...] = (
#     PICKUP,
#     DROPOFF,
#     FARE,
#     "total_amount",
#     "trip_distance",
#     "PULocationID",
#     "DOLocationID",
# )
# REQUIRED_ZONE_COLUMNS: tuple[str, ...] = ("LocationID", "Borough", "Zone")

# #: Reject reasons, in the order the rules are evaluated. A row gets the FIRST reason that applies.
# REJECT_REASONS: tuple[str, ...] = (
#     "missing_required_value",
#     "dropoff_before_pickup",
#     "non_positive_fare",
#     "pickup_outside_month",
# )
# REASON_COLUMN = "reject_reason"
# UNKNOWN = "Unknown"
# ZONE_OUTPUT_COLUMNS: tuple[str, ...] = ("pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone")

# # raw/yellow_tripdata/yellow_tripdata_2025-01.parquet -> ("2025", "01")
# _TRIP_KEY_PATTERN = re.compile(r"^raw/yellow_tripdata/yellow_tripdata_(\d{4})-(0[1-9]|1[0-2])\.parquet$")


# class RowCountMismatchError(Exception):
#     """Raised when raw rows != curated rows + quarantined rows."""


# def parse_month_from_key(key: str) -> tuple[str, str]:
#     """Return (year, month) as strings from a raw trip file key.

#     The month always comes from the file name, never from the data, so each file
#     maps to exactly one output partition.

#     Raises:
#         ValueError: if the key is not ``raw/yellow_tripdata/yellow_tripdata_YYYY-MM.parquet``.
#     """
#     match = _TRIP_KEY_PATTERN.fullmatch(key)
#     if match is None:
#         raise ValueError(
#             f"Unexpected trip file key {key!r}; expected raw/yellow_tripdata/yellow_tripdata_YYYY-MM.parquet"
#         )
#     return match.group(1), match.group(2)


# def month_bounds(year: str, month: str) -> tuple[pd.Timestamp, pd.Timestamp]:
#     """Return (start of month, start of next month); the first is inclusive, the second exclusive."""
#     start = pd.Timestamp(year=int(year), month=int(month), day=1)
#     return start, start + pd.offsets.MonthBegin(1)


# def validate_columns(df: pd.DataFrame, required: tuple[str, ...], name: str) -> None:
#     """Fail loudly if any required column is missing.

#     Raises:
#         ValueError: listing every missing column.
#     """
#     missing = [col for col in required if col not in df.columns]
#     if missing:
#         raise ValueError(f"{name} is missing required columns: {missing}")


# def assign_reject_reason(df: pd.DataFrame, month_start: pd.Timestamp) -> pd.DataFrame:
#     """Return a copy of ``df`` with a ``reject_reason`` column (None for valid rows).

#     Rules, evaluated in order (first match wins, so each row has at most one reason):
#       1. pickup, dropoff or fare is missing          -> missing_required_value
#       2. dropoff earlier than pickup                  -> dropoff_before_pickup
#       3. fare zero or negative                        -> non_positive_fare
#       4. pickup outside [month_start, next month)     -> pickup_outside_month

#     Kept on purpose: dropoff equal to pickup.

#     Raises:
#         TypeError: if the pickup/dropoff columns are not datetimes.
#     """
#     for col in (PICKUP, DROPOFF):
#         if not pd.api.types.is_datetime64_any_dtype(df[col]):
#             raise TypeError(f"Column {col!r} must be a datetime, got {df[col].dtype}")

#     next_month = month_start + pd.offsets.MonthBegin(1)
#     pickup, dropoff, fare = df[PICKUP], df[DROPOFF], df[FARE]

#     conditions = [
#         (pickup.isna() | dropoff.isna() | fare.isna()).to_numpy(),
#         (dropoff < pickup).to_numpy(),
#         (fare <= 0).to_numpy(),
#         ((pickup < month_start) | (pickup >= next_month)).to_numpy(),
#     ]
#     out = df.copy()
#     # object dtype keeps None for valid rows on both pandas 2.x and 3.x
#     out[REASON_COLUMN] = pd.Series(
#         np.select(conditions, list(REJECT_REASONS), default=None), index=df.index, dtype=object
#     )
#     return out


# def split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
#     """Split rows into (curated, quarantine).

#     Curated rows drop the ``reject_reason`` column; quarantined rows keep it.
#     Nothing is deleted: every input row ends up in exactly one of the two.
#     """
#     rejected = df[REASON_COLUMN].notna()
#     curated = df.loc[~rejected].drop(columns=REASON_COLUMN).reset_index(drop=True)
#     quarantine = df.loc[rejected].reset_index(drop=True)
#     return curated, quarantine


# def add_zones(curated: pd.DataFrame, zones: pd.DataFrame) -> pd.DataFrame:
#     """Add pickup/dropoff borough and zone names from the zone lookup.

#     Uses left joins so every trip is kept; IDs not found in the lookup get ``"Unknown"``.

#     Raises:
#         ValueError: if the lookup has duplicate LocationIDs (the join would duplicate trips).
#         AssertionError: if the join changed the row count (should be impossible after the check above).
#     """
#     validate_columns(zones, REQUIRED_ZONE_COLUMNS, "Zone lookup")
#     if zones["LocationID"].duplicated().any():
#         raise ValueError("Zone lookup has duplicate LocationID values; the join would duplicate trips")

#     lookup = zones[list(REQUIRED_ZONE_COLUMNS)]
#     pickup_zones = lookup.rename(
#         columns={"LocationID": "PULocationID", "Borough": "pickup_borough", "Zone": "pickup_zone"}
#     )
#     dropoff_zones = lookup.rename(
#         columns={"LocationID": "DOLocationID", "Borough": "dropoff_borough", "Zone": "dropoff_zone"}
#     )

#     out = curated.merge(pickup_zones, on="PULocationID", how="left")
#     out = out.merge(dropoff_zones, on="DOLocationID", how="left")
#     cols = list(ZONE_OUTPUT_COLUMNS)
#     out[cols] = out[cols].fillna(UNKNOWN)

#     assert len(out) == len(curated), "Zone join changed the row count"
#     return out


# def build_summary(curated: pd.DataFrame) -> pd.DataFrame:
#     """Daily trip count, total revenue (sum of total_amount) and average trip distance by pickup borough."""
#     return (
#         curated.assign(trip_date=curated[PICKUP].dt.date)
#         .groupby(["trip_date", "pickup_borough"], as_index=False)
#         .agg(
#             trip_count=(PICKUP, "size"),
#             total_revenue=("total_amount", "sum"),
#             avg_trip_distance=("trip_distance", "mean"),
#         )
#         .sort_values(["trip_date", "pickup_borough"], ignore_index=True)
#     )


# def reason_counts(quarantine: pd.DataFrame) -> dict[str, int]:
#     """Count quarantined rows per reason, including reasons with zero rows."""
#     counts = quarantine[REASON_COLUMN].value_counts().reindex(list(REJECT_REASONS), fill_value=0)
#     return {reason: int(n) for reason, n in counts.items()}


# def check_counts(raw_rows: int, curated_rows: int, quarantined_rows: int) -> None:
#     """Verify raw = curated + quarantined.

#     Raises:
#         RowCountMismatchError: with all three numbers, so the run fails loudly.
#     """
#     if raw_rows != curated_rows + quarantined_rows:
#         raise RowCountMismatchError(
#             f"Row count mismatch: raw={raw_rows}, curated={curated_rows}, "
#             f"quarantined={quarantined_rows}, difference={raw_rows - curated_rows - quarantined_rows}"
#         )
"""Cleaning rules, zone join and daily summary for the NYC taxi pipeline.

Pure functions only: no AWS calls and no I/O, so everything here can be
unit-tested locally (see tests/test_rules.py) and later ported to Spark
if volumes grow.

Design reference: docs/technical_design.pdf, sections 4-6.
"""

from __future__ import annotations

import re

import pandas as pd


PICKUP = "tpep_pickup_datetime"
DROPOFF = "tpep_dropoff_datetime"
FARE = "fare_amount"


# Columns required by the transformation.
REQUIRED_TRIP_COLUMNS: tuple[str, ...] = (
    PICKUP,
    DROPOFF,
    FARE,
    "total_amount",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
)

REQUIRED_ZONE_COLUMNS: tuple[str, ...] = (
    "LocationID",
    "Borough",
    "Zone",
)


# Reject reasons in evaluation order.
# The first matching reason wins.
REJECT_REASONS: tuple[str, ...] = (
    "missing_required_value",
    "dropoff_before_pickup",
    "non_positive_fare",
    "pickup_outside_month",
)

REASON_COLUMN = "reject_reason"
UNKNOWN = "Unknown"

ZONE_OUTPUT_COLUMNS: tuple[str, ...] = (
    "pickup_borough",
    "pickup_zone",
    "dropoff_borough",
    "dropoff_zone",
)


# raw/yellow_tripdata/yellow_tripdata_2025-01.parquet
# -> ("2025", "01")
_TRIP_KEY_PATTERN = re.compile(
    r"^raw/yellow_tripdata/"
    r"yellow_tripdata_(\d{4})-(0[1-9]|1[0-2])\.parquet$"
)


class RowCountMismatchError(Exception):
    """Raised when raw rows != curated rows + quarantined rows."""


def parse_month_from_key(key: str) -> tuple[str, str]:
    """Return (year, month) from a raw trip file key."""

    match = _TRIP_KEY_PATTERN.fullmatch(key)

    if match is None:
        raise ValueError(
            f"Unexpected trip file key {key!r}; expected "
            "raw/yellow_tripdata/yellow_tripdata_YYYY-MM.parquet"
        )

    return match.group(1), match.group(2)


def month_bounds(
    year: str,
    month: str,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return start of month and start of next month."""

    start = pd.Timestamp(
        year=int(year),
        month=int(month),
        day=1,
    )

    return start, start + pd.offsets.MonthBegin(1)


def validate_columns(
    df: pd.DataFrame,
    required: tuple[str, ...],
    name: str,
) -> None:
    """Fail if any required column is missing."""

    missing = [
        column
        for column in required
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            f"{name} is missing required columns: {missing}"
        )


def assign_reject_reason(
    df: pd.DataFrame,
    month_start: pd.Timestamp,
) -> pd.DataFrame:
    """Add reject_reason directly to the existing DataFrame.

    Rules are evaluated in order. A row receives only the first
    applicable rejection reason.

    Mutating the existing DataFrame avoids creating another complete
    copy of the multi-million-row input.
    """

    for column in (PICKUP, DROPOFF):
        if not pd.api.types.is_datetime64_any_dtype(df[column]):
            raise TypeError(
                f"Column {column!r} must be a datetime, "
                f"got {df[column].dtype}"
            )

    next_month = month_start + pd.offsets.MonthBegin(1)

    pickup = df[PICKUP]
    dropoff = df[DROPOFF]
    fare = df[FARE]

    # Pandas StringDtype avoids a large Python-object NumPy array.
    df[REASON_COLUMN] = pd.Series(
        pd.NA,
        index=df.index,
        dtype="string",
    )

    # 1. Missing required value
    mask = (
        pickup.isna()
        | dropoff.isna()
        | fare.isna()
    )

    df.loc[
        mask,
        REASON_COLUMN,
    ] = "missing_required_value"

    # 2. Dropoff before pickup
    mask = (
        df[REASON_COLUMN].isna()
        & (dropoff < pickup)
    )

    df.loc[
        mask,
        REASON_COLUMN,
    ] = "dropoff_before_pickup"

    # 3. Non-positive fare
    mask = (
        df[REASON_COLUMN].isna()
        & (fare <= 0)
    )

    df.loc[
        mask,
        REASON_COLUMN,
    ] = "non_positive_fare"

    # 4. Pickup outside source month
    mask = (
        df[REASON_COLUMN].isna()
        & (
            (pickup < month_start)
            | (pickup >= next_month)
        )
    )

    df.loc[
        mask,
        REASON_COLUMN,
    ] = "pickup_outside_month"

    return df


def split(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split rows into curated and quarantine DataFrames."""

    rejected = df[REASON_COLUMN].notna()

    curated = (
        df.loc[~rejected]
        .drop(columns=REASON_COLUMN)
        .reset_index(drop=True)
    )

    quarantine = (
        df.loc[rejected]
        .reset_index(drop=True)
    )

    return curated, quarantine


def add_zones(
    curated: pd.DataFrame,
    zones: pd.DataFrame,
) -> pd.DataFrame:
    """Add pickup/dropoff borough and zone names.

    Uses Series.map instead of two DataFrame merges.

    This significantly reduces peak memory usage because merge()
    can create another complete multi-million-row DataFrame.
    """

    validate_columns(
        zones,
        REQUIRED_ZONE_COLUMNS,
        "Zone lookup",
    )

    if zones["LocationID"].duplicated().any():
        raise ValueError(
            "Zone lookup has duplicate LocationID values; "
            "the lookup would produce ambiguous results"
        )

    lookup = zones.set_index("LocationID")

    borough_by_id = lookup["Borough"]
    zone_by_id = lookup["Zone"]

    curated["pickup_borough"] = (
        curated["PULocationID"]
        .map(borough_by_id)
        .fillna(UNKNOWN)
    )

    curated["pickup_zone"] = (
        curated["PULocationID"]
        .map(zone_by_id)
        .fillna(UNKNOWN)
    )

    curated["dropoff_borough"] = (
        curated["DOLocationID"]
        .map(borough_by_id)
        .fillna(UNKNOWN)
    )

    curated["dropoff_zone"] = (
        curated["DOLocationID"]
        .map(zone_by_id)
        .fillna(UNKNOWN)
    )

    return curated


def build_summary(
    curated: pd.DataFrame,
) -> pd.DataFrame:
    """Build daily metrics grouped by pickup borough.

    Keeps trip_date as datetime64 instead of converting millions of
    timestamps into Python date objects.
    """

    summary_source = pd.DataFrame(
        {
            "trip_date": curated[PICKUP].dt.floor("D"),
            "pickup_borough": curated["pickup_borough"],
            PICKUP: curated[PICKUP],
            "total_amount": curated["total_amount"],
            "trip_distance": curated["trip_distance"],
        }
    )

    summary = (
        summary_source
        .groupby(
            ["trip_date", "pickup_borough"],
            as_index=False,
            observed=True,
        )
        .agg(
            trip_count=(PICKUP, "size"),
            total_revenue=("total_amount", "sum"),
            avg_trip_distance=("trip_distance", "mean"),
        )
        .sort_values(
            ["trip_date", "pickup_borough"],
            ignore_index=True,
        )
    )

    return summary


def reason_counts(
    quarantine: pd.DataFrame,
) -> dict[str, int]:
    """Count quarantined rows for every rejection reason."""

    counts = (
        quarantine[REASON_COLUMN]
        .value_counts()
        .reindex(
            list(REJECT_REASONS),
            fill_value=0,
        )
    )

    return {
        reason: int(count)
        for reason, count in counts.items()
    }


def check_counts(
    raw_rows: int,
    curated_rows: int,
    quarantined_rows: int,
) -> None:
    """Verify raw rows = curated rows + quarantined rows."""

    if raw_rows != curated_rows + quarantined_rows:
        raise RowCountMismatchError(
            f"Row count mismatch: "
            f"raw={raw_rows}, "
            f"curated={curated_rows}, "
            f"quarantined={quarantined_rows}, "
            f"difference="
            f"{raw_rows - curated_rows - quarantined_rows}"
        )