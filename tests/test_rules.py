"""Unit tests for transform/rules.py. Run with: pytest tests/ -v (no AWS access needed)."""

import pandas as pd
import pytest

from transform import rules

JAN = pd.Timestamp("2025-01-01")


def trips(*rows):
    """Build a trip DataFrame from (pickup, dropoff, fare) tuples, with the other required columns filled."""
    df = pd.DataFrame(rows, columns=["tpep_pickup_datetime", "tpep_dropoff_datetime", "fare_amount"])
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    df["total_amount"] = df["fare_amount"].fillna(0) + 2.0
    df["trip_distance"] = 1.5
    df["PULocationID"] = 161
    df["DOLocationID"] = 236
    df["cbd_congestion_fee"] = 0.75
    return df


def reasons(df):
    """Reject reasons as a list, with None for kept rows."""
    return [None if pd.isna(r) else r for r in rules.assign_reject_reason(df, JAN)["reject_reason"]]


def zones():
    return pd.DataFrame(
        {
            "LocationID": [161, 236, 132],
            "Borough": ["Manhattan", "Manhattan", "Queens"],
            "Zone": ["Midtown Center", "Upper East Side North", "JFK Airport"],
            "service_zone": ["Yellow Zone", "Yellow Zone", "Airports"],
        }
    )


# --- cleaning rules ---------------------------------------------------------------

def test_dropoff_before_pickup_is_rejected():
    df = trips(("2025-01-06 09:00", "2025-01-06 08:50", 12.0))
    assert reasons(df) == ["dropoff_before_pickup"]


def test_zero_and_negative_fares_are_rejected():
    df = trips(
        ("2025-01-07 14:00", "2025-01-07 14:15", 0.0),
        ("2025-01-07 15:00", "2025-01-07 15:15", -5.0),
    )
    assert reasons(df) == ["non_positive_fare", "non_positive_fare"]


def test_pickup_outside_month_is_rejected_and_boundaries_are_correct():
    df = trips(
        ("2024-12-31 23:59", "2025-01-01 00:10", 18.0),  # before the month
        ("2025-02-01 00:00", "2025-02-01 00:10", 18.0),  # first second of next month
        ("2025-01-31 23:59", "2025-02-01 00:10", 18.0),  # last minute of the month: kept
    )
    assert reasons(df) == ["pickup_outside_month", "pickup_outside_month", None]


def test_missing_fare_is_rejected():
    df = trips(("2025-01-05 10:00", "2025-01-05 10:20", None))
    assert reasons(df) == ["missing_required_value"]


def test_missing_timestamp_is_rejected():
    df = trips((None, "2025-01-05 10:20", 10.0))
    assert reasons(df) == ["missing_required_value"]


def test_row_failing_two_rules_gets_only_the_first_reason():
    df = trips(("2025-01-06 09:00", "2025-01-06 08:50", None))  # missing fare AND dropoff before pickup
    assert reasons(df) == ["missing_required_value"]


def test_dropoff_equal_to_pickup_is_kept():
    df = trips(("2025-01-05 10:00", "2025-01-05 10:00", 10.0))
    assert reasons(df) == [None]


def test_valid_row_is_kept_unchanged():
    df = trips(("2025-01-05 10:00", "2025-01-05 10:20", 15.0))
    curated, quarantine = rules.split(rules.assign_reject_reason(df, JAN))
    assert len(quarantine) == 0
    pd.testing.assert_frame_equal(curated, df)  # every column and value unchanged, incl. cbd_congestion_fee


def test_non_datetime_columns_fail_loudly():
    df = trips(("2025-01-05 10:00", "2025-01-05 10:20", 15.0))
    df["tpep_pickup_datetime"] = df["tpep_pickup_datetime"].astype(str)
    with pytest.raises(TypeError):
        rules.assign_reject_reason(df, JAN)


# --- split ---------------------------------------------------------------------------

def test_split_keeps_every_row_exactly_once():
    df = trips(
        ("2025-01-05 10:00", "2025-01-05 10:20", 15.0),
        ("2025-01-06 09:00", "2025-01-06 08:50", 12.0),
        ("2025-01-07 14:00", "2025-01-07 14:15", 0.0),
        ("2025-01-08 16:00", "2025-01-08 16:30", 22.0),
    )
    curated, quarantine = rules.split(rules.assign_reject_reason(df, JAN))
    assert len(curated) + len(quarantine) == len(df)
    assert (len(curated), len(quarantine)) == (2, 2)
    assert "reject_reason" not in curated.columns
    assert quarantine["reject_reason"].notna().all()


def test_reason_counts_include_zero_reasons():
    df = trips(("2025-01-06 09:00", "2025-01-06 08:50", 12.0))
    _, quarantine = rules.split(rules.assign_reject_reason(df, JAN))
    assert rules.reason_counts(quarantine) == {
        "missing_required_value": 0,
        "dropoff_before_pickup": 1,
        "non_positive_fare": 0,
        "pickup_outside_month": 0,
    }


# --- zone join -----------------------------------------------------------------------

def test_add_zones_adds_names_and_marks_unknown_ids():
    df = trips(("2025-01-05 10:00", "2025-01-05 10:20", 15.0), ("2025-01-05 11:00", "2025-01-05 11:20", 15.0))
    df.loc[1, "PULocationID"] = 999  # not in the lookup
    out = rules.add_zones(df, zones())
    assert len(out) == len(df)
    assert list(out["pickup_borough"]) == ["Manhattan", "Unknown"]
    assert list(out["pickup_zone"]) == ["Midtown Center", "Unknown"]
    assert list(out["dropoff_zone"]) == ["Upper East Side North", "Upper East Side North"]


def test_add_zones_rejects_duplicate_location_ids():
    dup = pd.concat([zones(), zones().iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError):
        rules.add_zones(trips(("2025-01-05 10:00", "2025-01-05 10:20", 15.0)), dup)


# --- summary -------------------------------------------------------------------------

def test_build_summary_counts_sums_and_averages():
    df = trips(
        ("2025-01-05 10:00", "2025-01-05 10:20", 10.0),
        ("2025-01-05 12:00", "2025-01-05 12:20", 20.0),
        ("2025-01-06 09:00", "2025-01-06 09:20", 30.0),
    )
    df["trip_distance"] = [1.0, 3.0, 5.0]
    summary = rules.build_summary(rules.add_zones(df, zones()))
    jan5 = summary[summary["trip_date"] == pd.Timestamp("2025-01-05").date()].iloc[0]
    assert jan5["pickup_borough"] == "Manhattan"
    assert jan5["trip_count"] == 2
    assert jan5["total_revenue"] == pytest.approx(34.0)   # (10+2) + (20+2)
    assert jan5["avg_trip_distance"] == pytest.approx(2.0)
    assert len(summary) == 2


# --- count check and file names ------------------------------------------------------

def test_check_counts_passes_when_balanced():
    rules.check_counts(100, 95, 5)


def test_check_counts_raises_on_mismatch():
    with pytest.raises(rules.RowCountMismatchError):
        rules.check_counts(100, 95, 4)


def test_parse_month_from_valid_key():
    assert rules.parse_month_from_key("raw/yellow_tripdata/yellow_tripdata_2025-01.parquet") == ("2025", "01")


@pytest.mark.parametrize(
    "key",
    [
        "raw/yellow_tripdata/bad_name.parquet",
        "raw/yellow_tripdata/yellow_tripdata_2025-13.parquet",
        "raw/taxi_zone_lookup/taxi_zone_lookup.csv",
        "curated/yellow_tripdata_2025-01.parquet",
    ],
)
def test_parse_month_rejects_invalid_keys(key):
    with pytest.raises(ValueError):
        rules.parse_month_from_key(key)


def test_month_bounds():
    assert rules.month_bounds("2025", "12") == (pd.Timestamp("2025-12-01"), pd.Timestamp("2026-01-01"))
