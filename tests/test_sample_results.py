"""Resultados da amostra sintética comparados com resultados esperados escritos à mão (sample/expected/).

Os valores esperados não vêm do código: foram derivados da tabela de casos em sample/CASOS.md.
"""

import pytest
from helpers import (
    SAMPLE_INPUT,
    SAMPLE_INPUT_CSV,
    SQL_DIR,
    ZONES,
    expected_projection,
    project,
    read_csv_rows,
)

from mobility_pipeline.pipeline import partition_dir, run_pipeline
from mobility_pipeline.query import current_run_dir, run_query
from mobility_pipeline.transform import (
    REASON_DROPOFF_BEFORE_PICKUP,
    REASON_MISSING_PICKUP_DATETIME,
    REASON_MISSING_PICKUP_LOCATION,
    REASON_UNKNOWN_PICKUP_ZONE,
)

SOURCE_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID",
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "Airport_fee",
    "cbd_congestion_fee",
]  # fmt: skip


@pytest.fixture(scope="module")
def published(spark, tmp_path_factory):
    output = tmp_path_factory.mktemp("sample") / "output"
    summary = run_pipeline(spark, SAMPLE_INPUT, ZONES, output, "2025-01")
    return output, summary, current_run_dir(partition_dir(output, "2025-01"))


def _table(spark, run_dir, name):
    return spark.read.parquet(str(run_dir / name))


def test_valid_records_match_expected(spark, published):
    _, _, run_dir = published
    columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "fare_amount",
               "pickup_date", "pickup_hour", "pu_borough", "pu_zone"]  # fmt: skip
    actual = project(_table(spark, run_dir, "valid"), columns)
    assert actual == expected_projection("valid_projection.csv", columns)


def test_valid_output_keeps_every_source_column(spark, published):
    _, _, run_dir = published
    columns = _table(spark, run_dir, "valid").columns
    assert set(SOURCE_COLUMNS) <= set(columns)
    assert {"pickup_date", "pickup_hour", "pu_borough", "pu_zone", "source_file"} <= set(columns)


def test_rejected_records_match_expected_with_reasons(spark, published):
    _, _, run_dir = published
    columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "reject_reasons"]
    actual = project(_table(spark, run_dir, "rejected"), columns)
    assert actual == expected_projection("rejected_projection.csv", columns)


def test_demand_table_matches_expected(spark, published):
    _, _, run_dir = published
    columns = ["pickup_date", "pickup_hour", "pu_location_id", "pu_borough", "pu_zone", "trip_count"]
    actual = project(_table(spark, run_dir, "demand_hourly"), columns)
    assert actual == expected_projection("demand_hourly.csv", columns)


def test_every_source_record_is_valid_or_rejected_exactly_once(spark, published):
    _, _, run_dir = published
    source_count = len(read_csv_rows(SAMPLE_INPUT_CSV))
    valid = _table(spark, run_dir, "valid")
    rejected = _table(spark, run_dir, "rejected")
    assert source_count == 23
    assert valid.count() + rejected.count() == source_count
    assert valid.count() == 16 and rejected.count() == 7


def test_demand_totals_equal_valid_records(spark, published):
    _, _, run_dir = published
    total = _table(spark, run_dir, "demand_hourly").agg({"trip_count": "sum"}).first()[0]
    assert total == _table(spark, run_dir, "valid").count() == 16


def test_demand_grain_is_unique_per_day_hour_zone(spark, published):
    _, _, run_dir = published
    demand = _table(spark, run_dir, "demand_hourly")
    duplicated = (
        demand.groupBy("pickup_date", "pickup_hour", "pu_location_id")
        .count()
        .filter("count > 1")
        .count()
    )
    assert duplicated == 0


def test_summary_counts_and_reasons(published):
    _, summary, _ = published
    assert summary["status"] == "success"
    assert summary["rows_read"] == 23
    assert summary["rows_valid"] == 16
    assert summary["rows_rejected"] == 7
    # L21 e L22 têm dois motivos cada, por isso a soma por motivo é 9 e não 7.
    assert summary["rejected_by_reason"] == {
        REASON_DROPOFF_BEFORE_PICKUP: 1,
        REASON_MISSING_PICKUP_DATETIME: 3,
        REASON_MISSING_PICKUP_LOCATION: 2,
        REASON_UNKNOWN_PICKUP_ZONE: 3,
    }


def test_observations_are_counted_but_never_remove_records(published):
    _, summary, _ = published
    assert summary["observations"] == {
        "passenger_count_null": 1,      # L08 (Flex Fare)
        "passenger_count_zero": 1,      # L10
        "dropoff_null": 1,              # L15
        "total_amount_negative": 1,     # L11
        "trip_distance_zero": 2,        # L10 e L12
        "payment_type_flex_fare": 1,    # L08
    }  # fmt: skip
    assert summary["rows_valid"] == 16


def test_sql_queries_over_published_output(spark, published):
    output, summary, _ = published

    by_hour = run_query(spark, output, SQL_DIR / "demand_by_hour.sql").collect()
    assert [(row["pickup_hour"], row["trips"]) for row in by_hour] == [
        (0, 1), (8, 5), (9, 1), (10, 1), (11, 1), (17, 2), (18, 2), (19, 2), (23, 1),
    ]  # fmt: skip

    by_reason = run_query(spark, output, SQL_DIR / "rejected_by_reason.sql").collect()
    counted = {row["reason"]: row["registros"] for row in by_reason}
    assert counted == summary["rejected_by_reason"]
