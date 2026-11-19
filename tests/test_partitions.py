"""Partições mensais: cada mês é publicado separadamente, sem contagem dupla e com reprocessamento isolado.

Os dados são sintéticos e pequenos, criados aqui. Os resultados esperados foram calculados à mão
a partir das linhas abaixo (ver comentários em cada caso).
"""

from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from helpers import ZONES, project

from mobility_pipeline.errors import PipelineError
from mobility_pipeline.pipeline import partition_dir, run_pipeline
from mobility_pipeline.query import published_partitions, register_views, run_query
from mobility_pipeline.transform import REASON_MISSING_PICKUP_DATETIME, REASON_OUTSIDE_PARTITION_MONTH

SQL_PARTITION_TOTALS = "SELECT partition_month, COUNT(*) AS valid FROM valid GROUP BY partition_month ORDER BY partition_month"


def _write_trips(path, rows):
    """rows: lista de (pickup | None, dropoff | None, PULocationID)."""
    table = pa.table(
        {
            "tpep_pickup_datetime": pa.array([r[0] for r in rows], type=pa.timestamp("us")),
            "tpep_dropoff_datetime": pa.array([r[1] for r in rows], type=pa.timestamp("us")),
            "PULocationID": pa.array([r[2] for r in rows], type=pa.int32()),
        }
    )
    pq.write_table(table, path)
    return path


JAN = [
    (datetime(2025, 1, 1, 0, 0, 0), datetime(2025, 1, 1, 0, 10), 161),        # J1 válida (início do mês)
    (datetime(2025, 1, 31, 23, 59, 59), datetime(2025, 2, 1, 0, 5), 161),     # J2 válida (fim do mês; desembarque em fevereiro é aceito)
    (datetime(2025, 2, 1, 0, 0, 0), datetime(2025, 2, 1, 0, 10), 161),        # J3 fora: embarque em fevereiro
    (datetime(2024, 12, 31, 23, 59, 59), datetime(2025, 1, 1, 0, 10), 161),   # J4 fora: embarque em dezembro
    (None, datetime(2025, 1, 2, 0, 10), 161),                                 # J5 só motivo missing_pickup_datetime
]
# Jan esperado: válidas 2 (J1, J2); rejeitadas 3 (J3, J4, J5);
#   motivos: outside_partition_month 2, missing_pickup_datetime 1.
# Agregado Jan: (2025-01-01, 0, 161) = 1 e (2025-01-31, 23, 161) = 1.

FEB = [
    (datetime(2025, 2, 1, 0, 0, 0), datetime(2025, 2, 1, 0, 10), 161),       # F1 válida (início de fevereiro)
    (datetime(2025, 1, 31, 23, 59, 59), datetime(2025, 2, 1, 0, 5), 161),    # F2 fora: é a mesma corrida de J2
    (datetime(2025, 2, 15, 12, 0, 0), datetime(2025, 2, 15, 12, 30), 236),   # F3 válida
]
# Feb esperado: válidas 2 (F1, F3); rejeitadas 1 (F2, motivo outside_partition_month).


@pytest.fixture
def two_months(spark, tmp_path):
    jan = _write_trips(tmp_path / "jan.parquet", JAN)
    feb = _write_trips(tmp_path / "feb.parquet", FEB)
    output = tmp_path / "curated"
    run_pipeline(spark, jan, ZONES, output, "2025-01")
    run_pipeline(spark, feb, ZONES, output, "2025-02")
    return output


def test_rule_marks_out_of_month_pickups_and_keeps_records_counted(spark, tmp_path):
    output = tmp_path / "curated"
    summary = run_pipeline(spark, _write_trips(tmp_path / "jan.parquet", JAN), ZONES, output, "2025-01")

    assert summary["rows_read"] == 5
    assert summary["rows_valid"] == 2
    assert summary["rows_rejected"] == 3
    assert summary["rejected_by_reason"] == {
        REASON_MISSING_PICKUP_DATETIME: 1,
        REASON_OUTSIDE_PARTITION_MONTH: 2,
    }
    assert summary["month"] == "2025-01"


def test_january_partition_content_matches_hand_computed_expectation(spark, tmp_path):
    output = tmp_path / "curated"
    run_pipeline(spark, _write_trips(tmp_path / "jan.parquet", JAN), ZONES, output, "2025-01")
    run_dir = published_partitions(output)["2025-01"]

    rejected = spark.read.parquet(str(run_dir / "rejected"))
    assert project(rejected, ["tpep_pickup_datetime", "tpep_dropoff_datetime", "reject_reasons"]) == sorted([
        ("2025-02-01 00:00:00", "2025-02-01 00:10:00", "outside_partition_month"),
        ("2024-12-31 23:59:59", "2025-01-01 00:10:00", "outside_partition_month"),
        ("", "2025-01-02 00:10:00", "missing_pickup_datetime"),
    ])

    demand = spark.read.parquet(str(run_dir / "demand_hourly"))
    assert project(demand, ["pickup_date", "pickup_hour", "pu_location_id", "trip_count"]) == sorted([
        ("2025-01-01", "0", "161", "1"),
        ("2025-01-31", "23", "161", "1"),
    ])


def test_each_month_is_its_own_partition_with_its_own_pointer(spark, two_months):
    partitions = published_partitions(two_months)
    assert sorted(partitions) == ["2025-01", "2025-02"]
    assert partition_dir(two_months, "2025-01").name == "month=2025-01"
    assert (partition_dir(two_months, "2025-01") / "current").is_file()
    assert (partition_dir(two_months, "2025-02") / "current").is_file()


def test_same_trip_present_in_two_files_is_counted_once(spark, two_months):
    """J2 (embarque em 31/01) aparece no arquivo de janeiro e de fevereiro: conta só em janeiro."""
    register_views(spark, two_months)
    rows = spark.sql(SQL_PARTITION_TOTALS).collect()
    assert [(row["partition_month"], row["valid"]) for row in rows] == [("2025-01", 2), ("2025-02", 2)]
    assert spark.sql("SELECT COUNT(*) AS n FROM valid").first()["n"] == 4

    feb_rejected = spark.sql(
        "SELECT reason FROM (SELECT EXPLODE(reject_reasons) AS reason FROM rejected WHERE partition_month = '2025-02')"
    ).collect()
    assert [row["reason"] for row in feb_rejected] == ["outside_partition_month"]


def test_query_reads_all_published_partitions(spark, two_months):
    sql = "SELECT SUM(trip_count) AS total FROM demand_hourly"
    sql_file = two_months.parent / "total.sql"
    sql_file.write_text(sql, encoding="utf-8")
    assert run_query(spark, two_months, sql_file).first()["total"] == 4


def test_reprocessing_one_month_leaves_other_partition_untouched(spark, tmp_path):
    output = tmp_path / "curated"
    jan = _write_trips(tmp_path / "jan.parquet", JAN)
    feb = _write_trips(tmp_path / "feb.parquet", FEB)
    run_pipeline(spark, jan, ZONES, output, "2025-01")
    run_pipeline(spark, feb, ZONES, output, "2025-02")
    feb_before = published_partitions(output)["2025-02"]
    jan_before = published_partitions(output)["2025-01"]

    summary = run_pipeline(spark, jan, ZONES, output, "2025-01")

    assert published_partitions(output)["2025-02"] == feb_before
    assert published_partitions(output)["2025-01"] != jan_before
    assert published_partitions(output)["2025-01"].name == summary["run_id"]
    assert not jan_before.exists()
    register_views(spark, output)
    rows = spark.sql(SQL_PARTITION_TOTALS).collect()
    assert [(row["partition_month"], row["valid"]) for row in rows] == [("2025-01", 2), ("2025-02", 2)]


def test_invalid_month_is_refused_before_writing(spark, tmp_path):
    output = tmp_path / "curated"
    with pytest.raises(PipelineError, match="mês inválido"):
        run_pipeline(spark, _write_trips(tmp_path / "jan.parquet", JAN), ZONES, output, "2025-13")
    assert not output.exists()


def test_query_without_published_partitions_is_explained(spark, tmp_path):
    with pytest.raises(PipelineError, match="saída não encontrada|nenhuma partição publicada"):
        published_partitions(tmp_path / "vazio")
