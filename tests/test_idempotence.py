"""Reexecutar a mesma entrada não pode duplicar nem acrescentar resultados."""

from helpers import SAMPLE_INPUT, ZONES, project

from mobility_pipeline.pipeline import partition_dir, run_pipeline
from mobility_pipeline.query import current_run_dir

TABLES = {
    "valid": 16,
    "rejected": 7,
    "demand_hourly": 12,
}
VOLATILE_SUMMARY_FIELDS = {"run_id", "started_at", "finished_at", "duration_seconds"}


def _table_contents(spark, run_dir, name):
    frame = spark.read.parquet(str(run_dir / name))
    return project(frame, frame.columns), frame.count()


def test_rerun_publishes_same_logical_result_without_duplicates(spark, tmp_path):
    output = tmp_path / "output"

    first = run_pipeline(spark, SAMPLE_INPUT, ZONES, output, "2025-01")
    first_run = current_run_dir(partition_dir(output, "2025-01"))
    first_contents = {name: _table_contents(spark, first_run, name) for name in TABLES}

    second = run_pipeline(spark, SAMPLE_INPUT, ZONES, output, "2025-01")
    second_run = current_run_dir(partition_dir(output, "2025-01"))

    assert second["run_id"] != first["run_id"]
    # A execução anterior foi removida; só a atual permanece publicada.
    assert not first_run.exists()
    assert sorted(entry.name for entry in (partition_dir(output, "2025-01") / "runs").iterdir()) == [second["run_id"]]
    assert not any(entry.name.startswith(".tmp-") for entry in (partition_dir(output, "2025-01") / "runs").iterdir())

    for name, expected_count in TABLES.items():
        contents, count = _table_contents(spark, second_run, name)
        assert count == expected_count, f"{name} deveria ter {expected_count} linhas, tem {count}"
        assert contents == first_contents[name][0], f"{name} mudou entre execuções idênticas"

    stable_first = {k: v for k, v in first.items() if k not in VOLATILE_SUMMARY_FIELDS}
    stable_second = {k: v for k, v in second.items() if k not in VOLATILE_SUMMARY_FIELDS}
    assert stable_first == stable_second
