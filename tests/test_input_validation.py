"""Entradas ausentes, corrompidas ou incompatíveis: erro compreensível e nenhuma publicação incompleta."""

from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from helpers import SAMPLE_INPUT, ZONES

from mobility_pipeline import pipeline as pipeline_module
from mobility_pipeline.errors import (
    EmptyInputError,
    ExecutionError,
    IncompatibleInputError,
    InputCorruptedError,
    InputNotFoundError,
    PipelineError,
    ReconciliationError,
    ZoneReferenceError,
)
from mobility_pipeline.pipeline import run_pipeline
from mobility_pipeline.query import current_run_dir


def _publication_state(output):
    """Ponteiro atual e lista de execuções: deve ficar idêntico depois de uma falha."""
    pointer = output / "current"
    current = pointer.read_text(encoding="utf-8") if pointer.exists() else None
    runs_dir = output / "runs"
    runs = sorted(entry.name for entry in runs_dir.iterdir()) if runs_dir.exists() else []
    return current, runs


def _trip_table(**overrides):
    """Uma corrida mínima com as colunas exigidas pelo pipeline. Campos em overrides substituem os padrões."""
    columns = {
        "tpep_pickup_datetime": pa.array([datetime(2025, 1, 6, 8, 5)], type=pa.timestamp("us")),
        "tpep_dropoff_datetime": pa.array([datetime(2025, 1, 6, 8, 20)], type=pa.timestamp("us")),
        "PULocationID": pa.array([161], type=pa.int32()),
    }
    columns.update(overrides)
    return pa.table(columns)


@pytest.fixture
def published_output(spark, tmp_path):
    output = tmp_path / "output"
    run_pipeline(spark, SAMPLE_INPUT, ZONES, output)
    return output


def test_missing_input_fails_before_any_output_is_created(spark, tmp_path):
    output = tmp_path / "output"
    with pytest.raises(InputNotFoundError, match="entrada não encontrada"):
        run_pipeline(spark, tmp_path / "nao_existe.parquet", ZONES, output)
    assert not output.exists()


def test_directory_without_parquet_files_is_reported(spark, tmp_path):
    empty = tmp_path / "vazio"
    empty.mkdir()
    with pytest.raises(InputNotFoundError, match="nenhum arquivo .parquet"):
        run_pipeline(spark, empty, ZONES, tmp_path / "output")


def test_truncated_parquet_is_reported_as_corrupted(spark, tmp_path):
    truncated = tmp_path / "truncado.parquet"
    data = SAMPLE_INPUT.read_bytes()
    truncated.write_bytes(data[: len(data) // 2])
    with pytest.raises(InputCorruptedError, match="não foi possível ler"):
        run_pipeline(spark, truncated, ZONES, tmp_path / "output")


def test_text_file_with_parquet_name_is_reported_as_corrupted(spark, tmp_path):
    fake = tmp_path / "texto.parquet"
    fake.write_text("isto não é Parquet\n", encoding="utf-8")
    with pytest.raises(InputCorruptedError):
        run_pipeline(spark, fake, ZONES, tmp_path / "output")


def test_missing_required_column_is_incompatible(spark, tmp_path):
    path = tmp_path / "sem_zona.parquet"
    pq.write_table(_trip_table().drop_columns(["PULocationID"]), path)
    with pytest.raises(IncompatibleInputError, match="coluna ausente 'PULocationID'"):
        run_pipeline(spark, path, ZONES, tmp_path / "output")


def test_wrong_column_type_is_incompatible(spark, tmp_path):
    path = tmp_path / "zona_texto.parquet"
    pq.write_table(_trip_table(PULocationID=pa.array(["161"])), path)
    with pytest.raises(IncompatibleInputError, match="'PULocationID' tem tipo string"):
        run_pipeline(spark, path, ZONES, tmp_path / "output")


def test_timestamp_with_timezone_is_incompatible(spark, tmp_path):
    path = tmp_path / "com_fuso.parquet"
    aware = pa.array([datetime(2025, 1, 6, 8, 5)], type=pa.timestamp("us", tz="UTC"))
    pq.write_table(_trip_table(tpep_pickup_datetime=aware), path)
    with pytest.raises(IncompatibleInputError, match="esperado timestamp_ntz"):
        run_pipeline(spark, path, ZONES, tmp_path / "output")


def test_input_without_records_is_rejected(spark, tmp_path):
    path = tmp_path / "vazio.parquet"
    pq.write_table(_trip_table().slice(0, 0), path)
    with pytest.raises(EmptyInputError):
        run_pipeline(spark, path, ZONES, tmp_path / "output")


def test_zone_table_with_duplicate_ids_is_rejected(spark, tmp_path):
    zones = tmp_path / "zonas.csv"
    zones.write_text(
        '"LocationID","Borough","Zone","service_zone"\n'
        '1,"EWR","Newark Airport","EWR"\n'
        '1,"EWR","Duplicado","EWR"\n',
        encoding="utf-8",
    )
    with pytest.raises(ZoneReferenceError, match="LocationID duplicado"):
        run_pipeline(spark, SAMPLE_INPUT, zones, tmp_path / "output")


def test_zone_table_with_unexpected_header_is_rejected(spark, tmp_path):
    zones = tmp_path / "zonas.csv"
    zones.write_text("id,nome\n1,Newark\n", encoding="utf-8")
    with pytest.raises(ZoneReferenceError, match="cabeçalho"):
        run_pipeline(spark, SAMPLE_INPUT, zones, tmp_path / "output")


def test_missing_zone_table_is_reported(spark, tmp_path):
    with pytest.raises(InputNotFoundError, match="tabela de zonas não encontrada"):
        run_pipeline(spark, SAMPLE_INPUT, tmp_path / "sem_zonas.csv", tmp_path / "output")


@pytest.mark.parametrize(
    "bad_input",
    [
        pytest.param("missing", id="arquivo-ausente"),
        pytest.param("incompatible", id="coluna-ausente"),
        pytest.param("corrupted", id="parquet-truncado"),
    ],
)
def test_failed_input_keeps_previous_publication(spark, tmp_path, published_output, bad_input):
    before = _publication_state(published_output)
    before_total = (
        spark.read.parquet(str(current_run_dir(published_output) / "demand_hourly"))
        .agg({"trip_count": "sum"})
        .first()[0]
    )

    if bad_input == "missing":
        path = tmp_path / "nao_existe.parquet"
    elif bad_input == "incompatible":
        path = tmp_path / "sem_zona.parquet"
        pq.write_table(_trip_table().drop_columns(["PULocationID"]), path)
    else:
        path = tmp_path / "truncado.parquet"
        data = SAMPLE_INPUT.read_bytes()
        path.write_bytes(data[: len(data) // 2])

    with pytest.raises(PipelineError):
        run_pipeline(spark, path, ZONES, published_output)

    assert _publication_state(published_output) == before
    assert (
        spark.read.parquet(str(current_run_dir(published_output) / "demand_hourly"))
        .agg({"trip_count": "sum"})
        .first()[0]
        == before_total
    )


def test_processing_failure_does_not_publish(spark, monkeypatch, published_output):
    before = _publication_state(published_output)

    def broken_aggregation(valid):
        raise RuntimeError("falha simulada na agregação")

    monkeypatch.setattr(pipeline_module, "aggregate_demand", broken_aggregation)
    with pytest.raises(ExecutionError, match="falha simulada na agregação"):
        run_pipeline(spark, SAMPLE_INPUT, ZONES, published_output)

    assert _publication_state(published_output) == before


def test_failed_reconciliation_does_not_publish(spark, monkeypatch, published_output):
    before = _publication_state(published_output)

    def mismatch(*args, **kwargs):
        raise ReconciliationError("contagens não batem (simulado)")

    monkeypatch.setattr(pipeline_module, "_reconcile", mismatch)
    with pytest.raises(ReconciliationError):
        run_pipeline(spark, SAMPLE_INPUT, ZONES, published_output)

    assert _publication_state(published_output) == before


def test_failure_after_directory_rename_is_rolled_back(spark, monkeypatch, published_output):
    """A pasta da nova execução já foi renomeada, mas o ponteiro não foi trocado: a pasta deve sumir."""
    before = _publication_state(published_output)

    def pointer_failure(output_dir, run_id):
        raise OSError("disco cheio (simulado)")

    monkeypatch.setattr(pipeline_module, "_switch_current_pointer", pointer_failure)
    with pytest.raises(ExecutionError, match="disco cheio"):
        run_pipeline(spark, SAMPLE_INPUT, ZONES, published_output)

    assert _publication_state(published_output) == before
