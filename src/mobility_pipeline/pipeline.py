"""Orquestra uma execução local: lê, classifica, agrega, reconcilia e publica.

Modelo de publicação (ver docs/ARQUITETURA.md):
  1. Entrada validada antes de qualquer escrita.
  2. Saídas escritas em <saida>/runs/.tmp-<run_id>/.
  3. Contagens reconciliadas a partir dos arquivos gravados.
  4. Pasta renomeada para <saida>/runs/<run_id>/ (operação atômica no mesmo sistema de arquivos).
  5. Arquivo <saida>/current é trocado por substituição atômica (os.replace).
  6. Execuções antigas são removidas.

Se qualquer etapa falhar antes do passo 5, o ponteiro continua apontando para a última execução válida.
"""

import json
import logging
import os
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from mobility_pipeline import __version__
from mobility_pipeline.errors import ExecutionError, PipelineError, ReconciliationError
from mobility_pipeline.inputs import ensure_not_empty, read_trips, read_zones
from mobility_pipeline.transform import (
    aggregate_demand,
    classify_trips,
    count_observations,
    split_valid_rejected,
)

LOGGER = logging.getLogger(__name__)

CURRENT_POINTER = "current"
RUNS_DIR = "runs"
TABLES = ("valid", "rejected", "demand_hourly")
SUMMARY_FILE = "run_summary.json"


def run_pipeline(
    spark: SparkSession,
    input_path: Path,
    zones_path: Path,
    output_dir: Path,
) -> dict:
    """Executa o pipeline completo e devolve o resumo da execução (também gravado em disco)."""
    started_at = datetime.now(timezone.utc)
    started_clock = time.monotonic()
    run_id = f"{started_at:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    input_path = Path(input_path)
    zones_path = Path(zones_path)
    output_dir = Path(output_dir)

    runs_dir = output_dir / RUNS_DIR
    pending_dir = runs_dir / f".tmp-{run_id}"
    published_dir: Path | None = None
    try:
        # Validação antes de criar qualquer pasta de saída: entrada inválida não toca o disco de saída.
        trips = read_trips(spark, input_path)
        zones = read_zones(spark, zones_path)
        rows_read = ensure_not_empty(trips, input_path)
        LOGGER.info("entrada validada: %s registros lidos de %s", rows_read, input_path)

        runs_dir.mkdir(parents=True, exist_ok=True)
        pending_dir.mkdir()

        classified = classify_trips(trips, zones)
        valid, rejected = split_valid_rejected(classified)
        demand = aggregate_demand(valid)

        valid.write.mode("overwrite").parquet(str(pending_dir / "valid"))
        rejected.write.mode("overwrite").parquet(str(pending_dir / "rejected"))
        demand.write.mode("overwrite").parquet(str(pending_dir / "demand_hourly"))

        rows_valid, rows_rejected, trips_in_demand, reasons = _reconcile(
            spark, pending_dir, rows_read
        )
        observations = count_observations(trips)

        finished_at = datetime.now(timezone.utc)
        summary = {
            "run_id": run_id,
            "pipeline_version": __version__,
            "status": "success",
            "started_at": started_at.isoformat(timespec="seconds"),
            "finished_at": finished_at.isoformat(timespec="seconds"),
            "duration_seconds": round(time.monotonic() - started_clock, 3),
            "input_path": str(input_path),
            "zones_path": str(zones_path),
            "rows_read": rows_read,
            "rows_valid": rows_valid,
            "rows_rejected": rows_rejected,
            "rejected_by_reason": reasons,
            "observations": observations,
            "demand_trip_count_total": trips_in_demand,
        }
        (pending_dir / SUMMARY_FILE).write_text(
            json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        # Publicação: renomeia a pasta, depois troca o ponteiro atomicamente.
        published_dir = runs_dir / run_id
        os.rename(pending_dir, published_dir)
        _switch_current_pointer(output_dir, run_id)
        _remove_other_runs(runs_dir, keep=run_id)
    except PipelineError:
        _cleanup_failed(pending_dir, published_dir, output_dir, run_id)
        raise
    except Exception as exc:
        _cleanup_failed(pending_dir, published_dir, output_dir, run_id)
        raise ExecutionError(f"falha durante o processamento: {_first_line(exc)}") from exc

    LOGGER.info(
        "execução %s publicada: %s válidos, %s rejeitados",
        run_id,
        rows_valid,
        rows_rejected,
    )
    return summary


def _reconcile(spark: SparkSession, run_dir: Path, rows_read: int) -> tuple[int, int, int, dict]:
    """Confere as saídas gravadas contra a entrada. Qualquer divergência cancela a publicação."""
    rows_valid = spark.read.parquet(str(run_dir / "valid")).count()
    rejected = spark.read.parquet(str(run_dir / "rejected"))
    rows_rejected = rejected.count()
    if rows_valid + rows_rejected != rows_read:
        raise ReconciliationError(
            f"contagens não batem: lidos={rows_read}, válidos={rows_valid}, "
            f"rejeitados={rows_rejected}"
        )

    demand_total = (
        spark.read.parquet(str(run_dir / "demand_hourly"))
        .agg(F.sum("trip_count").alias("total"))
        .first()["total"]
    )
    demand_total = int(demand_total or 0)
    if demand_total != rows_valid:
        raise ReconciliationError(
            f"agregação diverge dos válidos: soma de trip_count={demand_total}, válidos={rows_valid}"
        )

    reason_rows = (
        rejected.select(F.explode("reject_reasons").alias("reason"))
        .groupBy("reason")
        .count()
        .collect()
    )
    reasons = {row["reason"]: int(row["count"]) for row in reason_rows}
    return rows_valid, rows_rejected, demand_total, dict(sorted(reasons.items()))


def _switch_current_pointer(output_dir: Path, run_id: str) -> None:
    temporary = output_dir / f".{CURRENT_POINTER}.tmp"
    temporary.write_text(run_id + "\n", encoding="utf-8")
    os.replace(temporary, output_dir / CURRENT_POINTER)


def _remove_other_runs(runs_dir: Path, keep: str) -> None:
    for entry in runs_dir.iterdir():
        if entry.name != keep:
            shutil.rmtree(entry, ignore_errors=True)


def _cleanup_failed(
    pending_dir: Path, published_dir: Path | None, output_dir: Path, run_id: str
) -> None:
    shutil.rmtree(pending_dir, ignore_errors=True)
    # Se a pasta já foi publicada mas o ponteiro não aponta para ela, ela é removida.
    if published_dir is not None and published_dir.exists():
        pointer = output_dir / CURRENT_POINTER
        current = pointer.read_text(encoding="utf-8").strip() if pointer.exists() else None
        if current != run_id:
            shutil.rmtree(published_dir, ignore_errors=True)


def _first_line(exc: Exception) -> str:
    text = str(exc).strip()
    return text.splitlines()[0] if text else type(exc).__name__
