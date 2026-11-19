"""Consultas SQL sobre a publicação atual de uma saída."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from mobility_pipeline.errors import PipelineError
from mobility_pipeline.pipeline import CURRENT_POINTER, RUNS_DIR, TABLES


def current_run_dir(output_dir: Path) -> Path:
    """Devolve a pasta da última execução publicada; falha se não houver publicação válida."""
    pointer = output_dir / CURRENT_POINTER
    if not pointer.is_file():
        raise PipelineError(f"nenhuma execução publicada em {output_dir}; rode 'run' antes")
    run_id = pointer.read_text(encoding="utf-8").strip()
    run_dir = output_dir / RUNS_DIR / run_id
    if not run_dir.is_dir():
        raise PipelineError(f"o ponteiro aponta para uma execução inexistente: {run_id}")
    return run_dir


def run_query(spark: SparkSession, output_dir: Path, sql_file: Path) -> DataFrame:
    """Registra as tabelas publicadas como views temporárias e executa o SQL do arquivo."""
    if not sql_file.is_file():
        raise PipelineError(f"arquivo SQL não encontrado: {sql_file}")
    run_dir = current_run_dir(output_dir)
    for table in TABLES:
        spark.read.parquet(str(run_dir / table)).createOrReplaceTempView(table)
    return spark.sql(sql_file.read_text(encoding="utf-8"))
