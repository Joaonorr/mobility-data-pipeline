"""Consultas SQL sobre as partições publicadas de uma saída."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from mobility_pipeline.errors import PipelineError
from mobility_pipeline.pipeline import CURRENT_POINTER, PARTITION_PREFIX, RUNS_DIR, TABLES

PARTITION_COLUMN = "partition_month"


def current_run_dir(partition: Path) -> Path:
    """Pasta da última execução publicada de uma partição; falha se não houver publicação válida."""
    pointer = partition / CURRENT_POINTER
    if not pointer.is_file():
        raise PipelineError(f"nenhuma execução publicada em {partition}; rode 'run' antes")
    run_id = pointer.read_text(encoding="utf-8").strip()
    run_dir = partition / RUNS_DIR / run_id
    if not run_dir.is_dir():
        raise PipelineError(f"o ponteiro aponta para uma execução inexistente: {run_id}")
    return run_dir


def published_partitions(output_root: Path) -> dict[str, Path]:
    """Mapeia cada mês publicado (AAAA-MM) para a pasta da sua última execução válida."""
    output_root = Path(output_root)
    if not output_root.is_dir():
        raise PipelineError(f"saída não encontrada: {output_root}; rode 'run' antes")

    partitions: dict[str, Path] = {}
    for partition in sorted(output_root.glob(f"{PARTITION_PREFIX}*")):
        pointer = partition / CURRENT_POINTER
        if not pointer.is_file():
            continue
        run_id = pointer.read_text(encoding="utf-8").strip()
        run_dir = partition / RUNS_DIR / run_id
        if not run_dir.is_dir():
            raise PipelineError(f"o ponteiro de {partition.name} aponta para uma execução inexistente: {run_id}")
        partitions[partition.name[len(PARTITION_PREFIX):]] = run_dir

    if not partitions:
        raise PipelineError(f"nenhuma partição publicada em {output_root}; rode 'run' antes")
    return partitions


def register_views(spark: SparkSession, output_root: Path) -> list[str]:
    """Registra as tabelas de todas as partições publicadas como views temporárias.

    Cada linha recebe `partition_month`. Colunas que existem só em alguns meses viram nulas nos outros.
    Devolve os meses registrados, em ordem.
    """
    partitions = published_partitions(output_root)
    for table in TABLES:
        frames: list[DataFrame] = []
        for month, run_dir in partitions.items():
            frame = spark.read.parquet(str(run_dir / table)).withColumn(PARTITION_COLUMN, F.lit(month))
            frames.append(frame)
        combined = frames[0]
        for frame in frames[1:]:
            combined = combined.unionByName(frame, allowMissingColumns=True)
        combined.createOrReplaceTempView(table)
    return list(partitions)


def run_query(spark: SparkSession, output_root: Path, sql_file: Path) -> DataFrame:
    """Registra as partições publicadas e executa o SQL do arquivo."""
    if not sql_file.is_file():
        raise PipelineError(f"arquivo SQL não encontrado: {sql_file}")
    register_views(spark, output_root)
    return spark.sql(sql_file.read_text(encoding="utf-8"))
