"""Leitura e validação das entradas: corridas Parquet e tabela de zonas da TLC.

Estas funções falham cedo e com mensagem clara. Nada é escrito no disco aqui.
"""

import csv
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from mobility_pipeline.errors import (
    EmptyInputError,
    IncompatibleInputError,
    InputCorruptedError,
    InputNotFoundError,
    ZoneReferenceError,
)

# Colunas exigidas pela primeira entrega. As demais colunas do dicionário Yellow Taxi são
# preservadas quando existem, mas não são obrigatórias: meses de anos diferentes podem ter
# colunas extras (ex.: cbd_congestion_fee a partir de 2025) sem quebrar a execução.
REQUIRED_TRIP_TYPES: dict[str, tuple[str, ...]] = {
    "tpep_pickup_datetime": ("timestamp_ntz",),
    "tpep_dropoff_datetime": ("timestamp_ntz",),
    "PULocationID": ("int", "bigint"),
}

ZONE_COLUMNS = ["LocationID", "Borough", "Zone", "service_zone"]

ZONE_SCHEMA = T.StructType(
    [
        T.StructField("LocationID", T.IntegerType(), False),
        T.StructField("Borough", T.StringType(), True),
        T.StructField("Zone", T.StringType(), True),
        T.StructField("service_zone", T.StringType(), True),
    ]
)


def _first_line(exc: Exception) -> str:
    text = str(exc).strip()
    return text.splitlines()[0] if text else type(exc).__name__


def check_trip_schema(df: DataFrame) -> None:
    """Confere se o DataFrame tem as colunas e os tipos exigidos; levanta IncompatibleInputError."""
    dtypes = dict(df.dtypes)
    problems = []
    for name, accepted in REQUIRED_TRIP_TYPES.items():
        if name not in dtypes:
            problems.append(f"coluna ausente '{name}'")
        elif dtypes[name] not in accepted:
            expected = " ou ".join(accepted)
            problems.append(f"coluna '{name}' tem tipo {dtypes[name]}, esperado {expected}")
    if problems:
        raise IncompatibleInputError(
            "entrada incompatível com o dicionário Yellow Taxi: " + "; ".join(problems)
        )


def read_trips(spark: SparkSession, path: Path) -> DataFrame:
    """Lê um arquivo ou diretório Parquet de corridas e valida a entrada antes de qualquer processamento."""
    if not path.exists():
        raise InputNotFoundError(f"entrada não encontrada: {path}")
    if path.is_dir() and not any(path.rglob("*.parquet")):
        raise InputNotFoundError(f"nenhum arquivo .parquet em {path}")

    try:
        trips = spark.read.parquet(str(path))
        # O schema é lido dos metadados (footer). Um arquivo truncado ou que não é Parquet falha aqui.
        _ = trips.schema
    except Exception as exc:  # o Spark usa várias classes de erro para isso
        raise InputCorruptedError(
            f"não foi possível ler {path} como Parquet: {_first_line(exc)}"
        ) from exc

    check_trip_schema(trips)
    return trips


def read_zones(spark: SparkSession, path: Path) -> DataFrame:
    """Lê a tabela oficial de zonas da TLC (CSV) e valida cabeçalho, chaves e nulos."""
    if not path.is_file():
        raise InputNotFoundError(f"tabela de zonas não encontrada: {path}")

    with path.open(newline="", encoding="utf-8") as handle:
        header = next(csv.reader(handle), [])
    if header != ZONE_COLUMNS:
        raise ZoneReferenceError(
            f"cabeçalho de {path} inesperado: {header}; esperado {ZONE_COLUMNS}"
        )

    try:
        zones = (
            spark.read.option("header", True)
            .option("mode", "FAILFAST")
            .schema(ZONE_SCHEMA)
            .csv(str(path))
        )
        rows = zones.collect()
    except Exception as exc:
        raise ZoneReferenceError(f"não foi possível ler {path}: {_first_line(exc)}") from exc

    ids = [row["LocationID"] for row in rows]
    if not ids:
        raise ZoneReferenceError(f"{path} não tem zonas")
    if len(ids) != len(set(ids)):
        raise ZoneReferenceError(f"{path} tem LocationID duplicado")
    return zones


def ensure_not_empty(trips: DataFrame, path: Path) -> int:
    """Conta os registros de entrada; zero registros interrompe a execução."""
    count = trips.count()
    if count == 0:
        raise EmptyInputError(f"a entrada não tem registros: {path}")
    return count
