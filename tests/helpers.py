"""Utilitários de teste: leitura dos resultados esperados (CSV escritos à mão) e normalização para comparação."""

import csv
from datetime import date, datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SAMPLE_DIR = REPO / "sample"
EXPECTED_DIR = SAMPLE_DIR / "expected"
SAMPLE_INPUT = SAMPLE_DIR / "yellow_sample.parquet"
SAMPLE_INPUT_CSV = SAMPLE_DIR / "yellow_sample_input.csv"
ZONES = REPO / "reference" / "taxi_zone_lookup.csv"
SQL_DIR = REPO / "sql"


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def normalize(value) -> str:
    """Converte um valor do Spark para o mesmo texto usado nos CSVs esperados."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, (list, tuple)):
        return ";".join(sorted(str(item) for item in value))
    return str(value)


def project(df, columns: list[str]) -> list[tuple[str, ...]]:
    """Lista ordenada de tuplas normalizadas, para comparar tabelas independentemente da ordem das linhas."""
    return sorted(
        tuple(normalize(row[column]) for column in columns) for row in df.select(*columns).collect()
    )


def expected_projection(csv_name: str, columns: list[str]) -> list[tuple[str, ...]]:
    rows = read_csv_rows(EXPECTED_DIR / csv_name)
    return sorted(tuple(row[column] for column in columns) for row in rows)
