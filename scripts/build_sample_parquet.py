"""Gera sample/yellow_sample.parquet a partir de sample/yellow_sample_input.csv.

O esquema espelha o Parquet Yellow Taxi mensal da TLC (verificado em 2025-01 com pyarrow):
mesmos nomes, mesma ordem de colunas e tipos. Timestamps são gravados sem fuso, como na fonte.

Uso (com o ambiente de desenvolvimento ativo):
    python scripts/build_sample_parquet.py
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
SOURCE_CSV = ROOT / "sample" / "yellow_sample_input.csv"
TARGET_PARQUET = ROOT / "sample" / "yellow_sample.parquet"

SCHEMA = pa.schema(
    [
        ("VendorID", pa.int32()),
        ("tpep_pickup_datetime", pa.timestamp("us")),
        ("tpep_dropoff_datetime", pa.timestamp("us")),
        ("passenger_count", pa.int64()),
        ("trip_distance", pa.float64()),
        ("RatecodeID", pa.int64()),
        ("store_and_fwd_flag", pa.large_string()),
        ("PULocationID", pa.int32()),
        ("DOLocationID", pa.int32()),
        ("payment_type", pa.int64()),
        ("fare_amount", pa.float64()),
        ("extra", pa.float64()),
        ("mta_tax", pa.float64()),
        ("tip_amount", pa.float64()),
        ("tolls_amount", pa.float64()),
        ("improvement_surcharge", pa.float64()),
        ("total_amount", pa.float64()),
        ("congestion_surcharge", pa.float64()),
        ("Airport_fee", pa.float64()),
        ("cbd_congestion_fee", pa.float64()),
    ]
)


def main() -> None:
    convert = pacsv.ConvertOptions(
        column_types={field.name: field.type for field in SCHEMA},
        null_values=[""],
        strings_can_be_null=True,
    )
    table = pacsv.read_csv(SOURCE_CSV, convert_options=convert)
    table = table.select(SCHEMA.names).cast(SCHEMA)
    pq.write_table(table, TARGET_PARQUET, compression="snappy")
    written = pq.ParquetFile(TARGET_PARQUET).schema_arrow
    assert written.names == SCHEMA.names, "ordem de colunas divergente"
    print(f"{TARGET_PARQUET.relative_to(ROOT)}: {table.num_rows} registros, {len(SCHEMA)} colunas")


if __name__ == "__main__":
    main()
