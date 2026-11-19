"""Regras de qualidade e transformações (PySpark).

Este módulo não sabe nada sobre Airflow nem sobre caminhos de arquivo: recebe DataFrames e devolve DataFrames,
por isso pode ser testado diretamente. As regras estão descritas em docs/REGRAS_QUALIDADE.md.

Princípio: nenhuma linha é descartada antes de ser classificada. Um registro só deixa de ser válido
quando falha uma regra de rejeição, e o motivo fica gravado no próprio registro rejeitado.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

REASON_MISSING_PICKUP_DATETIME = "missing_pickup_datetime"
REASON_MISSING_PICKUP_LOCATION = "missing_pickup_location"
REASON_UNKNOWN_PICKUP_ZONE = "unknown_pickup_zone"
REASON_DROPOFF_BEFORE_PICKUP = "dropoff_before_pickup"
REASON_OUTSIDE_PARTITION_MONTH = "outside_partition_month"

REJECTION_REASONS = (
    REASON_MISSING_PICKUP_DATETIME,
    REASON_MISSING_PICKUP_LOCATION,
    REASON_UNKNOWN_PICKUP_ZONE,
    REASON_DROPOFF_BEFORE_PICKUP,
    REASON_OUTSIDE_PARTITION_MONTH,
)

_ZONE_ID = "_zone_location_id"


def classify_trips(trips: DataFrame, zones: DataFrame, month: str) -> DataFrame:
    """Anexa a cada corrida os motivos de rejeição (array vazio = válida), sem remover linhas.

    `month` (AAAA-MM) é o mês da partição: embarques fora dele são rejeitados.

    Colunas acrescentadas: pu_borough, pu_zone, pu_service_zone (da tabela de zonas),
    reject_reasons (array<string>) e source_file (nome do arquivo de origem).
    """
    source_columns = list(trips.columns)
    pickup = F.col("tpep_pickup_datetime")
    dropoff = F.col("tpep_dropoff_datetime")
    pickup_location = F.col("PULocationID")

    zone_reference = zones.select(
        F.col("LocationID").alias(_ZONE_ID),
        F.col("Borough").alias("pu_borough"),
        F.col("Zone").alias("pu_zone"),
        F.col("service_zone").alias("pu_service_zone"),
    )

    # Junção à esquerda: se a zona não existir na tabela, as colunas de zona ficam nulas e a regra dispara.
    joined = trips.withColumn("source_file", F.col("_metadata.file_name")).join(
        F.broadcast(zone_reference),
        pickup_location == F.col(_ZONE_ID),
        "left",
    )

    # F.when devolve null quando a condição é falsa; array_compact remove esses nulos.
    # Comparações com null (ex.: dropoff < pickup quando pickup é nulo) não disparam a regra,
    # porque a regra de embarque já cobre esse caso.
    reasons = F.array_compact(
        F.array(
            F.when(pickup.isNull(), F.lit(REASON_MISSING_PICKUP_DATETIME)),
            F.when(pickup_location.isNull(), F.lit(REASON_MISSING_PICKUP_LOCATION)),
            F.when(
                pickup_location.isNotNull() & F.col(_ZONE_ID).isNull(),
                F.lit(REASON_UNKNOWN_PICKUP_ZONE),
            ),
            F.when(dropoff < pickup, F.lit(REASON_DROPOFF_BEFORE_PICKUP)),
            F.when(
                pickup.isNotNull() & (F.date_format(pickup, "yyyy-MM") != F.lit(month)),
                F.lit(REASON_OUTSIDE_PARTITION_MONTH),
            ),
        )
    )

    return joined.select(
        *[F.col(name) for name in source_columns],
        "pu_borough",
        "pu_zone",
        "pu_service_zone",
        "source_file",
        reasons.alias("reject_reasons"),
    )


def split_valid_rejected(classified: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Separa registros válidos e rejeitados. Juntos, reproduzem exatamente a entrada."""
    has_reason = F.size("reject_reasons") > 0
    source_columns = [c for c in classified.columns if c not in ("reject_reasons",)]
    zone_columns = ["pu_borough", "pu_zone", "pu_service_zone"]
    base_columns = [c for c in source_columns if c not in zone_columns]

    valid = (
        classified.filter(~has_reason)
        .withColumn("pickup_date", F.to_date(F.col("tpep_pickup_datetime")))
        .withColumn("pickup_hour", F.hour(F.col("tpep_pickup_datetime")))
        .select(*base_columns, "pickup_date", "pickup_hour", *zone_columns)
    )
    rejected = classified.filter(has_reason).select(*base_columns, *zone_columns, "reject_reasons")
    return valid, rejected


def aggregate_demand(valid: DataFrame) -> DataFrame:
    """Demanda por dia, hora e zona de embarque: uma linha por combinação, com a quantidade de viagens."""
    return (
        valid.groupBy(
            "pickup_date",
            "pickup_hour",
            F.col("PULocationID").alias("pu_location_id"),
            "pu_borough",
            "pu_zone",
        )
        .agg(F.count(F.lit(1)).alias("trip_count"))
        .orderBy("pickup_date", "pickup_hour", "pu_location_id")
    )


def count_observations(trips: DataFrame) -> dict[str, int]:
    """Conta valores incomuns que são mantidos nas saídas. Nenhum deles causa rejeição.

    Serve para tornar visível o que foi preservado. Ver docs/REGRAS_QUALIDADE.md (seção de observações).
    """
    columns = set(trips.columns)
    checks: dict[str, Column] = {}
    if "passenger_count" in columns:
        checks["passenger_count_null"] = F.col("passenger_count").isNull()
        checks["passenger_count_zero"] = F.col("passenger_count") == 0
    if "tpep_dropoff_datetime" in columns:
        checks["dropoff_null"] = F.col("tpep_dropoff_datetime").isNull()
    if "total_amount" in columns:
        checks["total_amount_negative"] = F.col("total_amount") < 0
    if "trip_distance" in columns:
        checks["trip_distance_zero"] = F.col("trip_distance") == 0
    if "payment_type" in columns:
        # 0 = "Flex Fare trip" no dicionário oficial (data_dictionary_trip_records_yellow.pdf, 18/03/2025)
        checks["payment_type_flex_fare"] = F.col("payment_type") == 0

    if not checks:
        return {}
    aggregates = [
        F.sum(F.when(condition, 1).otherwise(0)).alias(name) for name, condition in checks.items()
    ]
    row = trips.agg(*aggregates).first()
    return {name: int(row[name] or 0) for name in checks}
