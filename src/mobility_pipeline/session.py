"""Criação da sessão Spark usada pelo pipeline (execução local)."""

from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "mobility-pipeline") -> SparkSession:
    # TIMESTAMP_NTZ é lido sem conversão de fuso, então a hora do dia é a hora de NYC gravada na fonte.
    # A sessão em UTC só afeta tipos com fuso, que o pipeline não usa.
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
