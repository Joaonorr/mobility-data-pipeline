"""Interface de linha de comando: 'run' executa o pipeline; 'query' consulta a última publicação."""

import argparse
import json
import logging
import sys
from pathlib import Path

from mobility_pipeline.errors import PipelineError
from mobility_pipeline.pipeline import run_pipeline
from mobility_pipeline.query import run_query
from mobility_pipeline.session import create_spark_session

DEFAULT_ZONES = Path("reference/taxi_zone_lookup.csv")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mobility_pipeline", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="processa uma entrada Parquet e publica o resultado")
    run.add_argument("--input", required=True, type=Path, help="arquivo ou pasta Parquet de corridas")
    run.add_argument(
        "--zones",
        type=Path,
        default=DEFAULT_ZONES,
        help=f"CSV oficial de zonas da TLC (padrão: {DEFAULT_ZONES})",
    )
    run.add_argument("--output", required=True, type=Path, help="pasta de saída")

    query = commands.add_parser("query", help="executa SQL sobre a última publicação")
    query.add_argument("--output", required=True, type=Path, help="pasta de saída de uma execução")
    query.add_argument("--sql", required=True, type=Path, help="arquivo .sql com uma consulta")
    query.add_argument("--limit", type=int, default=50, help="linhas exibidas (padrão: 50)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    spark = create_spark_session()
    try:
        if args.command == "run":
            summary = run_pipeline(spark, args.input, args.zones, args.output)
            print(json.dumps(summary, indent=2, ensure_ascii=False))
        else:
            run_query(spark, args.output, args.sql).show(args.limit, truncate=False)
    except PipelineError as exc:
        print(f"ERRO: {exc}", file=sys.stderr)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
