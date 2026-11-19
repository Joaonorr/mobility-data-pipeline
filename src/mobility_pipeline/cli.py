"""Interface de linha de comando: 'download' obtém os meses; 'run' executa o pipeline; 'query' consulta a última publicação."""

import argparse
import json
import logging
import sys
from pathlib import Path

from mobility_pipeline.download import run_download
from mobility_pipeline.errors import PipelineError
from mobility_pipeline.pipeline import run_pipeline
from mobility_pipeline.query import run_query
from mobility_pipeline.session import create_spark_session

DEFAULT_ZONES = Path("reference/taxi_zone_lookup.csv")
DEFAULT_DOWNLOAD_CONFIG = Path("reference/tlc_download.json")
DEFAULT_RAW_DIR = Path("data/raw")


def _load_download_config(path: Path) -> dict:
    if not path.is_file():
        raise PipelineError(f"configuração de download não encontrada: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise PipelineError(f"configuração de download inválida em {path}: {exc.msg}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mobility_pipeline", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    download = commands.add_parser("download", help="baixa os meses configurados e atualiza o manifesto")
    download.add_argument(
        "--month",
        action="append",
        dest="months",
        help="mês AAAA-MM; repita para vários (padrão: meses de --config)",
    )
    download.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_DOWNLOAD_CONFIG,
        help=f"configuração com base_url e months (padrão: {DEFAULT_DOWNLOAD_CONFIG})",
    )
    download.add_argument("--base-url", help="sobrescreve base_url da configuração")
    download.add_argument("--dest", type=Path, default=DEFAULT_RAW_DIR, help=f"pasta de destino (padrão: {DEFAULT_RAW_DIR})")
    download.add_argument("--retries", type=int, default=3, help="tentativas por arquivo (padrão: 3)")

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
    if args.command == "download":
        try:
            config = _load_download_config(args.config)
            months = args.months or list(config.get("months", []))
            base_url = args.base_url or config["base_url"]
            summary = run_download(months, args.dest, base_url, retries=args.retries)
            print(json.dumps(summary, indent=2, ensure_ascii=False))
        except (PipelineError, KeyError) as exc:
            print(f"ERRO: {exc}", file=sys.stderr)
            return 1
        return 0

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
