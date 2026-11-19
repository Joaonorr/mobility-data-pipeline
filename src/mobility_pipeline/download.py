"""Download configurável de arquivos mensais Yellow Taxi da TLC, com manifesto de execução.

Regras:
- Um arquivo só é considerado completo depois de baixado inteiro, com o tamanho declarado conferido
  e com a assinatura Parquet (PAR1) no início e no fim. Até lá ele fica como `.part`.
- Cada arquivo completo tem SHA-256, tamanho, URL e data UTC no manifesto `manifest.json`.
- Reexecutar reaproveita o arquivo se o SHA-256 em disco ainda bate com o manifesto; caso contrário, baixa de novo.
- Uma falha em um mês não remove os outros meses nem apaga um arquivo completo anterior.
- O manifesto registra cada execução (id, versão, início, fim e resultado por mês).

O SHA-256 registrado é o da cópia obtida aqui. A TLC não publica checksums nesta fonte, então o
valor prova integridade e reprodutibilidade local, não autenticidade da origem.
"""

import hashlib
import http.client
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from mobility_pipeline import __version__
from mobility_pipeline.errors import PipelineError

LOGGER = logging.getLogger(__name__)

MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
PARQUET_MAGIC = b"PAR1"
CHUNK_SIZE = 1024 * 1024
USER_AGENT = f"mobility-data-pipeline/{__version__}"
MANIFEST_NAME = "manifest.json"
MANIFEST_SCHEMA_VERSION = 1


class DownloadError(PipelineError):
    """Falha ao obter um arquivo mensal. `retriable` indica se uma nova tentativa pode resolver."""

    def __init__(self, message: str, retriable: bool = False):
        super().__init__(message)
        self.retriable = retriable


def validate_month(month: str) -> str:
    if not MONTH_PATTERN.match(month):
        raise DownloadError(f"mês inválido '{month}': use o formato AAAA-MM, por exemplo 2025-01")
    return month


def month_file_name(month: str) -> str:
    return f"yellow_tripdata_{validate_month(month)}.parquet"


def month_url(base_url: str, month: str) -> str:
    return f"{base_url.rstrip('/')}/{month_file_name(month)}"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_manifest(dest: Path) -> dict:
    path = dest / MANIFEST_NAME
    if not path.exists():
        return {"schema_version": MANIFEST_SCHEMA_VERSION, "files": {}, "runs": []}
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DownloadError(f"manifesto ilegível em {path}: {exc.msg}") from exc
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise DownloadError(f"versão de manifesto não suportada em {path}")
    return manifest


def save_manifest(dest: Path, manifest: dict) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    temporary = dest / f".{MANIFEST_NAME}.tmp"
    temporary.write_text(json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, dest / MANIFEST_NAME)


def _sha256_of(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(CHUNK_SIZE):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _has_parquet_envelope(path: Path) -> bool:
    size = path.stat().st_size
    if size < 12:
        return False
    with path.open("rb") as handle:
        head = handle.read(4)
        handle.seek(-4, os.SEEK_END)
        tail = handle.read(4)
    return head == PARQUET_MAGIC and tail == PARQUET_MAGIC


def _fetch_to(url: str, part: Path, timeout: float, opener) -> tuple[str, int]:
    """Baixa `url` para `part`. Devolve (sha256, bytes). Lança DownloadError em qualquer falha."""
    request = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with opener(request, timeout=timeout) as response:
            declared = response.headers.get("Content-Length")
            digest = hashlib.sha256()
            received = 0
            with part.open("wb") as out:
                while chunk := response.read(CHUNK_SIZE):
                    out.write(chunk)
                    digest.update(chunk)
                    received += len(chunk)
    except HTTPError as exc:
        retriable = exc.code == 429 or exc.code >= 500
        raise DownloadError(f"HTTP {exc.code} ao baixar {url}", retriable=retriable) from exc
    except (URLError, TimeoutError, ConnectionError, http.client.HTTPException) as exc:
        reason = getattr(exc, "reason", None) or exc
        raise DownloadError(f"falha de rede ao baixar {url}: {reason}", retriable=True) from exc
    except OSError as exc:
        raise DownloadError(f"erro ao gravar {part}: {exc}") from exc

    if declared is not None and int(declared) != received:
        raise DownloadError(
            f"download incompleto de {url}: o servidor declarou {declared} bytes e chegaram {received}",
            retriable=True,
        )
    return digest.hexdigest(), received


def download_month(
    month: str,
    dest: Path,
    base_url: str,
    *,
    manifest: dict,
    retries: int = 3,
    backoff_seconds: float = 2.0,
    timeout: float = 60.0,
    opener=urlopen,
    sleep=time.sleep,
) -> dict:
    """Garante que o arquivo do mês existe em `dest` e está registrado no manifesto.

    Devolve um dicionário com `month`, `outcome` ("reused" ou "downloaded"), `file`, `sha256`, `bytes` e `url`.
    Atualiza `manifest["files"]` em memória; quem chama é responsável por gravá-lo.
    """
    validate_month(month)
    url = month_url(base_url, month)
    name = month_file_name(month)
    final = dest / name
    previous = manifest["files"].get(month)

    if final.is_file() and previous and previous.get("file") == name:
        sha, size = _sha256_of(final)
        if sha == previous["sha256"] and size == previous["bytes"]:
            LOGGER.info("%s: arquivo íntegro já existe, reaproveitado", month)
            return {"month": month, "outcome": "reused", "file": name, "sha256": sha, "bytes": size, "url": url}
        LOGGER.warning("%s: arquivo em disco não bate com o manifesto; baixando de novo", month)

    dest.mkdir(parents=True, exist_ok=True)
    part = dest / f".{name}.part"
    last_error: DownloadError | None = None
    for attempt in range(1, retries + 1):
        try:
            sha, size = _fetch_to(url, part, timeout, opener)
            if not _has_parquet_envelope(part):
                raise DownloadError(f"o arquivo baixado de {url} não é um Parquet válido (PAR1 ausente)")
            os.replace(part, final)
            entry = {"month": month, "url": url, "file": name, "sha256": sha, "bytes": size, "downloaded_at": _utc_now()}
            manifest["files"][month] = {k: v for k, v in entry.items() if k != "month"}
            return {**entry, "outcome": "downloaded"}
        except DownloadError as exc:
            last_error = exc
            part.unlink(missing_ok=True)
            if not exc.retriable or attempt == retries:
                break
            LOGGER.warning("%s: tentativa %s de %s falhou (%s); nova tentativa", month, attempt, retries, exc)
            sleep(backoff_seconds * attempt)
    assert last_error is not None
    raise last_error


def run_download(
    months: list[str],
    dest: Path,
    base_url: str,
    **options,
) -> dict:
    """Baixa todos os meses pedidos, grava o manifesto desta execução e devolve o resumo.

    Se algum mês falhar, os demais são processados e a função levanta DownloadError ao final,
    com o resumo completo gravado no manifesto.
    """
    if not months:
        raise DownloadError("nenhum mês informado")
    for month in months:
        validate_month(month)
    if len(set(months)) != len(months):
        raise DownloadError("meses repetidos na lista")

    run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
    started_at = _utc_now()
    manifest = load_manifest(dest)
    results: list[dict] = []
    failures: list[str] = []

    for month in months:
        try:
            results.append(download_month(month, dest, base_url, manifest=manifest, **options))
        except DownloadError as exc:
            LOGGER.error("%s: %s", month, exc)
            failures.append(f"{month}: {exc}")
            results.append({"month": month, "outcome": "failed", "error": str(exc), "url": month_url(base_url, month)})

    run = {
        "run_id": run_id,
        "pipeline_version": __version__,
        "started_at": started_at,
        "finished_at": _utc_now(),
        "base_url": base_url,
        "results": results,
    }
    manifest["runs"].append(run)
    save_manifest(dest, manifest)

    if failures:
        raise DownloadError(f"falha em {len(failures)} mês(es): " + "; ".join(failures))
    return {"run_id": run_id, "dest": str(dest), "results": results}
