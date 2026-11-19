"""Download dos meses mensais: testes com servidor HTTP local (sem rede externa e sem dados reais)."""

import hashlib
import http.server
import json
import threading

import pytest

from mobility_pipeline.cli import main
from mobility_pipeline.download import (
    DownloadError,
    load_manifest,
    month_file_name,
    month_url,
    run_download,
    validate_month,
)

PARQUET_BODY = b"PAR1" + b"0123456789" * 20 + b"PAR1"
OTHER_BODY = b"PAR1" + b"abcdefghij" * 20 + b"PAR1"


class FakeTlc:
    """Servidor local. Cada caminho tem uma fila de comportamentos; o último é repetido indefinidamente."""

    def __init__(self):
        self.routes: dict[str, list[tuple]] = {}
        self.hits: list[str] = []
        handler = self._handler()
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def base_url(self) -> str:
        host, port = self.server.server_address
        return f"http://{host}:{port}/trip-data"

    def set(self, month: str, *behaviours: tuple) -> None:
        self.routes[f"/trip-data/{month_file_name(month)}"] = list(behaviours)

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def _handler(self):
        fake = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):  # silencia o log do servidor de teste
                pass

            def do_GET(self):
                fake.hits.append(self.path)
                queue = fake.routes.get(self.path)
                if not queue:
                    self.send_error(404)
                    return
                behaviour = queue.pop(0) if len(queue) > 1 else queue[0]
                kind = behaviour[0]
                if kind == "ok":
                    body = behaviour[1]
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                elif kind == "status":
                    self.send_error(behaviour[1])
                elif kind == "truncated":
                    body, declared = behaviour[1], behaviour[2]
                    self.send_response(200)
                    self.send_header("Content-Length", str(declared))
                    self.end_headers()
                    self.wfile.write(body)
                    self.close_connection = True

        return Handler


@pytest.fixture
def tlc():
    server = FakeTlc()
    yield server
    server.close()


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


FAST = {"backoff_seconds": 0.0, "retries": 3}


def test_url_and_month_validation():
    assert month_file_name("2025-01") == "yellow_tripdata_2025-01.parquet"
    assert month_url("https://host/trip-data/", "2024-12") == "https://host/trip-data/yellow_tripdata_2024-12.parquet"
    for bad in ["2025-13", "2025-00", "25-01", "2025-1", "2025/01", ""]:
        with pytest.raises(DownloadError):
            validate_month(bad)


def test_downloads_file_and_records_checksum_in_manifest(tlc, tmp_path):
    tlc.set("2025-01", ("ok", PARQUET_BODY))
    dest = tmp_path / "raw"

    summary = run_download(["2025-01"], dest, tlc.base_url, **FAST)

    result = summary["results"][0]
    assert result["outcome"] == "downloaded"
    assert (dest / "yellow_tripdata_2025-01.parquet").read_bytes() == PARQUET_BODY
    assert not list(dest.glob("*.part"))

    manifest = load_manifest(dest)
    entry = manifest["files"]["2025-01"]
    assert entry["sha256"] == _sha(PARQUET_BODY)
    assert entry["bytes"] == len(PARQUET_BODY)
    assert entry["url"] == tlc.base_url + "/yellow_tripdata_2025-01.parquet"
    assert entry["downloaded_at"].endswith("+00:00")
    assert len(manifest["runs"]) == 1
    assert manifest["runs"][0]["results"][0]["outcome"] == "downloaded"


def test_rerun_reuses_verified_file_without_new_request(tlc, tmp_path):
    tlc.set("2025-01", ("ok", PARQUET_BODY))
    dest = tmp_path / "raw"
    run_download(["2025-01"], dest, tlc.base_url, **FAST)
    hits_after_first = len(tlc.hits)

    summary = run_download(["2025-01"], dest, tlc.base_url, **FAST)

    assert summary["results"][0]["outcome"] == "reused"
    assert len(tlc.hits) == hits_after_first
    manifest = load_manifest(dest)
    assert len(manifest["runs"]) == 2
    assert manifest["runs"][1]["results"][0]["outcome"] == "reused"


def test_local_file_that_differs_from_manifest_is_downloaded_again(tlc, tmp_path):
    tlc.set("2025-01", ("ok", PARQUET_BODY))
    dest = tmp_path / "raw"
    run_download(["2025-01"], dest, tlc.base_url, **FAST)
    (dest / "yellow_tripdata_2025-01.parquet").write_bytes(OTHER_BODY)
    tlc.set("2025-01", ("ok", PARQUET_BODY))

    summary = run_download(["2025-01"], dest, tlc.base_url, **FAST)

    assert summary["results"][0]["outcome"] == "downloaded"
    assert (dest / "yellow_tripdata_2025-01.parquet").read_bytes() == PARQUET_BODY


def test_failed_month_does_not_block_other_months_and_is_recorded(tlc, tmp_path):
    tlc.set("2024-12", ("status", 404))
    tlc.set("2025-01", ("ok", PARQUET_BODY))
    dest = tmp_path / "raw"

    with pytest.raises(DownloadError, match="2024-12"):
        run_download(["2024-12", "2025-01"], dest, tlc.base_url, **FAST)

    manifest = load_manifest(dest)
    assert "2024-12" not in manifest["files"]
    assert manifest["files"]["2025-01"]["sha256"] == _sha(PARQUET_BODY)
    failed = [r for r in manifest["runs"][0]["results"] if r["month"] == "2024-12"][0]
    assert failed["outcome"] == "failed"
    assert "HTTP 404" in failed["error"]
    assert not (dest / "yellow_tripdata_2024-12.parquet").exists()
    assert not list(dest.glob("*.part"))


def test_transient_server_error_is_retried(tlc, tmp_path):
    tlc.set("2024-12", ("status", 503), ("ok", PARQUET_BODY))
    dest = tmp_path / "raw"

    summary = run_download(["2024-12"], dest, tlc.base_url, **FAST)

    assert summary["results"][0]["outcome"] == "downloaded"
    assert tlc.hits.count("/trip-data/yellow_tripdata_2024-12.parquet") == 2


def test_truncated_transfer_is_never_published(tlc, tmp_path):
    tlc.set("2024-12", ("truncated", PARQUET_BODY[:10], len(PARQUET_BODY)))
    dest = tmp_path / "raw"

    with pytest.raises(DownloadError, match="incompleto|falha de rede"):
        run_download(["2024-12"], dest, tlc.base_url, **FAST)

    assert not (dest / "yellow_tripdata_2024-12.parquet").exists()
    assert not list(dest.glob("*.part"))
    assert "2024-12" not in load_manifest(dest)["files"]


def test_response_that_is_not_parquet_is_rejected_without_retry(tlc, tmp_path):
    tlc.set("2024-12", ("ok", b"<html>acesso negado</html>"))
    dest = tmp_path / "raw"

    with pytest.raises(DownloadError, match="PAR1") as info:
        run_download(["2024-12"], dest, tlc.base_url, **FAST)

    assert info.value.retriable is False
    assert tlc.hits.count("/trip-data/yellow_tripdata_2024-12.parquet") == 1
    assert not (dest / "yellow_tripdata_2024-12.parquet").exists()


def test_failed_redownload_keeps_previous_manifest_entry(tlc, tmp_path):
    tlc.set("2025-01", ("ok", PARQUET_BODY))
    dest = tmp_path / "raw"
    run_download(["2025-01"], dest, tlc.base_url, **FAST)
    before = load_manifest(dest)["files"]["2025-01"]
    (dest / "yellow_tripdata_2025-01.parquet").write_bytes(OTHER_BODY)
    tlc.set("2025-01", ("status", 404))

    with pytest.raises(DownloadError):
        run_download(["2025-01"], dest, tlc.base_url, **FAST)

    assert load_manifest(dest)["files"]["2025-01"] == before


def test_invalid_or_repeated_months_are_refused_before_any_request(tlc, tmp_path):
    with pytest.raises(DownloadError, match="mês inválido"):
        run_download(["2025-13"], tmp_path / "raw", tlc.base_url, **FAST)
    with pytest.raises(DownloadError, match="repetidos"):
        run_download(["2025-01", "2025-01"], tmp_path / "raw", tlc.base_url, **FAST)
    assert tlc.hits == []


def test_cli_downloads_configured_months(tlc, tmp_path, capsys):
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"base_url": tlc.base_url, "months": ["2025-01"]}), encoding="utf-8")
    tlc.set("2025-01", ("ok", PARQUET_BODY))
    dest = tmp_path / "raw"

    code = main(["download", "--config", str(config), "--dest", str(dest), "--retries", "1"])

    assert code == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed["results"][0]["outcome"] == "downloaded"
    assert (dest / "yellow_tripdata_2025-01.parquet").is_file()


def test_cli_reports_missing_config_with_exit_code_one(tmp_path, capsys):
    code = main(["download", "--config", str(tmp_path / "nao_existe.json"), "--dest", str(tmp_path / "raw")])
    assert code == 1
    assert "configuração de download não encontrada" in capsys.readouterr().err
