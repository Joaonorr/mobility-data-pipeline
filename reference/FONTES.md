# Fontes e proveniência

Todas as consultas foram feitas em 2026-11-19.

## Dados de referência versionados

| Arquivo | Origem | SHA-256 | Observação |
|---|---|---|---|
| `reference/taxi_zone_lookup.csv` | https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv (link da página oficial da TLC) | `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed` | 265 zonas (LocationID 1 a 265, sem lacunas). Inclui as zonas 264 ("N/A") e 265 ("Outside of NYC"). |

## Documentação oficial consultada

- Página de dados de viagens da TLC: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Dicionário de dados Yellow Taxi (versão de 18/03/2025): https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
  - Texto extraído com `pdftotext -layout`. Confirma `payment_type 0 = Flex Fare trip`, `RatecodeID 99 = Null/unknown` e que `trip_distance` é em milhas.
  - O dicionário não define um ID único de corrida nem o domínio de `passenger_count`.
  - Ele lista `airport_fee` em minúsculas; o Parquet real usa `Airport_fee`. O pipeline não depende dessa coluna.
- Guia do usuário de trip records: https://www.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf (não lido nesta entrega).

## Meses reais do projeto (baixados por `python -m mobility_pipeline download`, não versionados)

Configuração em `reference/tlc_download.json`. Os arquivos ficam em `data/raw/` e o manifesto em `data/raw/manifest.json`.

| Mês | URL | Bytes | SHA-256 (cópia obtida em 2026-11-19) | Registros | Colunas | `cbd_congestion_fee` |
|---|---|---|---|---|---|---|
| 2024-12 | `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet` | 61.524.085 | `41ebf7db80bebde60c58e5143c14cdf38ad04a0f3e3ff44215b3e240d55f6c78` | 3.668.371 | 19 | não |
| 2025-01 | `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet` | 59.158.238 | `9af277e4c0d3f9deb30644da822981e1e7df6af58313170fd3aa8a474485488a` | 3.475.226 | 20 | sim |

Contagens e colunas medidas com pyarrow 26.0.1 sobre os arquivos baixados. Os dois têm `tpep_pickup_datetime` como `timestamp[us]` (sem fuso) e `PULocationID` como `int32`.

O SHA-256 prova integridade e reprodutibilidade local. A TLC não publica checksum nesta fonte; não há como confirmar a autenticidade do arquivo por ela.

## Arquivo de amostra usado para verificar o esquema (2025-01)

Mesmo arquivo de `data/raw/yellow_tripdata_2025-01.parquet`, usado nas medições de `docs/REGRAS_QUALIDADE.md` antes de o download existir.
- Observação técnica: `nyc.gov` respondeu HTTP 403 para download direto por `curl`. O host do CloudFront (`d37ci6vzurychx.cloudfront.net`) respondeu normalmente. A etapa de download deve usar o CloudFront e registrar a URL usada.

## Versões de ferramentas

| Ferramenta | Versão fixada | Fonte | Observação |
|---|---|---|---|
| pyspark | 4.3.0 | PyPI (`info.version`); docs oficiais do Spark | Python >= 3.11. Docs: "Spark runs on Java 17/21/25". |
| pyarrow | 26.0.1 | PyPI | Python >= 3.11. Usado só em desenvolvimento. |
| pytest | 9.2.0 | PyPI | Python >= 3.10. |
| apache-airflow | 3.4.0 (ainda não fixado) | PyPI | Python `!=3.15,>=3.11`. Será fixado na etapa 3. |
