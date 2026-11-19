# mobility-data-pipeline

Pipeline local para estimar a demanda de corridas de táxi amarelo (Yellow Taxi) de Nova York por **dia, hora e zona de embarque**, a partir dos registros públicos da NYC Taxi & Limousine Commission (TLC).

Projeto pessoal de portfólio. Foco em decisões de engenharia de dados: validação, regras de qualidade explícitas, publicação segura, testes e reprodutibilidade.

**Estado atual:** primeira entrega funcional. Roda em PySpark local com uma amostra sintética. Ainda não processa meses reais, não tem Airflow, CI nem integração com AWS. O plano completo está em [`docs/PLANO.md`](docs/PLANO.md).

## O que a primeira entrega faz

- Lê um Parquet com o esquema Yellow Taxi.
- Separa os registros em **válidos** e **rejeitados**. Cada rejeitado traz os motivos.
- Agrega as viagens válidas por dia, hora e zona de embarque (`trip_count`).
- Publica o resultado de forma atômica. Reexecutar a mesma entrada não duplica nada.
- Mantém os valores incomuns da fonte e apenas os conta.

Regras de qualidade: [`docs/REGRAS_QUALIDADE.md`](docs/REGRAS_QUALIDADE.md).

## Pré-requisitos

- **Python 3.11 ou superior** (testado com 3.12.3)
- **Java 17, 21 ou 25**, com `JAVA_HOME` apontando para ele. O Spark 4.3 usa Java e não funciona sem isso. Testado com OpenJDK 25.
- **Git**, para obter o repositório

## Instalação

```bash
git clone https://github.com/Joaonorr/mobility-data-pipeline.git
cd mobility-data-pipeline

python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -e ".[dev]"

export JAVA_HOME=/caminho/para/um/jdk-17-21-ou-25
```

## Baixar os meses reais (opcional)

A amostra não depende disso. Os dois meses fixos do projeto (2024-12 e 2025-01, configurados em `reference/tlc_download.json`) podem ser baixados assim:

```bash
.venv/bin/python -m mobility_pipeline download
```

- Os arquivos vão para `data/raw/` (ignorado pelo Git; não versione) e o manifesto para `data/raw/manifest.json`.
- Cada arquivo registra URL, SHA-256, tamanho e data UTC. Cada execução registra o resultado por mês.
- Reexecutar reaproveita o arquivo se o SHA-256 em disco ainda bate com o manifesto.
- Uma falha em um mês não apaga o outro nem o arquivo anterior, e o comando termina com código 1.
- A TLC não publica checksum nesta fonte. O SHA-256 garante integridade local, não a origem.

Para baixar outros meses: `--month 2025-02 --month 2025-03`. Para outra origem: `--base-url`.

## Executar a amostra

A amostra está versionada em `sample/yellow_sample.parquet` (23 registros sintéticos, com casos válidos e inválidos descritos em [`sample/CASOS.md`](sample/CASOS.md)).

```bash
python -m mobility_pipeline run \
  --input sample/yellow_sample.parquet \
  --output data/output/sample
```

O comando imprime um resumo em JSON. O resultado esperado é:

```text
"rows_read": 23,
"rows_valid": 16,
"rows_rejected": 7,
```

Os logs do Spark vão para a saída de erro. Se o resumo aparecer, a execução foi publicada.

## Consultar a saída

Três consultas SQL vêm prontas em `sql/`. Cada uma roda sobre a última execução publicada:

```bash
python -m mobility_pipeline query --output data/output/sample --sql sql/rejected_by_reason.sql
python -m mobility_pipeline query --output data/output/sample --sql sql/demand_by_hour.sql
python -m mobility_pipeline query --output data/output/sample --sql sql/top_pickup_zones.sql
```

Para escrever outra consulta, crie um arquivo `.sql` e use estas três tabelas:

| Tabela | Conteúdo |
|---|---|
| `valid` | Registros válidos, com as colunas de origem e `pickup_date`, `pickup_hour`, `pu_borough`, `pu_zone` |
| `rejected` | Registros rejeitados, com as colunas de origem e `reject_reasons` (lista de motivos) |
| `demand_hourly` | Uma linha por dia, hora e zona de embarque, com `trip_count` |

O resumo de cada execução fica em `data/output/sample/runs/<id>/run_summary.json`. Ele traz contagens, motivos, observações e duração.

### Motivos de rejeição

| Motivo | Quando ocorre |
|---|---|
| `missing_pickup_datetime` | Embarque sem data e hora |
| `missing_pickup_location` | Embarque sem zona (`PULocationID` nulo) |
| `unknown_pickup_zone` | Zona que não existe na tabela oficial da TLC |
| `dropoff_before_pickup` | Desembarque anterior ao embarque |

## Testes

```bash
pytest
```

Os testes comparam os resultados do pipeline com resultados esperados escritos à mão em `sample/expected/`. Eles não são gerados pelo código. Também cobrem:

- reexecução sem duplicação;
- entradas ausentes, truncadas, com tipo errado ou sem registros, que devem falhar sem publicar nada;
- falhas simuladas no meio da execução, que devem deixar a publicação anterior intacta.

## Regenerar a amostra Parquet

O Parquet é gerado a partir de `sample/yellow_sample_input.csv` (que é a fonte legível da amostra):

```bash
python scripts/build_sample_parquet.py
```

## Estrutura

```
src/mobility_pipeline/   código do pipeline
  transform.py           regras e agregação (PySpark puro, testável sem orquestrador)
  pipeline.py            execução, reconciliação e publicação atômica
  inputs.py              validação das entradas
  query.py, cli.py       consultas e linha de comando
tests/                   testes (pytest)
sample/                  amostra sintética, casos e resultados esperados
reference/               tabela oficial de zonas da TLC e proveniência das fontes
sql/                     consultas de exemplo
scripts/                 geração da amostra Parquet
docs/                    plano, arquitetura e regras de qualidade
data/                    saídas locais (não versionado)
```

## Documentação

- [`docs/PLANO.md`](docs/PLANO.md): problema, fonte, etapas, decisões, riscos e critérios de aceite
- [`docs/ARQUITETURA.md`](docs/ARQUITETURA.md): fluxo, modelo de publicação, decisões técnicas e caminho para AWS
- [`docs/REGRAS_QUALIDADE.md`](docs/REGRAS_QUALIDADE.md): regras de rejeição, observações e justificativas
- [`reference/FONTES.md`](reference/FONTES.md): fontes oficiais, checksums e versões de ferramentas
- [`sample/CASOS.md`](sample/CASOS.md): o que cada linha da amostra testa

## Limitações

- Os dois meses reais são baixados, mas ainda não são processados pelo pipeline. Isso é a etapa 2.
- Só a amostra sintética foi executada pelo pipeline.
- Não há partição mensal: cada execução substitui a saída inteira.
- Uma execução por pasta de saída por vez; não há lock.
- Não há Airflow, CI nem métricas de desempenho ainda.
- Nada foi testado com AWS (S3, Glue, Athena ou IAM).

## Licença

MIT. Veja [LICENSE](LICENSE).
