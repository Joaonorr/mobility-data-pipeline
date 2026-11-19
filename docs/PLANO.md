# Plano do projeto

Documento vivo. Registra o que foi decidido, o que está feito e o que falta. Estado em 2026-11-19.

## 1. Problema e pergunta analítica

Quantas corridas de táxi amarelo começam em cada zona de Nova York, por dia e por hora?

A pergunta se responde com a agregação `demand_hourly` (dia × hora × zona de embarque, com `trip_count`). O projeto também precisa mostrar **o que foi descartado e por quê**, porque a qualidade da fonte afeta a resposta.

## 2. Fonte e campos

Fonte: registros mensais Yellow Taxi publicados pela NYC TLC (página oficial: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). O dicionário oficial está em `reference/FONTES.md`.

| Campo | Uso | Observação |
|---|---|---|
| `tpep_pickup_datetime` | Dia e hora da demanda | Obrigatório; sem fuso |
| `PULocationID` | Zona de embarque | Obrigatório; ID de 1 a 265 |
| `tpep_dropoff_datetime` | Validação (embarque antes do desembarque) | Obrigatório para essa regra |
| Demais 17 colunas | Preservadas nas saídas | Não obrigatórias, para tolerar mudanças de esquema entre anos |

Não há ID único de corrida. Não há deduplicação.

## 3. Arquitetura proposta

Resumo em `docs/ARQUITETURA.md`. Em uma frase: funções PySpark puras (`transform.py`) são chamadas por uma função de execução (`pipeline.py`) que valida a entrada, reconcilia as contagens e troca a publicação de forma atômica. O Airflow entra depois, como um chamador dessa mesma função.

## 4. Primeira entrega funcional (concluída)

- [x] Amostra sintética versionada: 23 registros com 20 colunas, incluindo casos válidos e inválidos (`sample/`, casos em `sample/CASOS.md`).
- [x] Resultados esperados escritos à mão, sem usar o código (`sample/expected/`).
- [x] Classificação, separação e agregação em PySpark (`src/mobility_pipeline/transform.py`).
- [x] Motivos de rejeição gravados em cada registro rejeitado; contagem consultável em `rejected_by_reason.sql`.
- [x] Reexecução não duplica resultados (teste de idempotência).
- [x] Entradas ausentes, corrompidas ou incompatíveis produzem erro claro e não publicam nada.
- [x] Comando documentado: `python -m mobility_pipeline run ...` e `query ...`.
- [x] README de instalação e uso.

## 5. Etapas seguintes

| Etapa | Escopo | Decisão já tomada |
|---|---|---|
| 1. Download de dois meses reais | **Concluída.** `python -m mobility_pipeline download` baixa do CloudFront, registra URL, SHA-256, tamanho e data em `data/raw/manifest.json`, e grava o resultado de cada execução. Arquivos grandes ficam fora do Git. | Meses fixados: `2024-12` (sem `cbd_congestion_fee`) e `2025-01` (com ela). Testa a tolerância a mudança de esquema. Ambos foram baixados e têm as colunas exigidas (ver `reference/FONTES.md`). |
| 2. Processamento mensal particionado | **Concluída na branch `feat/etapa-2-particao-mensal`.** `run --month AAAA-MM` publica `<saida>/month=AAAA-MM/`, com ponteiro próprio. Reprocessar um mês troca só a partição dele. `query` une as partições. | Embarque fora do mês é rejeitado com `outside_partition_month`. Lacuna de 21 corridas de 31/12/2024 medida e documentada em `docs/REGRAS_QUALIDADE.md`. |
| 3. DAG do Airflow | Tarefas: ingestão, validação, transformação e publicação, com retries e reprocessamento de um mês. | Fixar `apache-airflow` depois de confirmar a versão e a data de publicação no PyPI. |
| 4. Métricas por execução | Lidos, válidos, rejeitados, duração, partição e status. Falha nunca publica saída incompleta. | Já existe um `run_summary.json` com parte disso; a etapa 4 o estende a todas as partições. |
| 5. CI no GitHub Actions | Testes e verificações estáticas (ex.: ruff). Não baixa os meses completos; usa só a amostra. | Testes continuam na amostra; sem dados grandes no CI. |
| 6. Integração com S3 ou emulador | Opcional. Só depois do núcleo estar sólido. | O que for testado será descrito como tal. Emulação não valida Glue, Athena ou IAM. |

## 6. Decisões

| Decisão | Escolha | Por quê | Reversível? |
|---|---|---|---|
| Idioma da documentação | Português | O pedido e o público do README são em português | Sim |
| Idioma do código | Inglês nos identificadores e português nas mensagens de erro | Convenção comum em código; mensagens são para quem opera | Sim |
| Regra de desembarque antes do embarque | Rejeita, com motivo próprio | Intervalo impossível; a fonte não diz qual timestamp está errado | Sim (trocar uma linha) |
| Valores incomuns | Mantidos e contados | Pedido explícito; a fonte tem estornos e códigos especiais | Sim |
| Deduplicação | Não existe | Sem chave na fonte | Sim, se surgir uma chave confiável |
| Publicação | Pasta temporária + reconciliação + ponteiro atômico | Atende ao critério de não publicar saída incompleta | Sim |
| Versões | Fixadas: `pyspark==4.3.0`, `pyarrow==26.0.1`, `pytest==9.2.0` | Reprodutibilidade | Sim |
| Java | Recomendado 17, 21 ou 25 (docs do Spark); testado com 25 | Exigência do Spark 4.3 | Sim |

## 7. Riscos

| Risco | Impacto | Mitigação |
|---|---|---|
| `nyc.gov` bloqueia download direto (HTTP 403 para `curl`) | Etapa 1 falha | Usar o host do CloudFront, documentado em `reference/FONTES.md` |
| Esquema muda entre meses ou é padronizado pela TLC | Colunas somem ou mudam de tipo | Só as colunas usadas são obrigatórias; falha clara em caso de tipo incompatível |
| Java ausente ou versão não suportada | Spark não inicia | README indica as versões; mensagem de erro do Spark é repassada |
| Dados reais têm volume maior que a amostra | Testes lentos ou execução pesada | Testes continuam na amostra; medição de desempenho só com dados reais e registrada como observação |
| Lógica de "fora do mês" mal definida | Dupla contagem ou perda de registros na etapa 2 | Cada corrida entra em uma única partição; teste de corrida presente em dois arquivos; lacuna medida e documentada |
| Fonte com corridas de borda ausentes no mês vizinho | Corridas de 31/12/2024 sem partição (21 casos em 2025-01) | Medido e documentado; decisão de fechamento de fronteira fica para depois |

## 8. Critérios de aceite

| Critério | Estado |
|---|---|
| Uma pessoa sem contexto executa a amostra seguindo só o README | Parcial: verificado em cópia limpa do repositório, com venv novo e os comandos do README (resultado igual ao esperado). Falta uma pessoa externa testar. |
| Testes usam resultados esperados definidos independentemente da implementação | Atendido para a amostra (`sample/expected/`, escritos à mão) |
| Dois meses reais processados separadamente | Pendente (etapas 1 e 2) |
| Reprocessar um mês não duplica resultados | Atendido na amostra (teste de idempotência); pendente para meses reais |
| Registros rejeitados com contagem e motivo consultáveis | Atendido (`rejected_by_reason.sql`, `run_summary.json`) |
| Entrada ausente, corrompida ou incompatível: erro claro, sem publicação incompleta | Atendido e testado |
| Repositório explica arquitetura, decisões, limitações e caminho para AWS | Parcial: `docs/ARQUITETURA.md` cobre isso; atualizar ao final de cada etapa |
| CI executa verificações sem baixar meses completos | Pendente (etapa 5) |
