# Arquitetura

Estado: **primeira entrega funcional local** (amostra sintética) e **etapa 1** (download dos dois meses reais com manifesto). Os blocos marcados como "próxima etapa" não existem ainda.

## Fluxo

```
  Fonte TLC (Parquet mensal)            Tabela oficial de zonas (CSV)
  [etapa 1, feita: download com          reference/taxi_zone_lookup.csv
   URL, SHA-256 e manifesto em data/raw]            |
              |                                     |
              v                                     v
  +---------------------------------------------------------------+
  | mobility_pipeline.inputs    valida existência, Parquet,       |
  |                             esquema, tipos e registros        |
  +---------------------------------------------------------------+
              |
              v
  +---------------------------------------------------------------+
  | mobility_pipeline.transform  classify_trips  (motivos, sem    |
  |                              remover linhas)                  |
  |                              split_valid_rejected              |
  |                              aggregate_demand (dia x hora x zona) |
  +---------------------------------------------------------------+
              |
              v
  +---------------------------------------------------------------+
  | mobility_pipeline.pipeline  grava em runs/.tmp-<id>/          |
  |                             reconcilia contagens              |
  |                             renomeia para runs/<id>/          |
  |                             troca o ponteiro current (atômico)  |
  +---------------------------------------------------------------+
              |
              v
  <saida>/current  ->  runs/<id>/
                        valid/          registros válidos
                        rejected/       registros rejeitados + reject_reasons
                        demand_hourly/  viagens por dia, hora e zona de embarque
                        run_summary.json  contagens, motivos, observações, duração
              |
              v
  mobility_pipeline.query  ->  SQL (sql/*.sql) sobre a execução publicada
```

O `transform.py` não importa Spark session, caminhos nem Airflow. Recebe DataFrames e devolve DataFrames, então pode ser testado sem iniciar nenhum orquestrador.

## Modelo de publicação

Uma execução só substitui a anterior depois de estar completa e reconciliada:

1. A entrada é validada antes de qualquer pasta de saída ser criada.
2. As três tabelas e o resumo são gravados em `runs/.tmp-<id>/`.
3. `_reconcile` relê os arquivos gravados e confere: `válidos + rejeitados = lidos`, e a soma de `trip_count` é igual ao número de válidos.
4. A pasta é renomeada para `runs/<id>/`. Em um mesmo sistema de arquivos, essa renomeação é atômica.
5. O arquivo `current` é reescrito via `os.replace` (substituição atômica) para apontar para `<id>`.
6. As execuções antigas são removidas.

Se qualquer etapa falha antes do passo 5, `current` continua apontando para a última execução válida, e a pasta temporária é removida. Os testes em `tests/test_input_validation.py` verificam esse comportamento para entradas inválidas e para falhas simuladas na agregação, na reconciliação e na troca do ponteiro. Falhas de escrita em disco não foram simuladas.

**Limitação conhecida:** o modelo supõe uma execução por pasta de saída por vez. Não há lock. Duas execuções simultâneas na mesma pasta podem se atrapalhar na limpeza das execuções antigas.

## Decisões técnicas

| Decisão | Escolha | Motivo | Alternativa descartada |
|---|---|---|---|
| Motor de transformação | PySpark 4.3.0 em modo local (`local[*]`) | Pedido do projeto; o mesmo código pode rodar em cluster depois | Pandas (não avaliado em volume; PySpark é requisito do projeto) |
| Formato das saídas | Parquet | Esquema tipado, leitura eficiente pelo próprio Spark e por SQL | CSV (perde tipos; arquivo maior) |
| Tipo do embarque | Leitura como `timestamp_ntz`, exigida na validação | O Parquet da TLC grava horário sem fuso; com fuso, a hora do dia poderia mudar | Converter para UTC (muda a hora de agregação) |
| Fuso da sessão | `UTC` (`spark.sql.session.timeZone`) | Não afeta `timestamp_ntz`; deixa explícito que nenhuma conversão ocorre | Fuso do sistema operacional (depende da máquina) |
| Identidade de registros | Nenhuma chave artificial; registros idênticos são mantidos | A fonte não tem ID de corrida; uma chave inventada removeria corridas reais | `monotonically_increasing_id` (não é estável entre execuções) |
| Reprocessamento | Saída inteira recalculada; publicação por ponteiro | Garante que reexecutar produz o mesmo resultado lógico, sem acréscimo | Escrita em modo `append` (duplica resultados) |
| Orquestração | Ainda não há; `run_pipeline` é uma função chamável | Permite testar a lógica sem Airflow; a DAG chamará essa função na etapa 3 | Lógica dentro de tarefas do Airflow |
| Versões | Fixadas em `pyproject.toml` (`pyspark==4.3.0`, `pyarrow==26.0.1`, `pytest==9.2.0`) | Reprodutibilidade | Versões flexíveis (`>=`) |

### Java

O Spark 4.3 exige Java 17, 21 ou 25 (docs oficiais). Este projeto foi executado com OpenJDK 25.0. O JDK 24 instalado na máquina de desenvolvimento não está na lista oficial e não foi testado.

## Caminho para operar na AWS (não implementado nem testado)

O fluxo principal não depende de AWS. A evolução proposta é:

- **Armazenamento:** `s3://<bucket>/raw/...` para os arquivos da TLC (com URL e checksum no metadado) e `s3://<bucket>/curated/...` para as saídas particionadas por mês.
- **Processamento:** o mesmo código PySpark rodaria em Spark gerenciado (ex.: EMR ou Glue). Esse ponto **não foi testado**.
- **Consulta:** as saídas em Parquet poderiam ser consultadas por Athena. **Não foi testado**.
- **Permissões:** um papel IAM com acesso somente aos prefixos do projeto. **Não foi testado.**
- **Testes locais contra S3:** uma emulação (ex.: LocalStack) validaria apenas a integração de leitura e escrita com a API S3. **Não validaria** Glue, Athena nem IAM.

Nada disso foi executado nesta entrega. A integração com AWS é a etapa 6 do plano e só começa depois que o núcleo local e o CI estiverem sólidos.
