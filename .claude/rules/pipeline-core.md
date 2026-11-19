---
paths:
  - "src/mobility_pipeline/**/*.py"
---

# Núcleo do pipeline (PySpark)

- `transform.py` é puro: recebe e devolve DataFrames. Não importe `SparkSession`, caminhos, `cli`, `pipeline` nem Airflow aqui. Assim a lógica continua testável sem orquestrador.
- Classifique antes de separar. `classify_trips` anexa motivos sem remover linhas; `split_valid_rejected` divide. Valide que `válidos + rejeitados = lidos`.
- Um motivo de rejeição é uma constante em `transform.py` e entra em `REJECTION_REASONS`. Não crie motivo como string solta.
- Comparações com nulo não podem disparar regra por acidente. Se a regra depender de um campo que pode ser nulo, o caso nulo precisa estar coberto por teste ou comentário.
- Não gere identificadores não determinísticos nas saídas (`monotonically_increasing_id`, `uuid` dentro de linhas). A reexecução precisa produzir o mesmo conteúdo lógico.
- A sessão Spark roda com ANSI ligado (padrão do Spark 4). Evite `cast` que pode falhar silenciosamente ou lançar erro sem contexto; valide o tipo antes.
- Timestamps da fonte são `timestamp_ntz`. Não converta fuso sem uma decisão documentada em `docs/REGRAS_QUALIDADE.md`.
- Falhas esperadas lançam uma subclasse de `PipelineError` com mensagem em português para quem opera. Erros inesperados do Spark são encapsulados em `ExecutionError`.
- Publicação só acontece em `pipeline.py`, pelo ponteiro `current`. Nenhum outro módulo escreve em `<saida>/runs/` ou em `current`.
- Ao mudar uma regra, siga `/add-quality-rule`. Não altere só o código.
