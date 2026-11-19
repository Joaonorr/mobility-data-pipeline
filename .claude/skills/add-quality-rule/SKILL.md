---
name: add-quality-rule
description: Procedimento para criar ou alterar uma regra de qualidade (rejeição ou observação) do pipeline, do motivo à verificação. Use quando uma regra nova ou mudada for pedida. Recebe o nome da regra como argumento.
argument-hint: "<nome-da-regra> (ex.: dropoff_before_pickup)"
disable-model-invocation: true
---

# Alterar uma regra de qualidade: $ARGUMENTS

Siga a ordem. Não pule etapas: a justificativa vem antes do código.

1. **Justificativa.** Escreva ou atualize a entrada em `docs/REGRAS_QUALIDADE.md`: regra, campo(s), motivo e evidência. A evidência deve ser medida no arquivo real (ex.: `2025-01`, com pyarrow) ou explicitamente marcada como "defensiva, sem caso observado". Se a regra for só observação, diga que ela não remove linhas.
2. **Código.** Em `src/mobility_pipeline/transform.py`:
   - para rejeição: constante `REASON_*`, entrada em `REJECTION_REASONS` e expressão em `classify_trips`;
   - para observação: entrada em `count_observations`.
   Não altere `pipeline.py` para isso.
3. **Casos.** Acrescente linhas em `sample/yellow_sample_input.csv` que exercitem a regra nos dois sentidos (caso que dispara e caso vizinho que não dispara). Documente cada linha em `sample/CASOS.md` com o número da linha.
4. **Parquet.** Regenere com `.venv/bin/python scripts/build_sample_parquet.py`.
5. **Esperados à mão.** Atualize `sample/expected/*.csv` e as contagens em `tests/test_sample_results.py` a partir dos casos, não a partir da saída do código. Confirme cada número na tabela antes de salvar.
6. **Testes.** Adicione ou ajuste o teste da regra. Verifique que pelo menos um teste falha com a regra antiga (mutação temporária, restaurada em seguida e confirmada com `cmp`).
7. **Verificação.** Rode `.venv/bin/python -m pytest` e confirme que o resumo da amostra bate com os casos. Depois use `/check-release`.
8. **Registro.** Se a regra for uma decisão nova, registre em `docs/PLANO.md` (seção 6).

Não faça commit. Ao final, liste os arquivos alterados, a evidência usada e o resultado dos testes.
