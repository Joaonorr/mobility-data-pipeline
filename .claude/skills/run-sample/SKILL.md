---
name: run-sample
description: Executa o pipeline sobre a amostra sintética e confere o resumo contra os números esperados (23 lidos, 16 válidos, 7 rejeitados, motivos 3/2/3/1). Use quando for preciso rodar a amostra ou confirmar que a execução ainda está correta.
disable-model-invocation: true
---

# Executar a amostra

1. Confirme que o venv existe (`.venv/bin/python`) e que `JAVA_HOME` aponta para Java 17, 21 ou 25 (`.claude/settings.local.json`). Se não existir, pare e avise o usuário; não instale Java sozinho.
2. Rode a execução:

   ```bash
   .venv/bin/python -m mobility_pipeline run --input sample/yellow_sample.parquet --output data/output/sample
   ```

3. Confira o JSON impresso. Valores esperados, conforme `sample/CASOS.md`:
   - `rows_read`: 23
   - `rows_valid`: 16
   - `rows_rejected`: 7
   - `rejected_by_reason`: `dropoff_before_pickup` 1, `missing_pickup_datetime` 3, `missing_pickup_location` 2, `unknown_pickup_zone` 3
   - `observations`: `passenger_count_null` 1, `passenger_count_zero` 1, `dropoff_null` 1, `total_amount_negative` 1, `trip_distance_zero` 2, `payment_type_flex_fare` 1
4. Se algum número divergir, não ajuste o esperado. Compare com `sample/CASOS.md` e identifique qual lado está errado. Relate a divergência com o trecho exato da saída.
5. Se a execução terminar com código diferente de zero, mostre a linha `ERRO:` e não tente publicar de novo sem entender a causa.
6. Opcionalmente, rode as consultas de `sql/` com `query` para mostrar o resultado publicado.

Não escreva nada em `sample/expected/` nem em `reference/`.
