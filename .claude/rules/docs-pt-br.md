---
paths:
  - "docs/**"
  - "README.md"
  - "sample/CASOS.md"
  - "reference/**/*.md"
---

# Documentação

- Escreva em português (pt-BR), com acentuação correta. Identificadores técnicos (`PULocationID`, `trip_count`) ficam como estão no código.
- Não cite empresas nem vagas. Mencione só fontes públicas e técnicas: NYC TLC, Spark, Airflow, pytest.
- Todo número precisa de origem. Use uma destas formas:
  - comando executado, com o resultado observado (ex.: contagem do mês 2025-01, medida com pyarrow);
  - arquivo do repositório (ex.: `sample/expected/demand_hourly.csv`);
  - fonte oficial, com link e data de consulta em `reference/FONTES.md`.
- Não escreva "rápido", "escala bem" ou "reduz X%" sem medição registrada. Se não mediu, diga que não mediu.
- Limitações vão para uma seção explícita. Não esconda o que não foi testado: AWS, Airflow, CI e meses reais são "não testado" até existir evidência.
- Mantenha `README.md` executável: cada comando precisa ser o que realmente roda no repositório. Se mudar a CLI, atualize o README no mesmo passo.
- Links internos devem apontar para arquivos existentes. Antes de concluir, verifique com `/check-release` ou com o subagente `claim-checker`.
