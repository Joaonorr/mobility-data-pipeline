---
paths:
  - "sample/**"
  - "tests/**"
---

# Amostra e testes

- A amostra é a fonte da verdade dos testes. `sample/yellow_sample_input.csv` é editado à mão; `sample/yellow_sample.parquet` é gerado por `scripts/build_sample_parquet.py`. Não edite o Parquet diretamente.
- Resultados esperados (`sample/expected/*.csv`) são escritos à mão a partir de `sample/CASOS.md`. Nunca copie a saída do pipeline para lá. Se precisar mudar um esperado, mude primeiro o caso documentado e explique a mudança no commit.
- Cada linha de dado da amostra tem um caso em `CASOS.md` com o número da linha (L01 a L23). Ao adicionar uma linha, atualize o caso e as contagens (válidos, rejeitados e motivos).
- Os testes não podem usar o código da implementação para calcular o esperado. Use os CSVs ou contagens escritas no próprio teste.
- Teste de regra: ao alterar uma regra, verifique que pelo menos um teste falha com a regra antiga e passa com a nova. Já vimos que uma mutação simples (`<` para `<=`) derruba oito testes.
- Testes que falham não devem ser "consertados" alterando a expectativa sem justificativa documentada.
- Não adicione dados reais ao repositório para testes. Dados reais ficam fora do Git, e o CI usa só a amostra.
- Testes de entrada inválida criam arquivos em `tmp_path`, não dentro de `sample/`.
