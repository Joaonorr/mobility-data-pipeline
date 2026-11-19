---
name: check-release
description: Verificação completa antes de declarar uma etapa pronta. Roda a suíte de testes, a checagem estática, a amostra, a busca por nomes de empresas e a conferência de links e afirmações nos documentos. Use antes de dizer que algo está concluído.
disable-model-invocation: true
---

# Checagem de entrega

Execute as etapas na ordem. Se uma falhar, pare, mostre a falha com a saída relevante e não declare a entrega pronta.

1. **Testes:** `.venv/bin/python -m pytest`. Esperado: todos passam. Informe o total exato (hoje são 28).
2. **Estática:** `.venv/bin/python -m pyflakes src tests scripts`. Esperado: nenhuma saída.
3. **Amostra:** execute a skill `run-sample` (ou o mesmo comando) e confira os números.
4. **Empresas e vagas:** busque nos arquivos versionados (fora de `.venv/`, `.git/` e `data/`) por nomes de empresas conhecidas e termos de vaga. Esperado: nenhuma ocorrência. Ignore `LICENSE`, que já existia.
5. **Documentação:** confira que os comandos citados no `README.md` existem e que os caminhos de arquivos citados em `docs/` e `README.md` existem. Use o subagente `claim-checker` se houver muitos números.
6. **Versionamento:** `git status --short`. Confirme que não há arquivos de `data/`, `.venv/`, Parquet grande ou saídas do Spark prontos para commit. Não faça commit.

Ao final, responda com uma tabela de uma linha por etapa: passou, falhou ou não aplicável, com o número que sustenta a conclusão. Não declare "pronto" se alguma etapa não foi executada.
