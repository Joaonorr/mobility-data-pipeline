# mobility-data-pipeline

Pipeline local de demanda de corridas Yellow Taxi (NYC TLC) por dia, hora e zona de embarque.
Projeto pessoal de portfólio: a qualidade das decisões de engenharia importa mais que a quantidade de features.

Estado e próximos passos: `docs/PLANO.md`. Arquitetura e decisões: `docs/ARQUITETURA.md`. Regras de qualidade: `docs/REGRAS_QUALIDADE.md`.

## Comandos

Use o Python do venv diretamente; não há `activate` persistente entre comandos.

```bash
.venv/bin/python -m pytest                                   # suíte completa (28 testes, ~25 s)
.venv/bin/python -m mobility_pipeline download                # baixa os 2 meses de reference/tlc_download.json para data/raw (manifesto)
.venv/bin/python -m mobility_pipeline run --input sample/yellow_sample.parquet --month 2025-01 --output data/output/sample   # cria data/output/sample/month=2025-01/
.venv/bin/python -m mobility_pipeline query --output data/output/sample --sql sql/rejected_by_reason.sql   # une as partições publicadas
.venv/bin/python scripts/build_sample_parquet.py             # regenera sample/yellow_sample.parquet
.venv/bin/python -m pyflakes src tests scripts               # checagem estática (pyflakes, incluído em [dev])
```

Java 17, 21 ou 25 é obrigatório para o Spark. `JAVA_HOME` fica em `.claude/settings.local.json` nesta máquina. **O JDK 24 não é suportado pelo Spark 4.3 e não foi testado.**

## Layout

- `src/mobility_pipeline/`: `transform.py` (regras e agregação, puro), `pipeline.py` (execução e publicação), `inputs.py` (validação), `download.py` (download e manifesto), `query.py`, `cli.py`
- `tests/`: testes pytest; `helpers.py` lê os resultados esperados
- `sample/`: amostra sintética (`yellow_sample_input.csv` é a fonte legível), `CASOS.md`, `expected/` (escrito à mão)
- `reference/`: tabela oficial de zonas e `FONTES.md` (URLs, SHA-256, versões)
- `sql/`: consultas de exemplo sobre a última publicação
- `scripts/`: geração da amostra Parquet
- `data/`: saídas locais, ignoradas pelo Git

## Regras que não podem ser quebradas

- **Não commitar nem fazer push sem pedido explícito.** Edições na árvore de trabalho são livres; commit e push pedem confirmação.
- **Resultados esperados são escritos à mão** a partir de `sample/CASOS.md`. Nunca gere `sample/expected/*.csv` a partir da saída do código. Se um teste falhar, decida pelo caso documentado: quem está errado, o código ou a expectativa?
- **Nenhuma linha é removida por parecer incomum.** Toda rejeição tem motivo, regra escrita em `docs/REGRAS_QUALIDADE.md` e evidência. Valores incomuns são contados nas observações.
- **Não há deduplicação.** A fonte não tem ID de corrida. Não invente uma chave.
- **Não cite empresas nem vagas** em código, documentação, comentários ou mensagens. O projeto é de portfólio para processos seletivos diversos.
- **Não invente medições.** Número em documentação precisa ter origem: comando executado, arquivo medido ou fonte oficial citada. Registre só o que foi executado e observado.
- **Documentação e mensagens de erro em português.** Identificadores de código em inglês.
- **Não versione arquivos grandes.** Dados reais vão para `data/` (ignorado). Só a amostra de poucos KB fica no Git.
- **Decisões técnicas reversíveis:** decida, documente em `docs/PLANO.md` (seção 6) e siga. Pergunte só quando a escolha depender de algo que só o usuário sabe.

## Mudanças de regra e de dados

Para adicionar ou alterar uma regra de qualidade, use `/add-quality-rule`. Ele segue a ordem: justificativa, código, casos da amostra, resultado esperado escrito à mão, teste e verificação.

Para checar uma entrega, use `/check-release` antes de reportar que algo está pronto. Para executar a amostra e conferir o resumo, use `/run-sample`.

## Revisão

Para revisar mudanças em `src/`, use o subagente `pipeline-reviewer` (somente leitura). Para conferir se números e afirmações nos documentos têm origem, use `claim-checker`.

## Configuração do Claude Code neste projeto

- `.claude/settings.json`: permissões compartilhadas (testes e leituras liberados; commit, push, `rm` e alterações em `sample/expected/` pedem confirmação).
- `.claude/settings.local.json`: pessoal, fora do Git (`JAVA_HOME`).
- `.claude/rules/`: regras por caminho. Carregam quando o arquivo correspondente é lido ou editado.
- `.claude/skills/`: `run-sample`, `check-release`, `add-quality-rule`.
- `.claude/agents/`: `pipeline-reviewer` e `claim-checker`, ambos somente leitura.
