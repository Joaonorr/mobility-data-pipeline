"""Erros esperados do pipeline.

Toda falha prevista é uma subclasse de PipelineError, com mensagem pensada para quem opera a execução.
Quando uma dessas exceções é lançada, nenhum resultado novo é publicado.
"""


class PipelineError(Exception):
    """Falha esperada: a execução para e a publicação anterior permanece válida."""


class InputNotFoundError(PipelineError):
    """O caminho de entrada não existe ou não contém arquivos Parquet."""


class InputCorruptedError(PipelineError):
    """O arquivo existe, mas não pode ser lido como Parquet (ex.: truncado)."""


class IncompatibleInputError(PipelineError):
    """O Parquet é legível, mas não tem as colunas ou os tipos esperados."""


class EmptyInputError(PipelineError):
    """A entrada não contém nenhum registro; publicar um resultado vazio seria enganoso."""


class ZoneReferenceError(PipelineError):
    """A tabela de zonas de referência está ausente, mal formada ou inconsistente."""


class ExecutionError(PipelineError):
    """Falha durante o processamento Spark (ex.: página de dados corrompida)."""


class ReconciliationError(PipelineError):
    """As contagens das saídas não batem com a entrada; a publicação é cancelada."""
