class PipelineError(Exception):
    pass


class ConfigNotFoundError(PipelineError):
    def __init__(self, path: str):
        super().__init__(f"Arquivo de configuracao nao encontrado: {path}")
        self.path = path


class DataSourceNotFoundError(PipelineError):
    def __init__(self, source_id: str):
        super().__init__(f"Fonte de dados nao encontrada no catalogo: {source_id}")
        self.source_id = source_id


class OutputNotFoundError(PipelineError):
    def __init__(self, output_id: str):
        super().__init__(f"Saida nao encontrada no catalogo: {output_id}")
        self.output_id = output_id
