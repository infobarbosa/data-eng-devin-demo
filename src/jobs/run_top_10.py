import logging

from src.data_io.data_io_manager import DataIOManager
from src.transforms.vendas_transforms import VendasTransforms


class RunTop10Job:
    def __init__(self, data_io: DataIOManager, logger: logging.Logger = None):
        self._data_io = data_io
        self._logger = logger or logging.getLogger("top10_pipeline")
        self._transforms = VendasTransforms()

    def execute(self) -> None:
        self._logger.info("Lendo dados de pedidos...")
        df_pedidos = self._data_io.read("pedidos")
        self._logger.info(f"Pedidos carregados: {df_pedidos.count()} registros")

        self._logger.info("Lendo dados de clientes...")
        df_clientes = self._data_io.read("clientes")
        self._logger.info(f"Clientes carregados: {df_clientes.count()} registros")

        self._logger.info("Calculando valor total por pedido...")
        df_com_total = self._transforms.calcular_valor_total(df_pedidos)

        self._logger.info("Agregando por cliente...")
        df_agregado = self._transforms.agregar_por_cliente(df_com_total)

        self._logger.info("Rankeando Top 10 clientes...")
        df_ranking = self._transforms.rankear_top_n(df_agregado, n=10)

        self._logger.info("Enriquecendo com dados de clientes...")
        df_final = self._transforms.enriquecer_com_clientes(df_ranking, df_clientes)

        self._logger.info("Salvando resultado...")
        self._data_io.write(df_final, "top_10_clientes")

        self._logger.info("Top 10 Clientes:")
        df_final.show(truncate=False)

        self._logger.info("Pipeline concluido com sucesso!")
