from src.data_io.data_io_manager import DataIOManager
from src.transforms.vendas_transforms import VendasTransforms
from src.utils.logging_setup import LoggingSetup


class RunTop10Job:
    def __init__(self, data_io: DataIOManager) -> None:
        self._logger = LoggingSetup.get_logger(self.__class__.__name__)
        self._data_io = data_io
        self._transforms = VendasTransforms()

    def execute(self) -> None:
        self._logger.info("Starting Top 10 Clientes pipeline")

        pedidos = self._data_io.read("pedidos_bronze")
        self._logger.info("Pedidos loaded: %d rows", pedidos.count())

        clientes = self._data_io.read("clientes_bronze")
        self._logger.info("Clientes loaded: %d rows", clientes.count())

        pedidos_total = self._transforms.calcular_valor_total(pedidos)
        agregado = self._transforms.agregar_por_cliente(pedidos_total)
        ranking = self._transforms.ranking_top_n(agregado, n=10)
        resultado = self._transforms.enriquecer_com_clientes(ranking, clientes)

        resultado.show(truncate=False)

        self._data_io.write(resultado, "top_10_clientes")
        self._logger.info("Pipeline finished successfully")
