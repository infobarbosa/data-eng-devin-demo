from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class VendasTransforms:
    @staticmethod
    def calcular_valor_total(pedidos: DataFrame) -> DataFrame:
        return pedidos.withColumn(
            "valor_total",
            F.col("VALOR_UNITARIO") * F.col("QUANTIDADE"),
        )

    @staticmethod
    def agregar_por_cliente(pedidos_com_total: DataFrame) -> DataFrame:
        return pedidos_com_total.groupBy("ID_CLIENTE").agg(
            F.sum("valor_total").alias("total_compras"),
            F.count("ID_PEDIDO").alias("qtd_pedidos"),
        )

    @staticmethod
    def ranking_top_n(agregado: DataFrame, n: int = 10) -> DataFrame:
        window = Window.orderBy(F.col("total_compras").desc())
        return (
            agregado.withColumn("ranking", F.row_number().over(window))
            .filter(F.col("ranking") <= n)
            .orderBy("ranking")
        )

    @staticmethod
    def enriquecer_com_clientes(ranking: DataFrame, clientes: DataFrame) -> DataFrame:
        return ranking.join(
            clientes.select(
                F.col("id").alias("ID_CLIENTE"),
                F.col("nome"),
                F.col("email"),
            ),
            on="ID_CLIENTE",
            how="left",
        ).orderBy("ranking")
