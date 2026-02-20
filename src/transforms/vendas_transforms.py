from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class VendasTransforms:
    @staticmethod
    def calcular_valor_total(df_pedidos: DataFrame) -> DataFrame:
        return df_pedidos.withColumn(
            "VALOR_TOTAL",
            F.col("VALOR_UNITARIO") * F.col("QUANTIDADE"),
        )

    @staticmethod
    def agregar_por_cliente(df_pedidos_com_total: DataFrame) -> DataFrame:
        return df_pedidos_com_total.groupBy("ID_CLIENTE").agg(
            F.sum("VALOR_TOTAL").alias("TOTAL_COMPRAS"),
            F.count("ID_PEDIDO").alias("QTD_PEDIDOS"),
        )

    @staticmethod
    def rankear_top_n(df_agregado: DataFrame, n: int = 10) -> DataFrame:
        window = Window.orderBy(F.col("TOTAL_COMPRAS").desc())
        return (
            df_agregado.withColumn("RANKING", F.row_number().over(window))
            .filter(F.col("RANKING") <= n)
            .orderBy("RANKING")
        )

    @staticmethod
    def enriquecer_com_clientes(df_ranking: DataFrame, df_clientes: DataFrame) -> DataFrame:
        return df_ranking.join(
            df_clientes.select(
                F.col("id").alias("ID_CLIENTE"),
                F.col("nome").alias("NOME_CLIENTE"),
                F.col("email").alias("EMAIL_CLIENTE"),
            ),
            on="ID_CLIENTE",
            how="left",
        ).select(
            "RANKING",
            "ID_CLIENTE",
            "NOME_CLIENTE",
            "EMAIL_CLIENTE",
            "TOTAL_COMPRAS",
            "QTD_PEDIDOS",
        )
