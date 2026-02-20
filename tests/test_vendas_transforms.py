import pytest
from pyspark.sql import SparkSession

from src.transforms.vendas_transforms import VendasTransforms


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    yield session
    session.stop()


@pytest.fixture()
def pedidos(spark):
    data = [
        ("p1", "NOTEBOOK", 1500.0, 2, "2026-01-01", "SP", 1),
        ("p2", "CELULAR", 1000.0, 3, "2026-01-02", "RJ", 2),
        ("p3", "GELADEIRA", 2000.0, 1, "2026-01-03", "MG", 1),
        ("p4", "TV", 3000.0, 1, "2026-01-04", "SP", 3),
        ("p5", "NOTEBOOK", 1500.0, 1, "2026-01-05", "BA", 4),
        ("p6", "CELULAR", 1000.0, 2, "2026-01-06", "SP", 5),
        ("p7", "LIQUIDIFICADOR", 300.0, 5, "2026-01-07", "RJ", 6),
        ("p8", "TV", 3000.0, 2, "2026-01-08", "MG", 7),
        ("p9", "GELADEIRA", 2000.0, 3, "2026-01-09", "SP", 8),
        ("p10", "NOTEBOOK", 1500.0, 4, "2026-01-10", "BA", 9),
        ("p11", "CELULAR", 1000.0, 1, "2026-01-11", "RJ", 10),
        ("p12", "TV", 3000.0, 3, "2026-01-12", "SP", 11),
    ]
    schema = [
        "ID_PEDIDO",
        "PRODUTO",
        "VALOR_UNITARIO",
        "QUANTIDADE",
        "DATA_CRIACAO",
        "UF",
        "ID_CLIENTE",
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture()
def clientes(spark):
    data = [
        (1, "Alice Silva", "alice@email.com"),
        (2, "Bruno Costa", "bruno@email.com"),
        (3, "Carla Souza", "carla@email.com"),
        (4, "Diego Lima", "diego@email.com"),
        (5, "Elena Ramos", "elena@email.com"),
        (6, "Felipe Dias", "felipe@email.com"),
        (7, "Gabi Rocha", "gabi@email.com"),
        (8, "Hugo Melo", "hugo@email.com"),
        (9, "Iris Nunes", "iris@email.com"),
        (10, "Julio Gomes", "julio@email.com"),
        (11, "Karen Alves", "karen@email.com"),
    ]
    return spark.createDataFrame(data, ["id", "nome", "email"])


class TestVendasTransforms:
    def test_calcular_valor_total(self, pedidos):
        result = VendasTransforms.calcular_valor_total(pedidos)
        assert "valor_total" in result.columns
        row = result.filter("ID_PEDIDO = 'p1'").collect()[0]
        assert row["valor_total"] == 3000.0

    def test_agregar_por_cliente(self, pedidos):
        com_total = VendasTransforms.calcular_valor_total(pedidos)
        agregado = VendasTransforms.agregar_por_cliente(com_total)
        assert "total_compras" in agregado.columns
        assert "qtd_pedidos" in agregado.columns
        cliente_1 = agregado.filter("ID_CLIENTE = 1").collect()[0]
        assert cliente_1["total_compras"] == 5000.0
        assert cliente_1["qtd_pedidos"] == 2

    def test_ranking_top_n(self, pedidos):
        com_total = VendasTransforms.calcular_valor_total(pedidos)
        agregado = VendasTransforms.agregar_por_cliente(com_total)
        ranking = VendasTransforms.ranking_top_n(agregado, n=3)
        rows = ranking.collect()
        assert len(rows) == 3
        assert rows[0]["ranking"] == 1
        assert rows[0]["total_compras"] >= rows[1]["total_compras"]

    def test_ranking_top_10_returns_all_when_less(self, pedidos):
        com_total = VendasTransforms.calcular_valor_total(pedidos)
        agregado = VendasTransforms.agregar_por_cliente(com_total)
        ranking = VendasTransforms.ranking_top_n(agregado, n=10)
        rows = ranking.collect()
        assert len(rows) == 10

    def test_enriquecer_com_clientes(self, pedidos, clientes):
        com_total = VendasTransforms.calcular_valor_total(pedidos)
        agregado = VendasTransforms.agregar_por_cliente(com_total)
        ranking = VendasTransforms.ranking_top_n(agregado, n=3)
        resultado = VendasTransforms.enriquecer_com_clientes(ranking, clientes)
        assert "nome" in resultado.columns
        assert "email" in resultado.columns
        rows = resultado.collect()
        assert len(rows) == 3
        assert all(r["nome"] is not None for r in rows)
