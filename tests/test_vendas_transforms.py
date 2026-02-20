import pytest
from pyspark.sql import SparkSession

from src.transforms.vendas_transforms import VendasTransforms


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[1]").appName("TestVendasTransforms").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def transforms():
    return VendasTransforms()


@pytest.fixture
def df_pedidos(spark):
    data = [
        ("ped-001", "NOTEBOOK", 1500.0, 2, "2026-01-01", "SP", 1),
        ("ped-002", "CELULAR", 1000.0, 3, "2026-01-02", "RJ", 2),
        ("ped-003", "GELADEIRA", 2000.0, 1, "2026-01-03", "MG", 1),
        ("ped-004", "TV", 2500.0, 1, "2026-01-04", "SP", 3),
        ("ped-005", "NOTEBOOK", 1500.0, 1, "2026-01-05", "RJ", 4),
        ("ped-006", "CELULAR", 1000.0, 5, "2026-01-06", "MG", 5),
        ("ped-007", "LIQUIDIFICADOR", 300.0, 2, "2026-01-07", "SP", 6),
        ("ped-008", "TV", 2500.0, 2, "2026-01-08", "RJ", 7),
        ("ped-009", "GELADEIRA", 2000.0, 3, "2026-01-09", "MG", 8),
        ("ped-010", "NOTEBOOK", 1500.0, 4, "2026-01-10", "SP", 9),
        ("ped-011", "CELULAR", 1000.0, 1, "2026-01-11", "RJ", 10),
        ("ped-012", "LIQUIDIFICADOR", 300.0, 10, "2026-01-12", "MG", 11),
        ("ped-013", "TV", 2500.0, 3, "2026-01-13", "SP", 2),
    ]
    columns = [
        "ID_PEDIDO",
        "PRODUTO",
        "VALOR_UNITARIO",
        "QUANTIDADE",
        "DATA_CRIACAO",
        "UF",
        "ID_CLIENTE",
    ]
    return spark.createDataFrame(data, columns)


@pytest.fixture
def df_clientes(spark):
    data = [
        (1, "Alice Silva", "alice@email.com"),
        (2, "Bob Santos", "bob@email.com"),
        (3, "Carlos Lima", "carlos@email.com"),
        (4, "Diana Costa", "diana@email.com"),
        (5, "Eduardo Reis", "eduardo@email.com"),
        (6, "Fernanda Alves", "fernanda@email.com"),
        (7, "Gabriel Souza", "gabriel@email.com"),
        (8, "Helena Pereira", "helena@email.com"),
        (9, "Igor Martins", "igor@email.com"),
        (10, "Julia Oliveira", "julia@email.com"),
        (11, "Kevin Rocha", "kevin@email.com"),
    ]
    columns = ["id", "nome", "email"]
    return spark.createDataFrame(data, columns)


class TestVendasTransforms:
    def test_calcular_valor_total(self, transforms, df_pedidos):
        result = transforms.calcular_valor_total(df_pedidos)
        assert "VALOR_TOTAL" in result.columns

        row = result.filter(result.ID_PEDIDO == "ped-001").collect()[0]
        assert row["VALOR_TOTAL"] == 3000.0

        row = result.filter(result.ID_PEDIDO == "ped-002").collect()[0]
        assert row["VALOR_TOTAL"] == 3000.0

    def test_agregar_por_cliente(self, transforms, df_pedidos):
        df_com_total = transforms.calcular_valor_total(df_pedidos)
        result = transforms.agregar_por_cliente(df_com_total)

        assert "ID_CLIENTE" in result.columns
        assert "TOTAL_COMPRAS" in result.columns
        assert "QTD_PEDIDOS" in result.columns

        row_cliente_1 = result.filter(result.ID_CLIENTE == 1).collect()[0]
        assert row_cliente_1["TOTAL_COMPRAS"] == 5000.0
        assert row_cliente_1["QTD_PEDIDOS"] == 2

        row_cliente_2 = result.filter(result.ID_CLIENTE == 2).collect()[0]
        assert row_cliente_2["TOTAL_COMPRAS"] == 10500.0
        assert row_cliente_2["QTD_PEDIDOS"] == 2

    def test_rankear_top_n(self, transforms, df_pedidos):
        df_com_total = transforms.calcular_valor_total(df_pedidos)
        df_agregado = transforms.agregar_por_cliente(df_com_total)
        result = transforms.rankear_top_n(df_agregado, n=3)

        assert result.count() == 3

        rows = result.orderBy("RANKING").collect()
        assert rows[0]["RANKING"] == 1
        assert rows[1]["RANKING"] == 2
        assert rows[2]["RANKING"] == 3

        assert rows[0]["TOTAL_COMPRAS"] >= rows[1]["TOTAL_COMPRAS"]
        assert rows[1]["TOTAL_COMPRAS"] >= rows[2]["TOTAL_COMPRAS"]

    def test_rankear_top_10_default(self, transforms, df_pedidos):
        df_com_total = transforms.calcular_valor_total(df_pedidos)
        df_agregado = transforms.agregar_por_cliente(df_com_total)
        result = transforms.rankear_top_n(df_agregado)

        assert result.count() == 10

    def test_enriquecer_com_clientes(self, transforms, df_pedidos, df_clientes):
        df_com_total = transforms.calcular_valor_total(df_pedidos)
        df_agregado = transforms.agregar_por_cliente(df_com_total)
        df_ranking = transforms.rankear_top_n(df_agregado, n=5)
        result = transforms.enriquecer_com_clientes(df_ranking, df_clientes)

        assert "NOME_CLIENTE" in result.columns
        assert "EMAIL_CLIENTE" in result.columns
        assert "RANKING" in result.columns
        assert result.count() == 5

        expected_cols = [
            "RANKING",
            "ID_CLIENTE",
            "NOME_CLIENTE",
            "EMAIL_CLIENTE",
            "TOTAL_COMPRAS",
            "QTD_PEDIDOS",
        ]
        assert result.columns == expected_cols
