import os
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession

from src.core.config import ConfigLoader


class DataIOManager:
    def __init__(self, config: ConfigLoader, spark: SparkSession, base_path: str = None):
        self._config = config
        self._spark = spark
        if base_path is None:
            base_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "..",
                "..",
            )
        self._base_path = os.path.normpath(base_path)

    def _resolve_path(self, relative_path: str) -> str:
        return os.path.join(self._base_path, relative_path)

    def read(self, source_id: str) -> DataFrame:
        source = self._config.get_source(source_id)
        formato = source["formato"]
        caminho = self._resolve_path(source["caminho"])
        opcoes: Dict[str, Any] = source.get("opcoes", {})

        reader = self._spark.read.format(formato)
        for key, value in opcoes.items():
            reader = reader.option(key, value)

        return reader.load(caminho)

    def write(self, df: DataFrame, output_id: str, mode: str = "overwrite") -> None:
        output = self._config.get_output(output_id)
        formato = output["formato"]
        caminho = self._resolve_path(output["caminho"])

        df.write.format(formato).mode(mode).save(caminho)
