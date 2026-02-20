from typing import Dict

from pyspark.sql import SparkSession


class SparkManager:
    def __init__(self, app_name: str, master: str):
        self._app_name = app_name
        self._master = master
        self._spark: SparkSession = None

    @classmethod
    def from_config(cls, spark_config: Dict[str, str]) -> "SparkManager":
        return cls(
            app_name=spark_config.get("app_name", "SparkApp"),
            master=spark_config.get("master", "local[*]"),
        )

    def get_or_create(self) -> SparkSession:
        if self._spark is None:
            self._spark = SparkSession.builder.appName(self._app_name).master(self._master).getOrCreate()
        return self._spark

    def stop(self) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
