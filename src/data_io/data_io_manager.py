from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.core.exceptions import DataSourceNotFoundError
from src.utils.logging_setup import LoggingSetup


class DataIOManager:
    def __init__(
        self,
        spark: SparkSession,
        catalog: dict,
        output: dict,
        base_path: str | None = None,
    ) -> None:
        self._logger = LoggingSetup.get_logger(self.__class__.__name__)
        self._spark = spark
        self._catalog = catalog
        self._output = output
        self._base = Path(base_path) if base_path else Path.cwd()

    def _resolve(self, relative: str) -> str:
        p = Path(relative)
        if p.is_absolute():
            return str(p)
        return str(self._base / p)

    def read(self, source_id: str) -> DataFrame:
        if source_id not in self._catalog:
            raise DataSourceNotFoundError(f"Source '{source_id}' not found in catalog")
        entry = self._catalog[source_id]
        fmt = entry["format"]
        path = self._resolve(entry["path"])
        options = entry.get("options", {})
        self._logger.info("Reading %s [%s] from %s", source_id, fmt, path)
        reader = self._spark.read.format(fmt)
        for k, v in options.items():
            reader = reader.option(k, v)
        return reader.load(path)

    def write(self, df: DataFrame, output_id: str) -> None:
        if output_id not in self._output:
            raise DataSourceNotFoundError(f"Output '{output_id}' not found in config")
        entry = self._output[output_id]
        fmt = entry["format"]
        path = self._resolve(entry["path"])
        mode = entry.get("mode", "overwrite")
        self._logger.info("Writing %s [%s] to %s", output_id, fmt, path)
        df.write.format(fmt).mode(mode).save(path)
