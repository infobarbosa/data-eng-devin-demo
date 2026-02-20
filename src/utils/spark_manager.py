from pyspark.sql import SparkSession

from src.utils.logging_setup import LoggingSetup


class SparkManager:
    def __init__(self, app_name: str, master: str) -> None:
        self._logger = LoggingSetup.get_logger(self.__class__.__name__)
        self._app_name = app_name
        self._master = master
        self._spark: SparkSession | None = None

    def get_or_create(self) -> SparkSession:
        if self._spark is None:
            self._logger.info(
                "Creating SparkSession: app=%s master=%s",
                self._app_name,
                self._master,
            )
            self._spark = (
                SparkSession.builder.appName(self._app_name)
                .master(self._master)
                .getOrCreate()
            )
        return self._spark

    def stop(self) -> None:
        if self._spark is not None:
            self._logger.info("Stopping SparkSession")
            self._spark.stop()
            self._spark = None
