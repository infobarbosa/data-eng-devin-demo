import sys

from src.core.config import ConfigLoader
from src.data_io.data_io_manager import DataIOManager
from src.jobs.run_top_10 import RunTop10Job
from src.utils.logging_setup import LoggingSetup
from src.utils.spark_manager import SparkManager


def main(config_path: str = None) -> None:
    logger = LoggingSetup.configure()

    logger.info("Carregando configuracao...")
    config = ConfigLoader(config_path=config_path)

    logger.info("Inicializando SparkSession...")
    spark_manager = SparkManager.from_config(config.spark_config)
    spark = spark_manager.get_or_create()

    try:
        data_io = DataIOManager(config=config, spark=spark)
        job = RunTop10Job(data_io=data_io, logger=logger)
        job.execute()
    finally:
        logger.info("Encerrando SparkSession...")
        spark_manager.stop()


if __name__ == "__main__":
    config_file = sys.argv[1] if len(sys.argv) > 1 else None
    main(config_path=config_file)
