from src.core.config import ConfigLoader
from src.data_io.data_io_manager import DataIOManager
from src.jobs.run_top_10 import RunTop10Job
from src.utils.logging_setup import LoggingSetup
from src.utils.spark_manager import SparkManager


def main() -> None:
    LoggingSetup.configure()
    logger = LoggingSetup.get_logger("main")

    logger.info("Loading configuration")
    config = ConfigLoader()

    spark_cfg = config.spark_config
    spark_manager = SparkManager(
        app_name=spark_cfg.get("app_name", "TopClientesPipeline"),
        master=spark_cfg.get("master", "local[*]"),
    )
    spark = spark_manager.get_or_create()

    data_io = DataIOManager(
        spark=spark,
        catalog=config.catalog,
        output=config.output,
    )

    job = RunTop10Job(data_io=data_io)

    try:
        job.execute()
    finally:
        spark_manager.stop()


if __name__ == "__main__":
    main()
