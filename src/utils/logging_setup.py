import logging
import sys


class LoggingSetup:
    @staticmethod
    def configure(level: int = logging.INFO) -> logging.Logger:
        logger = logging.getLogger("top10_pipeline")
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        logger.setLevel(level)
        return logger
