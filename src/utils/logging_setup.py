import logging
import sys


class LoggingSetup:
    _configured = False

    @classmethod
    def configure(cls, level: int = logging.INFO) -> None:
        if cls._configured:
            return
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        root = logging.getLogger()
        root.setLevel(level)
        root.addHandler(handler)
        cls._configured = True

    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        cls.configure()
        return logging.getLogger(name)
