from pathlib import Path

import yaml

from src.core.exceptions import ConfigNotFoundError


class ConfigLoader:
    def __init__(self, config_path: str | None = None) -> None:
        if config_path is None:
            config_path = str(
                Path(__file__).resolve().parent.parent.parent / "config" / "config.yaml"
            )
        self._path = Path(config_path)
        if not self._path.exists():
            raise ConfigNotFoundError(f"Config file not found: {self._path}")
        with open(self._path, "r") as fh:
            self._data: dict = yaml.safe_load(fh)

    @property
    def spark_config(self) -> dict:
        return self._data.get("spark", {})

    @property
    def catalog(self) -> dict:
        return self._data.get("catalog", {})

    @property
    def output(self) -> dict:
        return self._data.get("output", {})

    def get(self, key: str, default=None):
        return self._data.get(key, default)
