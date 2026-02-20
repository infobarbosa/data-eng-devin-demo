import os
from typing import Any, Dict

import yaml

from src.core.exceptions import ConfigNotFoundError


class ConfigLoader:
    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "..",
                "..",
                "config",
                "config.yaml",
            )
        self._config_path = os.path.normpath(config_path)
        self._config: Dict[str, Any] = self._load()

    def _load(self) -> Dict[str, Any]:
        if not os.path.isfile(self._config_path):
            raise ConfigNotFoundError(self._config_path)
        with open(self._config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    @property
    def spark_config(self) -> Dict[str, str]:
        return self._config.get("spark", {})

    @property
    def catalogo(self) -> Dict[str, Any]:
        return self._config.get("catalogo", {})

    @property
    def saida(self) -> Dict[str, Any]:
        return self._config.get("saida", {})

    def get_source(self, source_id: str) -> Dict[str, Any]:
        from src.core.exceptions import DataSourceNotFoundError

        source = self.catalogo.get(source_id)
        if source is None:
            raise DataSourceNotFoundError(source_id)
        return source

    def get_output(self, output_id: str) -> Dict[str, Any]:
        from src.core.exceptions import OutputNotFoundError

        output = self.saida.get(output_id)
        if output is None:
            raise OutputNotFoundError(output_id)
        return output
