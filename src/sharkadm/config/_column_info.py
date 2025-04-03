import functools
import pathlib

import yaml


class LevelColumnInfoConfig:
    """Holds the configuration for a single node_level (sample, variable etc.)"""

    def __init__(self, level: str) -> None:
        self._level = level
        self._data = {}

    @property
    def level(self) -> str:
        return self._level

    @property
    def parameters(self) -> list[str]:
        return sorted(self._data)

    def add_parameter(self, parameter: str, info: dict) -> None:
        if info.get("node_level") != self.level:
            return
        self._data[parameter] = info


class ColumnInfoConfig:
    def __init__(self, path: str | pathlib.Path, encoding="cp1252") -> None:
        self._path = pathlib.Path(path)
        self._encoding = encoding
        self._data = {}
        self._levels = {}

        self._load_file()
        self._create_levels()

    def _load_file(self) -> None:
        with open(self._path, encoding=self._encoding) as fid:
            self._data = yaml.safe_load(fid)

    def _create_levels(self) -> None:
        for par, info in self._data.items():
            node_level = info["node_level"]
            level = self._levels.setdefault(node_level, LevelColumnInfoConfig(node_level))
            level.add_parameter(parameter=par, info=info)

    @property
    def all_parameters(self) -> list:
        """Returns a list of all parameters"""
        return list(self._data)

    @property
    def all_levels(self) -> list:
        """Returns a list of all levels"""
        levels = set()
        for info in self._data.values():
            levels.add(info["node_level"])
        return sorted([lev for lev in levels if lev])

    # @functools.cache
    # def get_level(self, parameter: str) -> str:
    #     """Returns the level for the given parameter"""
    #     return self._data[parameter]['node_level']
    #
    # def get_format(self, parameter: str) -> str:
    #     """Returns the format for the given parameter"""
    #     return self._data[parameter]['format']
    #
    # def get_level_config(self, node_level: str) -> LevelColumnInfoConfig:
    #     return self._levels.get(node_level)
