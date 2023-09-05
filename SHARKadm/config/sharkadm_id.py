import logging
import pathlib
import yaml

from . import utils

logger = logging.getLogger(__name__)


class SharkadmIdLevelHandler:

    def __init__(self, data_type: str, level: str, config: dict):
        self._data_type = data_type
        self._level: str = level
        self._all_levels: list = []
        self._config: dict = config
        self._id_columns: list[str] = []
        self._load_id_columns()

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} with data type "{self._data_type}" and level "{self._level}"'

    @property
    def data_type(self) -> str:
        return self._data_type

    @property
    def level(self) -> str:
        return self._level

    @property
    def included_levels(self) -> list[str]:
        return self._all_levels

    @property
    def id_columns(self) -> list[str]:
        return self._id_columns

    def _load_id_columns(self) -> None:
        self._id_columns = []
        self._all_levels = []
        for current_level, keys in self._config['levels'].items():
            if not keys:
                continue
            self._id_columns.extend(keys)
            self._all_levels.append(current_level)
            if current_level == self.level:
                break

    def get_id(self, data: dict) -> str:
        """Returns the id based on the given data"""
        parts = [self.data_type]
        for col in self.id_columns:
            parts.append(data[col])
        return '+'.join(parts)


class SharkadmIdConfig:

    def __init__(self, config_path: pathlib.Path):
        self._config_path = config_path
        self._config = {}
        self._load_config()

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} handling config file: "{self._config_path}"'

    def _load_config(self):
        with open(self._config_path) as fid:
            self._config = yaml.safe_load(fid)

    @property
    def data_type(self) -> str:
        return self._config['data_type']

    @property
    def levels(self) -> dict:
        return self._config['levels']

    def get_id_objects(self) -> dict[str, SharkadmIdLevelHandler]:
        objs = {}
        for level in self.levels:
            obj = SharkadmIdLevelHandler(self.data_type, level, self._config)
            objs[level] = obj
        return objs


class SharkadmIdHandler:
    def __init__(self, directory: str | pathlib.Path):
        self._directory: pathlib.Path = pathlib.Path(directory)
        self._id_objects: dict[str, dict[str, SharkadmIdHandler]] = {}
        self._load_sharkadm_id_objects()

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} for directory: {self._directory}'

    def _load_sharkadm_id_objects(self) -> None:
        self._id_objects = {}
        for path in self._directory.iterdir():
            config = SharkadmIdConfig(path)
            self._id_objects[config.data_type] = config.get_id_objects()
            
    def get_level_handler(self,
                          data_type: str = None,
                          level: str = None) -> SharkadmIdLevelHandler | None:
        data_type = utils.get_data_type_mapping(data_type)
        if level not in self._id_objects[data_type]:
            return
        return self._id_objects[data_type][level]




