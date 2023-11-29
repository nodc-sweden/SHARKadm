import logging
import pathlib
import yaml

from .data_type_mapper import DataTypeMapper

logger = logging.getLogger(__name__)


class CustomIdLevelHandler:
    """Id handler for a single datatype and specific level"""
    def __init__(self, data_type: str, level: str, config: dict):
        self._data_type = data_type
        self._level: str = level
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
    def id_columns(self) -> list[str]:
        return self._id_columns

    def _load_id_columns(self) -> None:
        self._id_columns = self._config['levels'][self.level]

    def get_id(self, data: dict) -> str:
        """Returns the id based on the given data"""
        parts = [self.data_type]
        for col in self.id_columns:
            parts.append(data[col].replace('/', '_'))
        return '_'.join(parts)


class CustomIdConfig:
    """Config id handler for a single datatype (single config file)"""
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

    def get_id_objects(self) -> dict[str, CustomIdLevelHandler]:
        objs = {}
        for level in self.levels:
            obj = CustomIdLevelHandler(self.data_type, level, self._config)
            objs[level] = obj
        return objs


class CustomIdsHandler:
    """Handler for ids for all data types"""
    def __init__(self, directory: str | pathlib.Path):
        self._directory: pathlib.Path = pathlib.Path(directory)
        self._id_objects: dict[str, dict[str, CustomIdLevelHandler]] = {}
        self._load_sharkadm_id_objects()

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} for directory: {self._directory}'

    def _load_sharkadm_id_objects(self) -> None:
        self._id_objects = {}
        for path in self._directory.iterdir():
            config = CustomIdConfig(path)
            self._id_objects[config.data_type] = config.get_id_objects()
            
    def get_level_handler(self,
                          data_type: str = None,
                          level: str = None) -> CustomIdLevelHandler | None:
                          # data_type_mapper: DataTypeMapper = None) -> SharkadmIdLevelHandler | None:
        # data_type = data_type_mapper.get(data_type)
        data_type = data_type.lower()
        if level not in self._id_objects[data_type]:
            return
        return self._id_objects[data_type][level]

    def get_levels_for_datatype(self, data_type: str) -> list[str]:
        return list(self._id_objects[data_type.lower()])





