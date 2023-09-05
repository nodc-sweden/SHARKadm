import pathlib
import logging
import pandas as pd

from SHARKadm.data.data_source.common import DataFile
from SHARKadm.config.sharkadm_id import SharkadmIdLevelHandler

from SHARKadm.config import sharkadm_id
from SHARKadm.config import column_info

logger = logging.getLogger(__name__)


class LevelData:

    def __init__(self, level_handler: SharkadmIdLevelHandler) -> None:
        self._level_handler = level_handler
        self._data: dict = {}

    @property
    def level(self) -> str:
        return self._level_handler.level

    @property
    def sharkadm_id(self) -> str:
        return self._level_handler.get_id(self._data)

    def add_data(self, col: str, value: str):
        if self._data.get(col):
            msg = f'Column {col} already added to data'
            logger.error(msg)
            raise KeyError(msg)
        self._data[col] = value


class DataHolder:
    """Class to hold data from a specific data type. Add data using the add_data_source method"""

    def __init__(self,
                 data_type: str = None,
                 column_info: column_info.ColumnInfoConfig = None,
                 id_handler: sharkadm_id.SharkadmIdHandler = None,
                 dataset_name: str = None
                 ) -> None:
        self._data_type = data_type
        self._column_info = column_info
        self._id_handler = id_handler
        self._dataset_name = dataset_name

        self._data: pd.DataFrame = pd.DataFrame()
        # self._data_sources: dict[str, DataSource] = {}
        self._levels: dict[str, dict[str, LevelData]] = {}

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} with data type "{self._data_type}": {self._dataset_name}'

    def add_data_source(self, data_source: DataFile) -> None:
        if data_source.data_type != self._data_type:
            msg = f'Data source {data_source} is not of type {self._data_type}'
            logger.error(msg)
            raise ValueError(msg)
        # self._data_sources[data_source.source] = data_source
        self._import_data_source(data_source)

    def _import_data_source(self, data_source: DataFile) -> None:
        """Add new data source to self._data"""
        new_data = data_source.get_data().copy(deep=True)
        new_data['data_source'] = data_source.source
        new_data['dataset_name'] = self._dataset_name
        self._add_sharkadm_ids_to_new_data(new_data)
        # TODO: Option to join instead
        self._data = pd.concat([self._data, new_data])
        self._data.reset_index(inplace=True)

    def _add_sharkadm_ids_to_new_data(self, new_data: pd.DataFrame) -> None:
        """sharkadm_id in taken from self._id_handler"""
        for level in self._column_info.all_levels:
            id_handler = self._id_handler.get_level_handler(data_type=self._data_type, level=level)
            if not id_handler:
                # new_data[f'sharkadm_{level}_id'] = ''
                continue
            new_data[f'sharkadm_{level}_id'] = new_data.apply(lambda row: id_handler.get_id(row), axis=1)



