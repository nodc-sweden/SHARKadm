
import logging

import pandas as pd

from SHARKadm.config.sharkadm_id import SharkadmIdLevelHandler
from SHARKadm.data.data_source.common import DataFile
from SHARKadm.transformers import Transformer
from SHARKadm.validators import Validator

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
                 dataset_name: str = None
                 ) -> None:
        self._data_type = data_type
        self._dataset_name = dataset_name

        self._data: pd.DataFrame = pd.DataFrame()

        self._transformers: list[Transformer] = []
        self._validators: list[Validator] = []

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} with data type "{self._data_type}": {self._dataset_name}'

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @property
    def data_type(self) -> str:
        return self._data_type

    def add_data_source(self, data_source: DataFile) -> None:
        if data_source.data_type != self._data_type:
            msg = f'Data source {data_source} is not of type {self._data_type}'
            logger.error(msg)
            raise ValueError(msg)
        self._import_data_source(data_source)

    def set_transformers(self, *args: Transformer) -> None:
        """Add one or more Transformers to the data holder"""
        self._transformers.extend(args)

    def transform_all(self) -> None:
        """Runs all transform objects in self._transformers"""
        for trans in self._transformers:
            trans.transform(self)

    def transform(self, *transformers: Transformer) -> 'DataHolder':
        for trans in transformers:
            trans.transform(self)
        return self

    def set_validators(self, *args: Validator) -> None:
        """Sets one or more Validators to the data holder"""
        self._validators.extend(args)

    def validate_all(self) -> None:
        """Runs all set validator objects in self._validators"""
        for val in self._validators:
            val.validate(self)

    def validate(self, *validators: Validator) -> 'DataHolder':
        for val in validators:
            val.validate(self)
        return self

    def _import_data_source(self, data_source: DataFile) -> None:
        """Add new data source to self._data"""
        new_data = data_source.get_data().copy(deep=True)
        new_data['data_source'] = data_source.source
        new_data['dataset_name'] = self._dataset_name
        # TODO: Option to join instead
        self._data = pd.concat([self._data, new_data])
        self._data.fillna('', inplace=True)
        self._data.reset_index(inplace=True)

