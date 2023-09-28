
import logging

from SHARKadm.data.data_holder import DataHolder
from SHARKadm.transformers import Transformer
from SHARKadm.validators import Validator

import pandas as pd

from typing import Protocol

logger = logging.getLogger(__name__)




# class LevelData:
#
#     def __init__(self, level_handler: SharkadmIdLevelHandler) -> None:
#         self._level_handler = level_handler
#         self._data: dict = {}
#
#     @property
#     def level(self) -> str:
#         return self._level_handler.level
#
#     @property
#     def sharkadm_id(self) -> str:
#         return self._level_handler.get_id(self._data)
#
#     def add_data(self, col: str, value: str):
#         if self._data.get(col):
#             msg = f'Column {col} already added to data'
#             logger.error(msg)
#             raise KeyError(msg)
#         self._data[col] = value


class SHARKadmController:
    """Class to hold data from a specific data type. Add data using the add_data_source method"""

    def __init__(self) -> None:

        self._data_holder: DataHolder | None = None

        self._transformers: list[Transformer] = []
        self._validators_before: list[Validator] = []
        self._validators_after: list[Validator] = []

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} with data type "{self.data_type}": {self.dataset_name}'

    @property
    def data_type(self) -> str:
        return self._data_holder.data_type

    @property
    def dataset_name(self) -> str:
        return self._data_holder.dataset_name

    @property
    def data(self) -> pd.DataFrame:
        return self._data_holder.data

    def set_data_holder(self, data_holder: DataHolder) -> None:
        self._data_holder = data_holder

    def set_transformers(self, *args: Transformer) -> None:
        """Add one or more Transformers to the data holder"""
        self._transformers = args

    def transform_all(self) -> None:
        """Runs all transform objects in self._transformers"""
        for trans in self._transformers:
            print(f'{trans=}')
            trans.transform(self._data_holder)

    def transform(self, *transformers: Transformer) -> 'SHARKadmController':
        for trans in transformers:
            trans.transform(self._data_holder)
        return self

    def set_validators_before(self, *args: Validator) -> None:
        """Sets one or more Validators to the data holder"""
        self._validators_before = args

    def validate_before_all(self) -> None:
        """Runs all set validator objects in self._validators_before"""
        for val in self._validators_before:
            val.validate(self._data_holder)

    def validate_before(self, *validators: Validator) -> 'SHARKadmController':
        for val in validators:
            val.validate(self._data_holder)
        return self

    def set_validators_after(self, *args: Validator) -> None:
        """Sets one or more Validators to the data holder"""
        self._validators_after = args

    def validate_after_all(self) -> None:
        """Runs all set validator objects in self._validators_after"""
        for val in self._validators_after:
            val.validate(self._data_holder)

    def validate_after(self, *validators: Validator) -> 'SHARKadmController':
        for val in validators:
            val.validate(self._data_holder)
        return self

    def start_data_handling(self):
        self.validate_before_all()
        self.transform_all()
        self.validate_after_all()

