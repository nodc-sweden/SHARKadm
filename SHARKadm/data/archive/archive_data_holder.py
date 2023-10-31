import logging
import pathlib
from abc import ABC, abstractmethod

import pandas as pd

from SHARKadm import config
# from SHARKadm.config import get_data_type_mapper
from SHARKadm.config.import_config import ImportMatrixConfig
from SHARKadm.config.import_config import ImportMatrixMapper
from SHARKadm.data import data_source
from SHARKadm.data.archive import delivery_note
from SHARKadm.data.data_holder import DataHolder

logger = logging.getLogger(__name__)


class ArchiveDataHolder(DataHolder, ABC):
    _data_type: str | None = None
    _data_format: str | None = None

    _date_str_format = '%Y-%m-%d'

    def __init__(self, archive_root_directory: str | pathlib.Path = None):
        self._archive_root_directory = pathlib.Path(archive_root_directory)

        self._data: pd.DataFrame = pd.DataFrame()
        self._dataset_name: str | None = None

        self._delivery_note: delivery_note.DeliveryNote | None = None
        self._import_matrix: ImportMatrixConfig | None = None
        self._import_matrix_mapper: ImportMatrixMapper | None = None
        # self._data_type_mapper = get_data_type_mapper()

        self._data_sources = {}

        self._initiate()
        self._load_delivery_note()
        self._load_import_matrix()
        self._load_data()

    def _initiate(self) -> None:
        self._dataset_name = self.archive_root_directory.name

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @property
    def data_type(self) -> str:
        # return self._data_type_mapper.get(self.data_format)
        return self._data_type

    # @property
    # def data_format(self) -> str:
    #     return self._data_format

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @property
    def archive_root_directory(self) -> pathlib.Path:
        return self._archive_root_directory

    @property
    def received_data_directory(self) -> pathlib.Path:
        return self.archive_root_directory / 'received_data'

    @property
    def processed_data_directory(self) -> pathlib.Path:
        return self.archive_root_directory / 'processed_data'

    @property
    def delivery_note_path(self) -> pathlib.Path:
        return self.processed_data_directory / 'delivery_note.txt'

    @property
    def delivery_note(self) -> delivery_note.DeliveryNote:
        return self._delivery_note

    @property
    def import_matrix_mapper(self) -> ImportMatrixMapper:
        return self._import_matrix_mapper

    @property
    def min_year(self) -> str:
        return str(min(self.data['datetime']).year)

    @property
    def max_year(self) -> str:
        return str(max(self.data['datetime']).year)

    @property
    def min_date(self) -> str:
        return min(self.data['datetime']).strftime(self._date_str_format)

    @property
    def max_date(self) -> str:
        return max(self.data['datetime']).strftime(self._date_str_format)

    @property
    def min_longitude(self) -> str:
        return str(min(self.data['sample_reported_longitude'].astype(float)))

    @property
    def max_longitude(self) -> str:
        return str(max(self.data['sample_reported_longitude'].astype(float)))

    @property
    def min_latitude(self) -> str:
        return str(min(self.data['sample_reported_latitude'].astype(float)))

    @property
    def max_latitude(self) -> str:
        return str(max(self.data['sample_reported_latitude'].astype(float)))

    def _load_delivery_note(self) -> None:
        self._delivery_note = delivery_note.DeliveryNote(self.delivery_note_path)

    def _load_import_matrix(self) -> None:
        """Loads the import matrix for the given data type and provider found in delivery note"""
        self._import_matrix = config.get_import_matrix_config(data_type=self.delivery_note.data_type)
        self._import_matrix_mapper = self._import_matrix.get_mapper(self.delivery_note.import_matrix_key)

    def _add_concatenated_column(self, new_column: str, columns_to_use: list[str]) -> None:
        """Adds a concatenated column specified in new_column using the columns listed in columns_to_use"""
        for col in columns_to_use:
            if new_column not in self._data.columns:
                self._data[new_column] = self._data[col]
            else:
                self._data[new_column] = self._data[new_column] + self._data[col]

    def _check_data_source(self, data_source: data_source.DataFile) -> None:
        #if self._data_type_mapper.get(data_source.data_type) != self._data_type_mapper.get(self.data_type):
        if data_source.data_type.lower() != self.data_type.lower():
            msg = f'Data source {data_source} is not of type {self.data_type}'
            logger.error(msg)
            raise ValueError(msg)

    @staticmethod
    def _get_data_from_data_source(data_source: data_source.DataFile) -> pd.DataFrame:
        data = data_source.get_data()
        data = data.fillna('')
        data.reset_index(inplace=True, drop=True)
        return data

    # def _concat_data_source(self, data_source: data_source.DataFile) -> None:
    #     self._check_data_source(data_source)
    #     self._data_sources[str(data_source)] = data_source
    #     """Concats new data source to self._data"""
    #     new_data = self._get_data_from_data_source(data_source)
    #     new_data['data_source'] = data_source.source
    #     new_data['dataset_name'] = self._dataset_name
    #     self._data = pd.concat([self._data, new_data])
    #     self._data.fillna('', inplace=True)
    #     self._data.reset_index(inplace=True, drop=True)

    def _set_data_source(self, data_source: data_source.DataFile) -> None:
        """Sets a single data source to self._data"""
        self._add_data_source(data_source)
        self._data = self._get_data_from_data_source(data_source)

    def _add_data_source(self, data_source: data_source.DataFile) -> None:
        """Adds a data source to instance variable self._data_sources. This method is not adding to data itself."""
        self._check_data_source(data_source)
        self._data_sources[str(data_source)] = data_source

    @abstractmethod
    def _load_data(self):
        ...

