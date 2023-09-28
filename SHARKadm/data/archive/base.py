import logging
import pathlib
from abc import ABC, abstractmethod

import pandas as pd

from SHARKadm import config
from SHARKadm.config.import_config import ImportMatrixConfig
from SHARKadm.config.import_config import ImportMatrixMapper
from SHARKadm.data import data_source
from SHARKadm.data.archive import delivery_note
from SHARKadm.data.data_holder import DataHolder

logger = logging.getLogger(__name__)


class ArchiveBase(DataHolder, ABC):
    _data_type: str | None = None

    def __init__(self, archive_root_directory: str | pathlib.Path = None):
        self._archive_root_directory = pathlib.Path(archive_root_directory)

        self._data: pd.DataFrame = pd.DataFrame()
        self._dataset_name: str | None = None

        self._delivery_note: delivery_note.DeliveryNote | None = None
        self._import_matrix: ImportMatrixConfig | None = None
        self._import_matrix_mapper: ImportMatrixMapper | None = None

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
        return self._data_type.lower()

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @property
    def archive_root_directory(self) -> pathlib.Path:
        return self._archive_root_directory

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

    def _load_delivery_note(self) -> None:
        self._delivery_note = delivery_note.DeliveryNote(self.delivery_note_path)

    def _load_import_matrix(self) -> None:
        """Loads the import matrix for the given data type and provider found in delivery note"""
        self._import_matrix = config.get_import_matrix_config(data_type=self.delivery_note.data_type)
        self._import_matrix_mapper = self._import_matrix.get_mapper(self.delivery_note.import_matrix_key)

    def _check_data_source(self, data_source: data_source.DataFile) -> None:
        if data_source.data_type != self.data_type:
            msg = f'Data source {data_source} is not of type {self.data_type}'
            logger.error(msg)
            raise ValueError(msg)

    def _concat_data_source(self, data_source: data_source.DataFile) -> None:
        self._check_data_source(data_source)
        """Concats new data source to self._data"""
        new_data = data_source.get_data().copy(deep=True)
        new_data['data_source'] = data_source.source
        new_data['dataset_name'] = self._dataset_name
        self._data = pd.concat([self._data, new_data])
        self._data.fillna('', inplace=True)
        self._data.reset_index(inplace=True)

    @abstractmethod
    def _load_data(self):
        ...

