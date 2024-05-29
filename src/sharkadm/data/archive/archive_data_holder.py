import logging
import pathlib
from abc import ABC, abstractmethod

import pandas as pd

from sharkadm import config
# from sharkadm.config import get_data_type_mapper
from sharkadm.config.import_matrix import ImportMatrixConfig
from sharkadm.config.import_matrix import ImportMatrixMapper
from sharkadm.data import data_source
from sharkadm.data.archive import delivery_note, analyse_info
from sharkadm.data.archive import sampling_info
from sharkadm.data.data_holder import DataHolder
from sharkadm import adm_logger

logger = logging.getLogger(__name__)


class ArchiveDataHolder(DataHolder, ABC):
    _data_type: str | None = None
    _data_format: str | None = None

    _date_str_format = '%Y-%m-%d'

    def __init__(self, archive_root_directory: str | pathlib.Path = None, **kwargs):
        self._archive_root_directory = pathlib.Path(archive_root_directory)

        self._data: pd.DataFrame = pd.DataFrame()
        self._dataset_name: str | None = None

        self._delivery_note: delivery_note.DeliveryNote | None = None
        self._sampling_info: sampling_info.SamplingInfo | None = None
        self._analyse_info: analyse_info.AnalyseInfo | None = None
        self._import_matrix: ImportMatrixConfig | None = None
        self._import_matrix_mapper: ImportMatrixMapper | None = None
        # self._data_type_mapper = get_data_type_mapper()

        self._data_sources = {}

        self._initiate()
        self._load_delivery_note()
        self._load_sampling_info()
        self._load_analyse_info()
        self._load_import_matrix()
        self._load_data()

    @staticmethod
    def get_data_holder_description() -> str:
        return """Holds data from an archive"""

    def _initiate(self) -> None:
        self._dataset_name = self.archive_root_directory.name

    # @property
    # def data(self) -> pd.DataFrame:
    #     return self._data

    @property
    def header_mapper(self):
        # TODO: Change
        return self._import_matrix_mapper

    @property
    def data_type(self) -> str:
        # return self._data_type_mapper.get(self.data_format)
        return self._data_type

    @property
    def delivery_note(self) -> delivery_note.DeliveryNote:
        return self._delivery_note

    @property
    def sampling_info(self) -> sampling_info.SamplingInfo:
        return self._sampling_info

    @property
    def analyse_info(self) -> analyse_info.AnalyseInfo:
        return self._analyse_info

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @property
    def reporting_institute(self) -> str:
        return self._delivery_note.reporting_institute

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
    def sampling_info_path(self) -> pathlib.Path:
        return self.processed_data_directory / 'sampling_info.txt'

    @property
    def analyse_info_path(self) -> pathlib.Path:
        return self.processed_data_directory / 'analyse_info.txt'

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
        self._delivery_note = delivery_note.DeliveryNote.from_txt_file(self.delivery_note_path)

    def _load_sampling_info(self) -> None:
        if not self.sampling_info_path.exists():
            adm_logger.log_workflow(f'No sampling info file for {self.dataset_name}', level=adm_logger.INFO)
            return
        self._sampling_info = sampling_info.SamplingInfo.from_txt_file(self.sampling_info_path,
                                                                       mapper=self._import_matrix_mapper)

    def _load_analyse_info(self) -> None:
        if not self.analyse_info_path.exists():
            adm_logger.log_workflow(f'No analyse info file for {self.dataset_name}', level=adm_logger.INFO)
            return
        self._analyse_info = analyse_info.AnalyseInfo.from_txt_file(self.analyse_info_path, mapper=self._import_matrix_mapper)

    def _load_import_matrix(self) -> None:
        """Loads the import matrix for the given data type and provider found in delivery note"""
        self._import_matrix = config.get_import_matrix_config(data_type=self.delivery_note.data_type)
        if not self._import_matrix:
            self._import_matrix = config.get_import_matrix_config(data_type=self.delivery_note.data_format)
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

    @abstractmethod
    def _load_data(self):
        ...


