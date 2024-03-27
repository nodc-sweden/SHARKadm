import logging
import pathlib

import pandas as pd
from typing import Protocol

from sharkadm.data.data_holder import DataHolder
from sharkadm.data import data_source
from sharkadm.data.archive import sampling_info
from sharkadm import adm_logger

logger = logging.getLogger(__name__)


class HeaderMapper(Protocol):

    def get_internal_name(self, external_par: str) -> str:
        ...


class LimsDataHolder(DataHolder):
    _data_type = 'physicalchemical'

    def __init__(self,
                 lims_root_directory: str | pathlib.Path = None,
                 header_mapper: HeaderMapper = None):
        super().__init__()
        self._lims_root_directory = pathlib.Path(lims_root_directory)
        if not self._lims_root_directory.is_dir():
            raise NotADirectoryError(self._lims_root_directory)
        if self._lims_root_directory.name.lower() == 'raw_data':
            self._lims_root_directory = self._lims_root_directory.parent

        self._header_mapper = header_mapper

        self._data: pd.DataFrame = pd.DataFrame()
        self._dataset_name: str | None = None

        self._sampling_info: sampling_info.SamplingInfo | None = None

        self._load_sampling_info()
        self._load_data()

    @staticmethod
    def get_data_holder_description() -> str:
        return """Holds data from a lims export"""

    @property
    def data_file_path(self) -> pathlib.Path:
        return self._lims_root_directory / 'Raw_data' / 'data.txt'

    @property
    def sampling_info_path(self) -> pathlib.Path:
        return self._lims_root_directory / 'Raw_data' / 'sampling_info.txt'

    @property
    def sampling_info(self) -> sampling_info.SamplingInfo:
        return self._sampling_info

    def _load_data(self) -> None:
        d_source = data_source.TxtColumnFormatDataFile(path=self.data_file_path, data_type=self.data_type)
        if self._header_mapper:
            d_source.map_header(self._header_mapper)
        self._data = self._get_data_from_data_source(d_source)
        self._dataset_name = self._lims_root_directory.stem

    def _load_sampling_info(self) -> None:
        if not self.sampling_info_path.exists():
            adm_logger.log_workflow(f'No sampling info file for {self.dataset_name}', level=adm_logger.INFO)
            return
        self._sampling_info = sampling_info.SamplingInfo.from_txt_file(self.sampling_info_path,
                                                                       mapper=self._header_mapper)


    @staticmethod
    def _get_data_from_data_source(data_source: data_source.DataFile) -> pd.DataFrame:
        data = data_source.get_data()
        data = data.fillna('')
        data.reset_index(inplace=True, drop=True)
        return data

    # @property
    # def data(self) -> pd.DataFrame:
    #     return self._data
    #
    # @data.setter
    # def data(self, df: pd.DataFrame) -> None:
    #     if type(df) != pd.DataFrame:
    #         raise 'Data must be of type pd.DataFrame'
    #     self._data = df

    @property
    def data_type(self) -> str:
        return self._data_type

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @property
    def columns(self) -> list[str]:
        return sorted(self.data.columns)




