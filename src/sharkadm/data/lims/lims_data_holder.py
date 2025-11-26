import logging
import pathlib
from typing import Protocol

import pandas as pd
import polars as pl

from sharkadm.config.data_type import data_type_handler
from sharkadm.data.archive import analyse_info, sampling_info
from sharkadm.data.data_holder import PandasDataHolder, PolarsDataHolder
from sharkadm.data.data_source.base import DataFile
from sharkadm.data.data_source.txt_file import (
    CsvRowFormatPolarsDataFile,
    TxtColumnFormatDataFile,
)
from sharkadm.sharkadm_logger import adm_logger

logger = logging.getLogger(__name__)


class HeaderMapper(Protocol):
    def get_internal_name(self, external_par: str) -> str: ...


class LimsDataHolder(PandasDataHolder):
    _data_type_internal = "physicalchemical"
    _data_type = "Physical and Chemical"
    _data_format = "LIMS"
    _data_structure = "column"

    def __init__(
        self,
        lims_root_directory: str | pathlib.Path | None = None,
        header_mapper: HeaderMapper = None,
    ):
        super().__init__()
        self._lims_root_directory = pathlib.Path(lims_root_directory)
        if not self._lims_root_directory.is_dir():
            raise NotADirectoryError(self._lims_root_directory)
        if self._lims_root_directory.name.lower() == "raw_data":
            self._lims_root_directory = self._lims_root_directory.parent
        raw_data_path = self._lims_root_directory / "Raw_data"
        if not raw_data_path.exists():
            raise NotADirectoryError(
                f"Invalid LIMS directory: {self._lims_root_directory}"
            )

        self._header_mapper = header_mapper

        self._data: pd.DataFrame = pd.DataFrame()
        self._dataset_name: str | None = None

        self._sampling_info: sampling_info.SamplingInfo | None = None
        self._analyse_info: analyse_info.AnalyseInfo | None = None

        self._qf_column_prefix = "Q_"

        self._load_sampling_info()
        self._load_analyse_info()
        self._load_data()

    @staticmethod
    def get_data_holder_description() -> str:
        return """Holds data from a lims export"""

    @property
    def data_file_path(self) -> pathlib.Path:
        return self._lims_root_directory / "Raw_data" / "data.txt"

    @property
    def sampling_info_path(self) -> pathlib.Path:
        return self._lims_root_directory / "Raw_data" / "sampling_info.txt"

    @property
    def analyse_info_path(self) -> pathlib.Path:
        return self._lims_root_directory / "Raw_data" / "analyse_info.txt"

    @property
    def sampling_info(self) -> sampling_info.SamplingInfo:
        return self._sampling_info

    @property
    def analyse_info(self) -> analyse_info.AnalyseInfo:
        return self._analyse_info

    def _load_data(self) -> None:
        d_source = TxtColumnFormatDataFile(
            path=self.data_file_path, data_type=self.data_type
        )
        if self._header_mapper:
            d_source.map_header(self._header_mapper)
        self._set_data_source(d_source)
        # self._data = self._get_data_from_data_source(d_source)
        self._dataset_name = self._lims_root_directory.stem

    def _load_sampling_info(self) -> None:
        if not self.sampling_info_path.exists():
            adm_logger.log_workflow(
                f"No sampling info file for {self.dataset_name}", level=adm_logger.INFO
            )
            return
        self._sampling_info = sampling_info.SamplingInfo.from_txt_file(
            self.sampling_info_path, mapper=self._header_mapper
        )

    def _load_analyse_info(self) -> None:
        if not self.analyse_info_path.exists():
            adm_logger.log_workflow(
                f"No analyse info file for {self.dataset_name}", level=adm_logger.INFO
            )
            return
        self._analyse_info = analyse_info.AnalyseInfo.from_lims_txt_file(
            self.analyse_info_path, mapper=self._header_mapper
        )

    @staticmethod
    def _get_data_from_data_source(data_source: DataFile) -> pd.DataFrame:
        data = data_source.get_data()
        data = data.fillna("")
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
    def data_type_internal(self) -> str:
        return self._data_type_internal

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @property
    def columns(self) -> list[str]:
        return sorted(self.data.columns)


class PolarsLimsDataHolder(PolarsDataHolder):
    # _data_type_internal = "physicalchemical"
    _data_type = data_type_handler.get_datatype("physicalchemical")
    # _data_type = "Physical and Chemical"
    _data_format = "LIMS"
    _data_structure = "column"

    def __init__(
        self,
        lims_root_directory: str | pathlib.Path | None = None,
        header_mapper: HeaderMapper = None,
    ):
        super().__init__()
        self._lims_root_directory = pathlib.Path(lims_root_directory)
        if not self._lims_root_directory.is_dir():
            raise NotADirectoryError(self._lims_root_directory)
        if self._lims_root_directory.name.lower() == "raw_data":
            self._lims_root_directory = self._lims_root_directory.parent
        raw_data_path = self._lims_root_directory / "Raw_data"
        if not raw_data_path.exists():
            raise NotADirectoryError(
                f"Invalid LIMS directory: {self._lims_root_directory}"
            )

        self._header_mapper = header_mapper

        self._data: pl.DataFrame = pl.DataFrame()
        self._dataset_name: str | None = None

        self._sampling_info: sampling_info.SamplingInfo | None = None
        self._analyse_info: analyse_info.AnalyseInfo | None = None

        self._qf_column_prefix = "Q_"

        self._load_sampling_info()
        self._load_analyse_info()
        self._load_data()

    @staticmethod
    def get_data_holder_description() -> str:
        return """Holds data from a lims export"""

    @property
    def data_file_path(self) -> pathlib.Path:
        return self._lims_root_directory / "Raw_data" / "data.txt"

    @property
    def sampling_info_path(self) -> pathlib.Path:
        return self._lims_root_directory / "Raw_data" / "sampling_info.txt"

    @property
    def analyse_info_path(self) -> pathlib.Path:
        return self._lims_root_directory / "Raw_data" / "analyse_info.txt"

    @property
    def sampling_info(self) -> sampling_info.SamplingInfo:
        return self._sampling_info

    @property
    def analyse_info(self) -> analyse_info.AnalyseInfo:
        return self._analyse_info

    def _load_data(self) -> None:
        data_source = CsvRowFormatPolarsDataFile(
            path=self.data_file_path, data_type=self.data_type
        )
        if self._header_mapper:
            data_source.map_header(self._header_mapper)
        self._set_data_source(data_source)
        self._dataset_name = self._lims_root_directory.stem

    def _load_sampling_info(self) -> None:
        if not self.sampling_info_path.exists():
            adm_logger.log_workflow(
                f"No sampling info file for {self.dataset_name}", level=adm_logger.INFO
            )
            return
        self._sampling_info = sampling_info.SamplingInfo.from_txt_file(
            self.sampling_info_path, mapper=self._header_mapper
        )

    def _load_analyse_info(self) -> None:
        if not self.analyse_info_path.exists():
            adm_logger.log_workflow(
                f"No analyse info file for {self.dataset_name}", level=adm_logger.INFO
            )
            return
        self._analyse_info = analyse_info.AnalyseInfo.from_lims_txt_file(
            self.analyse_info_path, mapper=self._header_mapper
        )

    @staticmethod
    def _get_data_from_data_source(data_source: DataFile) -> pl.DataFrame:
        data = data_source.get_data()
        data = data.fill_nan("")
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
        return self._data_type.data_type

    @property
    def data_type_internal(self) -> str:
        return self._data_type.data_type_internal

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @property
    def columns(self) -> list[str]:
        return sorted(self.data.columns)
