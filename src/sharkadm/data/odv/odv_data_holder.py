import pathlib
from typing import Protocol

import polars as pl

from sharkadm.data.archive import analyse_info, sampling_info
from sharkadm.data.data_holder import PolarsDataHolder
from sharkadm.data.data_source.profile.standard_format_file import (
    OdvProfilePolarsDataFile,
)


class HeaderMapper(Protocol):
    def get_internal_name(self, external_par: str) -> str: ...


class PolarsOdvDataHolder(PolarsDataHolder):
    _data_type_internal = "physicalchemical"
    _data_type = "Physical and Chemical"
    _data_format = "ODV"
    _data_structure = "profile"

    def __init__(
        self,
        path: str | pathlib.Path | None = None,
        header_mapper: HeaderMapper = None,
        **kwargs,
    ):
        super().__init__()
        root_path = pathlib.Path(path)
        self._kwargs = kwargs

        if not root_path.exists():
            raise FileNotFoundError(path)
        if root_path.is_file():
            self._paths = [root_path]
        else:
            self._paths = [p for p in root_path.iterdir() if p.suffix == ".txt"]

        self._header_mapper = header_mapper

        self._data: pl.DataFrame = pl.DataFrame()
        self._dataset_name = f"ODV_{root_path.name}"

        self._sampling_info: sampling_info.SamplingInfo | None = None
        self._analyse_info: analyse_info.AnalyseInfo | None = None

        self._qf_column_prefix = "QV:"

        # self._load_sampling_info()
        # self._load_analyse_info()
        self._load_data()

    @staticmethod
    def get_data_holder_description() -> str:
        return """Holds data from one or more odv profiles"""

    # @property
    # def data_file_path(self) -> pathlib.Path:
    #     return self._lims_root_directory / "Raw_data" / "data.txt"
    #
    # @property
    # def sampling_info_path(self) -> pathlib.Path:
    #     return self._lims_root_directory / "Raw_data" / "sampling_info.txt"
    #
    # @property
    # def analyse_info_path(self) -> pathlib.Path:
    #     return self._lims_root_directory / "Raw_data" / "analyse_info.txt"

    # @property
    # def sampling_info(self) -> sampling_info.SamplingInfo:
    #     return self._sampling_info
    #
    # @property
    # def analyse_info(self) -> analyse_info.AnalyseInfo:
    #     return self._analyse_info

    def _load_data(self) -> None:
        dfs = []
        columns = None
        for path in self._paths:
            print(f"{path=}")
            data_source = OdvProfilePolarsDataFile(
                path=path,
                data_type=self.data_type,
                # encoding=self._kwargs.pop('encoding', 'utf-8'),
                **self._kwargs,
            )
            if self._header_mapper:
                data_source.map_header(self._header_mapper)
            if not columns:
                columns = set(data_source.data.columns)
            columns.intersection_update(data_source.data.columns)
            dfs.append(data_source.data)
            self._data_sources[str(data_source)] = data_source
        dfs = [df.select(sorted(columns)) for df in dfs]
        self._data = pl.concat(dfs, how="vertical_relaxed")
        self._data.fill_nan("")
        self._add_date_and_time()

    def _add_date_and_time(self):
        self._data = self._data.with_columns(
            pl.col("sample_iso_datetime").str.slice(0, 10).alias("SDATE"),
            pl.col("sample_iso_datetime").str.slice(10, 8).alias("STIME"),
        )

    # def _load_sampling_info(self) -> None:
    #     #TODO: To be added
    #     if not self.sampling_info_path.exists():
    #         adm_logger.log_workflow(
    #             f"No sampling info file for {self.dataset_name}", level=adm_logger.INFO
    #         )
    #         return
    #     self._sampling_info = sampling_info.SamplingInfo.from_txt_file(
    #         self.sampling_info_path, mapper=self._header_mapper
    #     )
    #
    # def _load_analyse_info(self) -> None:
    #     # TODO: To be added
    #     if not self.analyse_info_path.exists():
    #         adm_logger.log_workflow(
    #             f"No analyse info file for {self.dataset_name}", level=adm_logger.INFO
    #         )
    #         return
    #     self._analyse_info = analyse_info.AnalyseInfo.from_lims_txt_file(
    #         self.analyse_info_path, mapper=self._header_mapper
    #     )

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
