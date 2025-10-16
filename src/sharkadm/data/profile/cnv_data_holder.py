import pathlib
from typing import Protocol

import polars as pl

from sharkadm.data.archive import metadata
from sharkadm.data.data_source.profile.cnv_file import CnvDataFile
from sharkadm.data.profile.base import PolarsProfileDataHolder


class HeaderMapper(Protocol):
    def get_internal_name(self, external_par: str) -> str: ...


class PolarsCnvDataHolder(PolarsProfileDataHolder):
    _data_format = "cnv"

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
            self.parent_directory = root_path.parent
        else:
            self._paths = [p for p in root_path.iterdir() if p.suffix == ".cnv"]
            self._paths = [path for path in self._paths if path.name[0] != "u"]
            self.parent_directory = root_path

        self._header_mapper = header_mapper

        self._data: pl.DataFrame = pl.DataFrame()
        self._dataset_name = f"CNV_{root_path.name}"

        # self._sampling_info: sampling_info.SamplingInfo | None = None
        # self._analyse_info: analyse_info.AnalyseInfo | None = None
        # self._metadata:

        # self._load_sampling_info()
        # self._load_analyse_info()
        self._load_data()

    @staticmethod
    def get_data_holder_description() -> str:
        return """Holds data from one or more cnv profiles"""

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
            data_source = CnvDataFile(
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

    def add_metadata(self, data: metadata.Metadata) -> None:
        for (date, time), df in self._data.group_by(self.date_column, self.time_column):
            boolean = (pl.col(self.date_column) == date) & (
                pl.col(self.time_column) == time
            )
            kw = {self.date_column: date, self.time_column: time[:5]}
            print(f"{kw=}")
            meta = data.get_info(**kw)
            print(f"{meta=}")
            if len(meta) != 1:
                raise Exception("Metadata error")
            for col, value in meta[0].items():
                if col not in self._data.columns:
                    self._data = self._data.with_columns(pl.lit("").alias(col))
                self._data = self._data.with_columns(
                    pl.when(boolean).then(pl.lit(value)).otherwise(pl.col(col)).alias(col)
                )
