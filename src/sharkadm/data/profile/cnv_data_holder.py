import pathlib
from typing import Protocol

import polars as pl

from sharkadm.data.data_holder import PolarsDataHolder
from sharkadm.data.data_source.profile.cnv_file import CnvDataFile


class HeaderMapper(Protocol):
    def get_internal_name(self, external_par: str) -> str: ...


class PolarsCnvDataHolder(PolarsDataHolder):
    _data_type_internal = "physicalchemical"
    _data_type = "Physical and Chemical"
    _data_format = "cnv"
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
            self.parent_directory = root_path.parent
        else:
            self._paths = [p for p in root_path.iterdir() if p.suffix == ".cnv"]
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
