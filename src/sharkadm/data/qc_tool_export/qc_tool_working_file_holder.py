import pathlib
from typing import Protocol

import polars as pl

from sharkadm.data.data_holder import PolarsDataHolder
from sharkadm.data.data_source.base import PolarsDataFile
from sharkadm.data.data_source.txt_file import CsvRowFormatPolarsDataFile


class HeaderMapper(Protocol):
    def get_internal_name(self, external_par: str) -> str: ...


class PolarsQcToolDataHolder(PolarsDataHolder):
    # _data_type_internal = "physicalchemical"
    _data_type_synonym = "physicalchemical"
    # _data_type = "Physical and Chemical"
    # _data_format = "LIMS"
    _data_structure = "row"

    def __init__(
        self,
        qc_file_path: str | pathlib.Path | None = None,
        header_mapper: HeaderMapper = None,
    ):
        super().__init__()

        self._qc_file_path = pathlib.Path(qc_file_path)

        self._header_mapper = header_mapper

        self._data: pl.DataFrame = pl.DataFrame()
        self._dataset_name: str | None = None

        #self._qf_column_prefix = "Q_"
        self._load_data()

    @staticmethod
    def get_data_holder_description() -> str:
        return """Holds data from a qc-tool working file export"""

    def _load_data(self) -> None:
        data_source = CsvRowFormatPolarsDataFile(
            path=self._qc_file_path, data_type=self.data_type
        )
        if self._header_mapper:
            data_source.map_header(self._header_mapper)
        self._set_data_source(data_source)
        self._dataset_name = self._qc_file_path.stem

    @staticmethod
    def _get_data_from_data_source(data_source: PolarsDataFile) -> pl.DataFrame:
        data = data_source.get_data()
        data = data.fill_nan("")
        return data

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @property
    def columns(self) -> list[str]:
        return sorted(self.data.columns)