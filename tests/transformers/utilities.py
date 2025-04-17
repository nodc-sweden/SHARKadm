import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.data.archive.analyse_info import AnalyseInfo


class DataframeDataHolder(PolarsDataHolder):
    """Minimal polars data holder"""

    _data_type_internal = "phytoplankton"
    _data_type = "data type"
    _data_format = "format"
    _data_structure = "structure"

    def __init__(self, data: pl.DataFrame, info: dict):
        super().__init__()
        self._data = data
        self._dataset_name = "Bob"
        self.analyse_info = AnalyseInfo(info)

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @property
    def data_type_internal(self) -> str:
        return self._data_type_internal

    @property
    def data_type(self) -> str:
        return self._data_type

    @staticmethod
    def get_data_holder_description() -> str:
        return "Simple data holder"
