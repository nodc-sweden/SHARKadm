import polars as pl

from sharkadm.data.archive.analyse_info import AnalyseInfo
from sharkadm.data.data_holder import PolarsDataHolder
from sharkadm.transformers import PolarsAddAnalyseInfo


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


def test_add_analyse_info(given_data_in_row_format, given_analyse_info):
    given_data_holder = DataframeDataHolder(given_data_in_row_format, given_analyse_info)
    given_transformer = PolarsAddAnalyseInfo()

    columns_before = set(given_data_holder.columns)

    # When transforming the data
    given_transformer.transform(given_data_holder)

    # Then the columns of the data after is a true superset of the columns before
    # I.e. there are columns added and no columns are removed
    columns_after = set(given_data_holder.columns)
    assert columns_after > columns_before
