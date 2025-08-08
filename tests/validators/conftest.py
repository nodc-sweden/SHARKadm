import polars as pl
import pytest

from sharkadm.data import PolarsDataHolder


class PolarsDataFrameHolder(PolarsDataHolder):
    def __init__(self, data: pl.DataFrame):
        super().__init__()
        self._data = data

    @property
    def data_type(self) -> str:
        return "data_type"

    @property
    def data_type_internal(self) -> str:
        return "data_type_internal"

    @property
    def dataset_name(self) -> str:
        return "dataset_name"

    def get_data_holder_description(self) -> str:
        return "data_holder_description"


@pytest.fixture
def polars_data_frame_holder_class():
    return PolarsDataFrameHolder
