import polars as pl

from sharkadm.data.data_source.base import PolarsDataSource


def test_polars_data_source_always_has_dataframe():
    given_data_source = PolarsDataSource()

    data = given_data_source.get_data()
    assert isinstance(data, pl.DataFrame)
