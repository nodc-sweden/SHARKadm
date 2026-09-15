from unittest.mock import Mock

import polars as pl
from nodc_config import Config

from sharkadm.data.data_source.base import PolarsDataSource


def test_polars_data_source_always_has_dataframe():
    config = Mock(Config)
    given_data_source = PolarsDataSource(nodc_conf=config)

    data = given_data_source.get_data()
    assert isinstance(data, pl.DataFrame)
