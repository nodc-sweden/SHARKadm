from unittest.mock import patch

import numpy as np
import pandas as pd
import polars as pl
import pytest

from sharkadm.transformers.replace import (
    PolarsReplaceNanWithNone,
    ReplaceNanWithEmptyString,
)


@pytest.mark.parametrize(
    "given_visit_key, given_latitude, given_longitude, given_depths, "
    "given_parameter, given_value, expected_empty_strings",
    (
        ("ABC123", 57.3, 10.1, 5.0, "PHOS", np.nan, 1),  # example data with nan values
        ("ABC123", 58.2, np.nan, 7.3, "PTOT", np.nan, 2),  # example data with nan values
    ),
)
@patch("sharkadm.config.get_all_data_types", return_value=[])
def test_validate_replace_nan_with_empty_string(
    mocked_data_types,
    pandas_data_frame_holder_class,
    given_visit_key,
    given_latitude,
    given_longitude,
    given_depths,
    given_parameter,
    given_value,
    expected_empty_strings,
):
    # Arrange
    given_data = pd.DataFrame(
        {
            "visit_key": [given_visit_key],
            "sample_latitude_dd": [given_latitude],
            "sample_longitude_dd": [given_longitude],
            "sample_depth_m": [given_depths],
            "parameter": [given_parameter],
            "value": [given_value],
        }
    )

    # Given a valid data holder
    given_data_holder = pandas_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # There should be nan values in the dataframe
    # before application of transformer
    assert given_data_holder.data.isna().values.any(), (
        "There are no nan values in the dataframe"
    )

    # Transforming the data
    ReplaceNanWithEmptyString().transform(given_data_holder)

    # After transformation there should be no nan values
    # in the dataframe
    assert not given_data_holder.data.isna().values.any(), (
        "There are still nan values in the dataframe"
    )

    # The number of None in the dataframe should match the expected value
    assert expected_empty_strings == (given_data_holder.data == "").sum().sum(), (
        "The number of fields with empty string do not match the expected value"
    )


@pytest.mark.parametrize(
    "given_visit_key, given_latitude, given_longitude, given_depths, "
    "given_parameter, given_value, expected_none",
    (
        ("ABC123", 57.3, 10.1, 5.0, "PHOS", np.nan, 1),  # example data with nan values
        ("ABC123", 58.2, np.nan, 7.3, "PTOT", np.nan, 2),  # example data with nan values
    ),
)
@patch("sharkadm.config.get_all_data_types", return_value=[])
def test_validate_polars_replace_nan_with_none(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_visit_key,
    given_latitude,
    given_longitude,
    given_depths,
    given_parameter,
    given_value,
    expected_none,
):
    # Arrange
    given_data = pl.DataFrame(
        {
            "visit_key": given_visit_key,
            "sample_latitude_dd": given_latitude,
            "sample_longitude_dd": given_longitude,
            "sample_depth_m": given_depths,
            "parameter": given_parameter,
            "value": given_value,
        }
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # There should be nan values in the dataframe
    # before application of transformer
    number_of_nans_in_data_holder = 0
    for col in given_data_holder.data.columns:
        try:
            number_of_nans_in_data_holder += given_data_holder.data.select(
                pl.col(col).cast(pl.Float64).is_nan().sum()
            ).row(0)[0]
        except Exception:
            pass

    assert number_of_nans_in_data_holder > 0, "There are no nan values in the dataframe"

    # Transforming the data
    PolarsReplaceNanWithNone().transform(given_data_holder)

    number_of_nans_in_data_holder = 0
    for col in given_data_holder.data.columns:
        try:
            number_of_nans_in_data_holder += given_data_holder.data.select(
                pl.col(col).cast(pl.Float64).is_nan().sum()
            ).row(0)[0]
        except Exception:
            pass

    # After transformation there should be no nan values
    # in the dataframe
    assert number_of_nans_in_data_holder == 0, (
        "There are still nan values in the dataframe"
    )

    # The number of None in the dataframe should match the expected value
    assert (
        expected_none
        == given_data_holder.data.null_count().select(pl.all()).to_numpy().sum()
    ), "The number of fields with None do not match the expected value"
