from datetime import datetime
from unittest.mock import patch

import polars as pl
import pytest

from sharkadm.transformers.date_and_time import PolarsAddDatetime


@pytest.mark.parametrize(
    "given_sample_date, given_sample_time, expected_datetime",
    (
        ("1950-01-01", "10:00", "1950-01-01 10:00"),  # Date and time exist
        ("2010-01-01", " ", "2010-01-01 00:00"),  # Missing time
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_data_and_time(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_sample_date,
    given_sample_time,
    expected_datetime,
):
    # Given data with given visit date and sample time
    given_data = pl.DataFrame(
        [
            {
                "sample_date": given_sample_date,
                "sample_time": given_sample_time,
            }
        ]
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    assert "datetime" not in given_data_holder.data.columns, (
        "The column datetime already exist."
    )

    # transforming the data
    PolarsAddDatetime().transform(given_data_holder)

    # After transformation there should be a datetime_str
    # column in the dataframe
    assert "datetime" in given_data_holder.data.columns, (
        "The column datetime was not added to the dataframe"
    )

    result = given_data_holder.data.select("datetime").to_series()[0]

    # The value in the column datetime should match the expected value
    if result is not None:
        assert result == datetime.strptime(expected_datetime, "%Y-%m-%d %H:%M"), (
            "The added datetime to the column datetime_str differs from the expected"
        )
    else:
        assert expected_datetime is None, (
            "The added datetime to the column datetime_str differs from the expected"
        )
