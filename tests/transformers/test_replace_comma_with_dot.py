from unittest.mock import patch

import polars as pl
import pytest

from sharkadm import adm_logger
from sharkadm.transformers import PolarsReplaceCommaWithDot


@pytest.mark.parametrize(
    "given_row_number, given_parameter, given_value, expected_value",
    (
        (1, "latitude", "12345678,9", "12345678.9"),
        (2, "longitude", "1234567,89", "1234567.89"),
        (3, "depth", "123456,789", "123456.789"),
        (4, "DIVIDE", "12345,6789", "12345.6789"),
        (5, "MULTIPLY", "1234,56789", "1234.56789"),
        (6, "COPY_VARIABLE", "123,456789", "123.456789"),
        (7, "sampled_volume", "12,3456789", "12.3456789"),
        (8, "sampler_area", "1,23456789", "1.23456789"),
        (9, "wind", "1,0", "1.0"),
        (10, "pressure", "0,0", "0.0"),
        (11, "temperature", "-123,56", "-123.56"),
        (12, "wind", "", ""),  # Empty, no change
        (13, "depth", "123", "123"),  # No decimals, no change
        (14, "pressure", "12.3", "12.3"),  # Already decimal dot, no change
        (15, "other_column", "12,3", "12,3"),  # Wrong column, no change
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_replace_comma_with_dot(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_row_number,
    given_parameter,
    given_value,
    expected_value,
):
    # Given data with a specific value for a specific parameter
    given_data = pl.DataFrame(
        [
            {
                "row_number": given_row_number,
                given_parameter: given_value,
            },
        ]
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When transforming the data
    adm_logger.reset_log()
    PolarsReplaceCommaWithDot().transform(given_data_holder)

    # Then the data is the expected value
    transformed_value = given_data_holder.data[given_parameter].first()
    assert transformed_value == expected_value

    # And if data was changed, the original row for the data is logged
    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    transformation_logs = [
        log for log in all_logs if log["log_type"] == adm_logger.TRANSFORMATION
    ]

    if given_value != expected_value:
        assert len(transformation_logs) == 1

        log_message = transformation_logs[0]
        assert log_message["row_numbers"] == [given_row_number]
    else:
        assert len(transformation_logs) == 0
