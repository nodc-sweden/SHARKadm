from unittest.mock import patch

import polars as pl
import pytest

from sharkadm import adm_logger
from sharkadm.validators import ValidatePositiveValues


@pytest.mark.parametrize(
    "columns_to_validate, given_column, given_negative_value, given_row_number",
    (
        (("air_pressure_hpa", "wind_direction_code"), "air_pressure_hpa", -1.23, 3),
        (
            ("weather_observation_code", "cloud_observation_code"),
            "weather_observation_code",
            -10,
            4,
        ),
        (("wave_observation_code", "water_depth_m"), "wave_observation_code", -3, 5),
        (
            ("wind_speed_ms", "ice_observation_code", "water_depth_m"),
            "wind_speed_ms",
            -30.1,
            9,
        ),
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_negative_values_are_identified(
    mocked_data_types,
    polars_data_frame_holder_class,
    columns_to_validate,
    given_column,
    given_negative_value,
    given_row_number,
):
    # Given data with a negative value in a given column and row
    assert given_negative_value < 0

    given_values = [
        {
            column: f"{12.34 * (0.8 + ((col_n + row_n / 10) % 0.123)):.3f}"
            for col_n, column in enumerate(columns_to_validate, start=1)
        }
        | {"row_number": row_n}
        for row_n in range(1, given_row_number * 2 + 2)
    ]

    given_values[given_row_number - 1][given_column] = str(given_negative_value)

    given_data = pl.DataFrame(given_values)

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidatePositiveValues(columns_to_validate).validate(given_data_holder)

    # Then there should be exactly one failed validation messages for
    # that specific validator and column
    all_logs = adm_logger.data
    specific_validation_logs = [
        log
        for log in all_logs
        if log["log_type"] == adm_logger.VALIDATION
        and log["cls"] == "ValidatePositiveValues"
    ]

    failed_validations_logs = [
        log for log in specific_validation_logs if not log["validation_success"]
    ]

    failed_logs_for_specific_column = [
        log for log in failed_validations_logs if log["column"] == given_column
    ]

    assert len(failed_logs_for_specific_column) == 1

    # And the row is correctly identified
    assert given_row_number in failed_logs_for_specific_column[0]["row_numbers"]
