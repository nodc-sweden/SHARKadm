from unittest.mock import patch

import polars as pl
import pytest

from sharkadm import adm_logger
from sharkadm.transformers import FormatSerialNumber


@pytest.mark.parametrize(
    "given_row_number, given_serial_number, expected_formatted_serial_number",
    (
        (1, "0001", "0001"),  # Already padded
        (1, "1234", "1234"),  # No need for padding
        (2, "42", "0042"),  # Without padding
        (3, "007", "0007"),  # Too few zeroes
        (4, "00000000000000009999", "9999"),  # Too many zeroes
        (5, "   3", "0003"),  # Padded with leading spaces
        (6, "123   ", "0123"),  # Padded with trailing spaces
        (7, "\t7", "0007"),  # Padded with tab
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_format_serial_number_succeeds(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_row_number,
    given_serial_number,
    expected_formatted_serial_number,
):
    # Given data with a given serial number
    given_data = pl.DataFrame(
        [{"row_number": given_row_number, "SERNO": given_serial_number}]
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When transforming the data
    adm_logger.reset_log()
    FormatSerialNumber().transform(given_data_holder)

    # Then the data is the expected value
    transformed_value = given_data_holder.data["SERNO"].first()
    assert transformed_value == expected_formatted_serial_number

    # And the original value is still available
    original_value = given_data_holder.data["reported_serial_number"].first()
    assert original_value == given_serial_number

    # And if data was changed, the original row for the data is logged
    # Then there should be exactly one transformation message
    all_logs = adm_logger.data
    transformation_logs = [
        log for log in all_logs if log["log_type"] == adm_logger.TRANSFORMATION
    ]

    if given_serial_number != expected_formatted_serial_number:
        assert len(transformation_logs) == 1

        log_message = transformation_logs[0]
        assert log_message["row_numbers"] == [given_row_number]
    else:
        assert len(transformation_logs) == 0


@pytest.mark.parametrize(
    "given_row_number, given_serial_number, check_row_numbers",
    (
        (1, "1.23", True),  # Float number
        (2, "12345", True),  # Too high number
        (3, "S13", True),  # Not numeric
        (4, 123, False),  # Not string
        (5, "", True),  # Empty value
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_format_serial_numbers_fails(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_row_number,
    given_serial_number,
    check_row_numbers,
):
    # Given data with a given serial number
    given_data = pl.DataFrame(
        [{"row_number": given_row_number, "SERNO": given_serial_number}]
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # Given the original data is cloned
    data_before_transform = given_data_holder.data.clone()

    # When transforming the data
    adm_logger.reset_log()
    FormatSerialNumber().transform(given_data_holder)

    # Then the data is not altered
    data_after_transform = given_data_holder.data
    assert data_after_transform.equals(data_before_transform)

    # Then there should be exactly one transformation message
    all_logs = adm_logger.data
    transformation_logs = [
        log for log in all_logs if log["log_type"] == adm_logger.TRANSFORMATION
    ]

    transformation_warnings = [
        log for log in transformation_logs if log["level"] == adm_logger.WARNING
    ]

    assert len(transformation_warnings) == 1

    if check_row_numbers:
        log_message = transformation_warnings[0]
        assert log_message["row_numbers"] == [given_row_number]
