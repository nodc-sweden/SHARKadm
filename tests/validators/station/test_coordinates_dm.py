from unittest.mock import patch

import polars as pl
import pytest

from sharkadm import adm_logger
from sharkadm.validators.station.coordinates_dm import ValidateCoordinatesDm


@patch("sharkadm.config.get_all_data_types")
def test_coordinates_dm_validation_fails_if_no_data(
    mocked_data_types, polars_data_frame_holder_class
):
    # Given a valid data holder without data
    given_data_holder = polars_data_frame_holder_class(pl.DataFrame())
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateCoordinatesDm().validate(given_data_holder)

    # Then there should be exactly one failed validation message
    all_logs = adm_logger.data
    failed_validation_logs = [
        log
        for log in all_logs
        if log["log_type"] == adm_logger.VALIDATION and not log["validation_success"]
    ]

    assert len(failed_validation_logs)


@pytest.mark.parametrize(
    "given_latitude_value, given_longitude_value, expected_success",
    (
        ("", "", False),
        ("X", "Y", False),
        ("00.000", "00.000", False),  # No room for both degrees and minutes
        ("000.000", "000.000", True),
        ("000,000", "000,000", False),  # Comma as decimal separator
        ("1234", "1234", True),  # No decimals
        ("9000.000", "18000.000", True),
        ("-9000.000", "-18000.000", True),
        ("9000.001", "1000.000", False),  # Latitude degrees above 90
        ("-9000.001", "1000.000", False),  # Latitude degrees below -90
        ("1000.000", "18000.001", False),  # Longitude degrees above 180
        ("1000.000", "-18000.001", False),  # Longitude degrees below -180
        ("1259.999", "1234.567", True),  # Minutes below 60
        ("1260.000", "1234.567", False),  # Minutes not below 60
        ("1234.567", "1259.999", True),  # Minutes below 60
        ("1234.567", "1260.000", False),  # Minutes not below 60
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_validate_coordinates_dm(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_latitude_value,
    given_longitude_value,
    expected_success,
):
    # Given data with given coordinates
    given_data = pl.DataFrame(
        [{"LATIT": given_latitude_value, "LONGI": given_longitude_value, "row_number": 1}]
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateCoordinatesDm().validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success
