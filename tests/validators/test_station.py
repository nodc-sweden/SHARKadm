from unittest.mock import patch

import pandas as pd
import pytest

from sharkadm import adm_logger
from sharkadm.validators.station import (
    ValidateCoordinatesDm,
    ValidateCoordinatesSweref99,
    ValidateNameInMaster,
    ValidateSynonymsInMaster,
)


@patch("sharkadm.config.get_all_data_types")
def test_name_validation_fails_if_no_station_list(
    mocked_data_types, pandas_data_frame_holder_class
):
    # Given a valid data holder
    given_data_holder = pandas_data_frame_holder_class(pd.DataFrame())
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data without a set of known station names
    adm_logger.reset_log()
    ValidateNameInMaster().validate(given_data_holder)

    # Then there should be exactly one failed validation message
    all_logs = adm_logger.data
    failed_validation_logs = [
        log
        for log in all_logs
        if log["log_type"] == adm_logger.VALIDATION and not log["validation_success"]
    ]

    assert len(failed_validation_logs)


@pytest.mark.parametrize(
    "given_station_name, given_known_station_names, expected_success",
    (
        ("station_1", {"station_1"}, True),
        ("station_1", {"not_station_1"}, False),
        ("station NAME", {"STATION name"}, True),
        ("station_1", {"not_station_1", "station_1", "station_2"}, True),
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_validate_station_name(
    mocked_data_types,
    pandas_data_frame_holder_class,
    given_station_name,
    given_known_station_names,
    expected_success,
):
    # Given data with a given station name
    given_data = pd.DataFrame([{"statn": given_station_name}])

    # Given a valid data holder
    given_data_holder = pandas_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateNameInMaster(given_known_station_names).validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success


@pytest.mark.parametrize(
    "given_station_name, given_master_synonym_list, expected_success",
    (
        ("a1", {"Station 1": {"b2"}}, False),
        ("a1", {"Station A1": {"a1"}}, True),
        ("sTaTiOn AlIaS", {"Station": {"station alias"}}, True),
        (
            "The station",
            {
                "Station A1": {"a1", "The station"},
                "Another station": {"b2", "The Station"},
            },
            True,
        ),
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_validate_station_synonym(
    mocked_data_types,
    pandas_data_frame_holder_class,
    given_station_name,
    given_master_synonym_list,
    expected_success,
):
    # Given data with a given station name
    given_data = pd.DataFrame([{"statn": given_station_name, "row_number": 1}])

    # Given a valid data holder
    given_data_holder = pandas_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateSynonymsInMaster(given_master_synonym_list).validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success


@patch("sharkadm.config.get_all_data_types")
def test_coordinates_dm_validation_fails_if_coordinates(
    mocked_data_types, pandas_data_frame_holder_class
):
    # Given a valid data holder
    given_data_holder = pandas_data_frame_holder_class(pd.DataFrame())
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data without a set of known station names
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
    pandas_data_frame_holder_class,
    given_latitude_value,
    given_longitude_value,
    expected_success,
):
    # Given data with given coordinates
    given_data = pd.DataFrame(
        [{"LATIT": given_latitude_value, "LONGI": given_longitude_value, "row_number": 1}]
    )

    # Given a valid data holder
    given_data_holder = pandas_data_frame_holder_class(given_data)
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


@pytest.mark.parametrize(
    "given_latitude_value, given_longitude_value, expected_success",
    (
        ("", "", False),
        ("X", "Y", False),
        ("5799999.999", "500000", False),  # Below lower latitude bound
        ("5800000", "500000", True),  # On lower latitude bound
        ("7500000", "500000", True),  # On upper latitude bound
        ("7500000.001", "500000", False),  # Above upper latitude bound
        ("6500000", "-150000.001", False),  # Below lower longitude bound
        ("6500000", "-150000", True),  # On lower longitude bound
        ("6500000", "1400000", True),  # On upper longitude bound
        ("6500000", "1400000.001", False),  # Above upper longitude bound
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_validate_coordinates_sweref99(
    mocked_data_types,
    pandas_data_frame_holder_class,
    given_latitude_value,
    given_longitude_value,
    expected_success,
):
    # Given data with given Sweref 99 coordinates
    given_data = pd.DataFrame(
        [{"LATIT": given_latitude_value, "LONGI": given_longitude_value, "row_number": 1}]
    )

    # Given a valid data holder
    given_data_holder = pandas_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateCoordinatesSweref99().validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success
