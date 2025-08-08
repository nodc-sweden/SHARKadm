from unittest.mock import patch

import polars as pl
import pytest

from sharkadm import adm_logger
from sharkadm.validators.station.name_in_master import ValidateNameInMaster


@patch("sharkadm.config.get_all_data_types")
def test_name_validation_fails_if_no_station_list(
    mocked_data_types, polars_data_frame_holder_class
):
    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(pl.DataFrame())
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
        ("BCS III-10", {"not_station_1", "BCS III-10", "station_2"}, True),
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_validate_station_name(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_station_name,
    given_known_station_names,
    expected_success,
):
    # Given data with a given station name
    given_data = pl.DataFrame([{"reported_station_name": given_station_name}])

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
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
