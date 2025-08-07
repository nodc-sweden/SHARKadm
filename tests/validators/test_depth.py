from unittest.mock import patch

import polars as pl
import pytest

from sharkadm import adm_logger
from sharkadm.validators.depth import ValidateSampleDepth, ValidateSecchiDepth


@pytest.mark.parametrize(
    "given_sample_depth, given_water_depth, expected_success",
    (
        (99.9, 100, True),
        (100, 100, False),
        (100.1, 100, False),
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_sample_depth_is_validated_against_water_depth(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_sample_depth,
    given_water_depth,
    expected_success,
):
    # Given data with given sample depth and water depth
    given_data = pl.DataFrame(
        [
            {
                "sample_depth_m": given_sample_depth,
                "water_depth_m": given_water_depth,
                "row_number": 1,
            }
        ]
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateSampleDepth().validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success


@pytest.mark.parametrize(
    "given_parameters", ({"water_depth_m": "100"}, {"sample_depth_m": "90"}, {})
)
@patch("sharkadm.config.get_all_data_types")
def test_sample_depth_cant_be_validated_if_missing_parameters(
    mocked_data_types, polars_data_frame_holder_class, given_parameters
):
    # Given data with subset of required parameters
    given_data = pl.DataFrame([given_parameters | {"row_number": 1}])

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateSampleDepth().validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status should be false
    log_message = validator_logs[0]
    assert not log_message["validation_success"]


@pytest.mark.parametrize(
    "given_secchi_depth, given_water_depth, expected_success",
    (
        (99.9, 100, True),
        (100, 100, False),
        (100.1, 100, False),
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_secchi_depth_is_validated_against_water_depth(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_secchi_depth,
    given_water_depth,
    expected_success,
):
    # Given data with given secchi depth and water depth
    given_data = pl.DataFrame(
        [
            {
                "parameter": "SECCHI",
                "value": given_secchi_depth,
                "water_depth_m": given_water_depth,
                "row_number": 1,
            }
        ]
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateSecchiDepth().validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success


@pytest.mark.parametrize(
    "given_parameters",
    (
        {"water_depth_m": "100", "parameter": "Not SECCHI", "value": "8"},
        {"parameter": "SECCHI", "value": "8"},
        {"water_depth_m": "100", "parameter": "SECCHI"},
        {"water_depth_m": "100", "value": "8"},
        {"water_depth_m": "100"},
        {"parameter": "SECCHI"},
        {"value": "8"},
        {},
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_secchi_depth_cant_be_validated_if_missing_parameters(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_parameters,
):
    # Given data with subset of required parameters
    given_data = pl.DataFrame([given_parameters | {"row_number": 1}])

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateSecchiDepth().validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status should be false
    log_message = validator_logs[0]
    assert not log_message["validation_success"]
