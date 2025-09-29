from unittest.mock import patch

import polars as pl
import pytest

from sharkadm import adm_logger
from sharkadm.validators.weather import ValidateWeath, ValidateWeatherConsistency


@pytest.mark.parametrize(
    "given_visit_key, given_weather_observation_code, expected_success",
    (
        ("20230802_1050_ZZ99_SKÅPESUND", "0", True),
        ("20230802_1050_ZZ99_SKÅPESUND", "9", True),
        ("20230530_1115_ZZ99_SMÅHOLMARNA", "10", False),
        ("20230802_1050_ZZ99_SKÅPESUND", "5", True),
        ("20230530_0925_ZZ99_SVENSHOLMEN", "7.0", False),  # Float
        ("20230802_1050_ZZ99_SKÅPESUND", "07", False),  # Zeropadded
        ("20230802_1050_ZZ99_SKÅPESUND", "", True),  # Missing as str
        ("20230802_1050_ZZ99_SKÅPESUND", None, True),  # Missing as None
        ("20230802_1050_ZZ99_SKÅPESUND", " ", False),  # White space
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_validate_weath(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_visit_key,
    given_weather_observation_code,
    expected_success,
):
    # Given data with given visit date
    given_data = pl.DataFrame(
        [
            {
                "visit_key": given_visit_key,
                "weather_observation_code": given_weather_observation_code,
            }
        ]
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateWeath().validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success


@pytest.mark.parametrize(
    "given_visit_key, given_weather_observation_code, given_cloud_observation_code, "
    "expected_success",
    (
        ("20230802_1050_ZZ99_SKÅPESUND", "0", "0", True),
        ("20230802_1050_ZZ99_SKÅPESUND", "0", "1", False),
        ("20230802_1050_ZZ99_SKÅPESUND", "0", "8", False),
        ("20230802_1050_ZZ99_SKÅPESUND", "0", "9", True),
        ("20230530_1115_ZZ99_SMÅHOLMARNA", "1", "0", False),
        ("20230530_1115_ZZ99_SMÅHOLMARNA", "1", "1", True),
        ("20230530_1115_ZZ99_SMÅHOLMARNA", "1", "6", True),
        ("20230530_1115_ZZ99_SMÅHOLMARNA", "1", "7", False),
        ("20230530_1115_ZZ99_SMÅHOLMARNA", "1", "8", False),
        ("20230530_1115_ZZ99_SMÅHOLMARNA", "1", "9", True),
        ("20230802_1050_ZZ99_SKÅPESUND", "2", "1", False),
        ("20230802_1050_ZZ99_SKÅPESUND", "2", "6", False),
        ("20230802_1050_ZZ99_SKÅPESUND", "2", "7", True),
        ("20230802_1050_ZZ99_SKÅPESUND", "2", "8", True),
        ("20230802_1050_ZZ99_SKÅPESUND", "2", "9", True),
        ("20230530_0925_ZZ99_SVENSHOLMEN", "2.0", "8", True),  # Float
        ("20230530_0925_ZZ99_SVENSHOLMEN", "2", "7.0", True),  # Float
        ("20230802_1050_ZZ99_SKÅPESUND", "0", "07", False),  # Zeropadded, cast to int
        ("20230802_1050_ZZ99_SKÅPESUND", "01", "07", False),  # Zeropadded, cast to int
        ("20230802_1050_ZZ99_SKÅPESUND", "", "1", True),  # Missing as str
        ("20230802_1050_ZZ99_SKÅPESUND", "2", None, True),  # Missing as None
        ("20230802_1050_ZZ99_SKÅPESUND", " ", "1", True),  # White space
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_validate_weather_consistency(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_visit_key,
    given_weather_observation_code,
    given_cloud_observation_code,
    expected_success,
):
    # Given data with given visit date
    given_data = pl.DataFrame(
        [
            {
                "visit_key": given_visit_key,
                "weather_observation_code": given_weather_observation_code,
                "cloud_observation_code": given_cloud_observation_code,
            }
        ]
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateWeatherConsistency().validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success
