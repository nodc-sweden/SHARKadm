from unittest.mock import patch

import polars as pl
import pytest

from sharkadm import adm_logger
from sharkadm.validators.waves import ValidateWaves


@pytest.mark.parametrize(
    "given_visit_date, given_station, given_wave_observation_code, expected_success",
    (
        ("20230802", "SKÅPESUND", "0", True),
        ("20230802", "SKÅPESUND", "9", True),
        ("20230530", "SMÅHOLMARNA", "10", False),
        ("20230802", "SKÅPESUND", "5", True),
        ("20230530", "SVENSHOLMEN", "7.0", False),  # Float
        ("20230802", "SKÅPESUND", "07", False),  # Zeropadded
        ("20230802", "SKÅPESUND", "", True),  # Missing as str
        ("20230802", "SKÅPESUND", None, True),  # Missing as None
        ("20230802", "SKÅPESUND", " ", False),  # White space
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_validate_waves(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_visit_date,
    given_station,
    given_wave_observation_code,
    expected_success,
):
    # Given data with given visit date
    given_data = pl.DataFrame(
        [
            {
                "visit_date": given_visit_date,
                "reported_station_name": given_station,
                "wave_observation_code": given_wave_observation_code,
            }
        ]
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateWaves().validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success
