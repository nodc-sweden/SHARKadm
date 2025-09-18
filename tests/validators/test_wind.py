from unittest.mock import patch

import polars as pl
import pytest

from sharkadm import adm_logger
from sharkadm.validators.wind import ValidateWindir


@pytest.mark.parametrize(
    "given_visit_key, given_wind_direction_code, expected_success",
    (
        ("20230802_1050_ZZ99_SKÅPESUND", "00", True),
        ("20230530_1115_ZZ99_SMÅHOLMARNA", "37", False),  # Incorrect direction
        ("20230530_0925_ZZ99_SVENSHOLMEN", "08", True),  # zero padding before
        ("20230802_1050_ZZ99_SKÅPESUND", "99", True),
        ("20230802_1050_ZZ99_SKÅPESUND", "1", True),
        ("20230802_1050_ZZ99_SKÅPESUND", "", True),
        ("20230802_1050_ZZ99_SKÅPESUND", None, True),
        ("20230802_1050_ZZ99_SKÅPESUND", " ", False),
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_validate_windir(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_visit_key,
    given_wind_direction_code,
    expected_success,
):
    # Given data with given visit date
    given_data = pl.DataFrame(
        [
            {
                "visit_key": given_visit_key,
                "wind_direction_code": given_wind_direction_code,
            }
        ]
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateWindir().validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success
