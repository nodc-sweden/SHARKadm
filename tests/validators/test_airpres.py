from unittest.mock import patch

import polars as pl
import pytest

from sharkadm import adm_logger
from sharkadm.validators.air_pressure import ValidateAirpres


@pytest.mark.parametrize(
    "given_date, given_station, given_air_pressure_hpa, expected_success",
    (
        ("20230802", "SKÅPESUND", "900.00", True),  # Float
        ("20230530", "SMÅHOLMARNA", "1100.1", False),  # Possibly too high
        ("20230530", "SVENSHOLMEN", "1000", True),  # Int
        ("20230802", "SKÅPESUND", "850", False),  # Possibly too low
        ("20230802", "SKÅPESUND", "", True),  # Missing as str
        ("20230802", "SKÅPESUND", None, True),  # Missing as None
        ("20230802", "SKÅPESUND", " ", False),  # White space
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_validate_airpres(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_date,
    given_station,
    given_air_pressure_hpa,
    expected_success,
):
    # Given data with given visit date
    given_data = pl.DataFrame(
        [
            {
                "visit_date": given_date,
                "reported_station_name": given_station,
                "air_pressure_hpa": given_air_pressure_hpa,
            }
        ]
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateAirpres().validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success
