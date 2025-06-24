from unittest.mock import patch

import pandas as pd
import pytest

from sharkadm import adm_logger
from sharkadm.validators.station.position_within_station_radius import (
    ValidatePositionWithinStationRadius,
)


@pytest.mark.parametrize(
    "given_station_name, given_longitude_value, given_latitude_value, "
    "given_stations, expected_success",
    (
        (
            "Station x",
            "735000",
            "6500000",
            (),  # No stations
            False,
        ),
        (
            "Station x",
            "735000",
            "6500000",
            (
                ("Station 1", "735000", "6500000", 1000),  # Bull's eye
            ),
            True,
        ),
        (
            "Station x",
            "735000",
            "6500000",
            (("Station 1", "750000", "7000000", 1000),),
            False,
        ),
        (
            "Station x",
            "735000",
            "6500000",
            (
                ("Station 1", "750000", "7000000", 1000),
                ("Station 2", "735000", "6500000", 1000),  # Matching one of two
            ),
            True,
        ),
        (
            "Station x",
            "735000",
            "6500000",
            (
                ("Station 1", "750000", "7000000", 1000),
                ("Station 2", "735500", "6500000", 1000),  # Matching multiple
                ("Station 3", "735000", "6500500", 1000),  # Matching multiple
            ),
            True,
        ),
        (
            "Station x",
            "735000",
            "6500000",
            (
                ("Station 1", "735999.999", "6500000", 1000),  # Just inside the radius
            ),
            True,
        ),
        (
            "Station x",
            "735000",
            "6500000",
            (
                ("Station 1", "736000", "6500000", 1000),  # Just outside the radius
            ),
            False,
        ),
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_validate_position_within_station_radius(
    mocked_data_types,
    pandas_data_frame_holder_class,
    given_station_name,
    given_longitude_value,
    given_latitude_value,
    given_stations,
    expected_success,
):
    # Given data with given coordinates
    given_data = pd.DataFrame(
        [
            {
                "statn": given_station_name,
                "LATIT": given_latitude_value,
                "LONGI": given_longitude_value,
                "row_number": 1,
            }
        ]
    )

    # Given a valid data holder
    given_data_holder = pandas_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidatePositionWithinStationRadius(given_stations).validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success
