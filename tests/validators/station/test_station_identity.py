from unittest.mock import patch

import pandas as pd
import pytest

from sharkadm import adm_logger
from sharkadm.validators.station.station_identity import ValidateStationIdentity


@pytest.mark.parametrize(
    "given_station_name, given_longitude_value, given_latitude_value, "
    "given_known_stations, expected_success",
    (
        (
            "name x",
            "751000",
            "7010000",
            (("name y", {"synonym y"}, "750000", "7000000", 1000),),
            False,
        ),  # Nothing matches
        (
            "name x",
            "750000",
            "7000000",
            (("name x", {"synonym x"}, "750000", "7000000", 1000),),
            True,
        ),  # Correct name and position
        (
            "synonym x",
            "750000",
            "7000000",
            (("name x", {"synonym x"}, "750000", "7000000", 1000),),
            True,
        ),  # Correct synonym and position
        (
            "name x",
            "751000",
            "7010000",
            (("name x", {"synonym x"}, "750000", "7000000", 1000),),
            False,
        ),  # Correct name, wrong position
        (
            "synonym x",
            "751000",
            "7010000",
            (("name x", {"synonym x"}, "750000", "7000000", 1000),),
            False,
        ),  # Correct synonym, wrong position
        (
            "name x",
            "750000",
            "7000000",
            (
                ("name y", {"synonym y"}, "751000", "7001000", 1000),
                ("name x", {"synonym x"}, "750000", "7000000", 1000),
            ),
            True,
        ),  # Multiple stations, one matches name and position
        (
            "name x",
            "750000",
            "7000000",
            (
                ("name y", {"synonym y"}, "750000", "7000000", 1000),
                ("name x", {"synonym x"}, "751000", "7001000", 1000),
            ),
            False,
        ),  # Multiple stations, one matches name, one matches position
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_validate_station_identity(
    mocked_data_types,
    pandas_data_frame_holder_class,
    given_station_name,
    given_longitude_value,
    given_latitude_value,
    given_known_stations,
    expected_success,
):
    # Given data with a given station name and position
    given_data = pd.DataFrame(
        [
            {
                "reported_station_name": given_station_name,
                "LATIT": given_latitude_value,
                "LONGI": given_longitude_value,
                "visit_key": "visit_1",
                "row_number": 1,
            }
        ]
    )

    # Given a valid data holder
    given_data_holder = pandas_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateStationIdentity(given_known_stations).validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success
