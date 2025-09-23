import datetime
from unittest.mock import patch

import polars as pl
import pyproj
import pytest

from sharkadm import adm_logger
from sharkadm.validators import ValidateSpeed
from sharkadm.validators.speed import approximate_distance


@pytest.mark.parametrize(
    "given_timed_positions, expected_success",
    (
        (
            (("2025-09-11T12:00:00.000000", "56", "18", "10"),),
            True,  # Only one position
        ),
        (
            (
                ("2025-09-11T12:00:00.000000", "56", "18", "10"),
                ("2025-09-11T12:00:00.000000", "56", "18", "10"),
            ),
            True,  # Same position and time is ok
        ),
        (
            (
                ("2025-09-11T12:00:00.000000", "56", "18", "10"),
                ("2025-09-11T13:00:00.000000", "56", "18", "10"),
            ),
            True,  # Same position and time is ok
        ),
        (
            (
                ("2025-09-11T12:00:00.000000", "56", "18", "10"),
                ("2025-09-11T13:00:00.000000", "56.50", "18", "10"),
            ),
            False,  # > 55.56 km north in 1 hour
        ),
        (
            (
                ("2025-09-11T12:00:00.000000", "56", "18", "10"),
                ("2025-09-11T13:00:00.000000", "56.49", "18", "10"),
            ),
            True,  # < 50.56 km north in 1 hour
        ),
        (
            (
                ("2025-09-11T12:00:00.000000", "56", "18", "10"),
                ("2025-09-11T13:00:00.000000", "55.50", "18", "10"),
            ),
            False,  # > 55.56 south in 1 hour
        ),
        (
            (
                ("2025-09-11T12:00:00.000000", "56", "18", "10"),
                ("2025-09-11T13:00:00.000000", "55.51", "18", "10"),
            ),
            True,  # < 55.56 south in 1 hour
        ),
        (
            (
                ("2025-09-11T12:00:00.000000", "56", "18", "10"),
                ("2025-09-11T13:00:00.000000", "56", "18.90", "10"),
            ),
            False,  # > 55.56 km east in 1 hour
        ),
        (
            (
                ("2025-09-11T12:00:00.000000", "56", "18", "10"),
                ("2025-09-11T13:00:00.000000", "56", "18.89", "10"),
            ),
            True,  # < 55.56 km east in 1 hour
        ),
        (
            (
                ("2025-09-11T12:00:00.000000", "56", "18", "10"),
                ("2025-09-11T13:00:00.000000", "56", "17.10", "10"),
            ),
            False,  # > 55.56 km west in 1 hour
        ),
        (
            (
                ("2025-09-11T12:00:00.000000", "56", "18", "10"),
                ("2025-09-11T13:00:00.000000", "56", "17.11", "10"),
            ),
            True,  # < 55.56 km west in 1 hour
        ),
        (
            (
                ("2025-09-11T12:00:00.000000", "56", "18", "10"),
                ("2025-09-11T13:00:00.000000", "56.35", "18.65", "10"),
            ),
            False,  # > 55.56 km north-east in 1 hour
        ),
        (
            (
                ("2025-09-11T12:00:00.000000", "56", "18", "10"),
                ("2025-09-11T13:00:00.000000", "56.34", "18.64", "10"),
            ),
            True,  # < 55.56 km north-east in 1 hour
        ),
        (
            (
                ("2025-09-11T12:00:00.000000", "56", "18", "10"),
                ("2025-09-11T13:00:00.000000", "55.65", "17.35", "10"),
            ),
            False,  # > 55.56 km south-west in 1 hour
        ),
        (
            (
                ("2025-09-11T12:00:00.000000", "56", "18", "10"),
                ("2025-09-11T13:00:00.000000", "55.66", "17.36", "10"),
            ),
            True,  # < 55.56 km south-west in 1 hour
        ),
        (
            (
                ("2025-09-11T22:00:00.000000", "56", "18", "10"),
                ("2025-09-11T23:00:00.000000", "56.2", "18", "10"),
                ("2025-09-12T00:00:00.000000", "56.2", "18.2", "10"),
                ("2025-09-12T01:00:00.000000", "56.8", "18.2", "10"),
                ("2025-09-12T02:00:00.000000", "56.8", "18.4", "10"),
            ),
            False,  # Multiple positions, one bad
        ),
        (
            (
                ("2025-09-11T22:00:00.000000", "56", "18", "10"),
                ("2025-09-11T23:00:00.000000", "56.2", "18", "10"),
                ("2025-09-12T00:00:00.000000", "56.2", "18.2", "10"),
                ("2025-09-12T01:00:00.000000", "56.4", "18.2", "10"),
                ("2025-09-12T02:00:00.000000", "56.4", "18.4", "10"),
            ),
            True,  # Multiple positions, all good
        ),
        (
            (
                ("2025-09-12T00:00:00.000000", "56.2", "18.2", "10"),
                ("2025-09-11T22:00:00.000000", "56", "18", "10"),
                ("2025-09-12T02:00:00.000000", "56.4", "18.4", "10"),
                ("2025-09-11T23:00:00.000000", "56.2", "18", "10"),
                ("2025-09-12T01:00:00.000000", "56.4", "18.2", "10"),
            ),
            True,  # Multiple positions, unordered, all good
        ),
        (
            (
                ("2025-09-12T12:00:00.000000", "55", "13", "10"),
                ("2025-09-12T13:00:00.000000", "55.2", "13", "10"),
                ("2025-09-12T14:00:00.000000", "65", "24", "10"),
                ("2025-09-12T15:00:00.000000", "65.2", "24", "10"),
            ),
            False,  # Distance withing same cruises is tested
        ),
        (
            (
                ("2025-09-12T12:00:00.000000", "55", "13", "10"),
                ("2025-09-12T13:00:00.000000", "55.2", "13", "10"),
                ("2025-09-12T14:00:00.000000", "65", "24", "11"),
                ("2025-09-12T15:00:00.000000", "65.2", "24", "11"),
            ),
            True,  # Distance over different cruises is ignored
        ),
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_speed_between_positions_are_validated_with_simple_data(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_timed_positions,
    expected_success,
):
    # Given data with a position and time
    given_values = [
        {
            "row_number": n,
            "datetime": datetime.datetime.fromisoformat(given_datetime),
            "sample_latitude_dd": given_latitude,
            "sample_longitude_dd": given_longitude,
            "platform_code": given_shipc,
        }
        for n, (
            given_datetime,
            given_latitude,
            given_longitude,
            given_shipc,
        ) in enumerate(given_timed_positions, start=1)
    ]
    given_data = pl.DataFrame(given_values)

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateSpeed().validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success


@patch("sharkadm.config.get_all_data_types")
def test_speed_between_positions_with_valid_detailed_data(
    mocked_data_types, polars_data_frame_holder_class
):
    # Given data with a position and time
    given_values = [
        {
            "row_number": 1,
            "datetime": datetime.datetime.fromisoformat("2025-09-15T12:00:00.000000"),
            "sample_latitude_dd": "56",
            "sample_longitude_dd": "18",
            "parameter": "NTRI",
            "value": "0.5",
            "DEPH": "1.0",
            "platform_code": "10",
        },
        {
            "row_number": 1,
            "datetime": datetime.datetime.fromisoformat("2025-09-15T12:00:00.000000"),
            "sample_latitude_dd": "56",
            "sample_longitude_dd": "18",
            "parameter": "NTRA",
            "value": "4.5",
            "DEPH": "1.0",
            "platform_code": "10",
        },
        {
            "row_number": 2,
            "datetime": datetime.datetime.fromisoformat("2025-09-15T12:00:00.000000"),
            "sample_latitude_dd": "56",
            "sample_longitude_dd": "18",
            "parameter": "NTRI",
            "value": "1.0",
            "DEPH": "5.0",
            "platform_code": "10",
        },
        {
            "row_number": 2,
            "datetime": datetime.datetime.fromisoformat("2025-09-15T12:00:00.000000"),
            "sample_latitude_dd": "56",
            "sample_longitude_dd": "18",
            "parameter": "NTRA",
            "value": "5.0",
            "DEPH": "5.0",
            "platform_code": "10",
        },
        {
            "row_number": 3,
            "datetime": datetime.datetime.fromisoformat("2025-09-15T13:00:00.000000"),
            "sample_latitude_dd": "56.2",
            "sample_longitude_dd": "18",
            "parameter": "NTRI",
            "value": "0.5",
            "DEPH": "1.0",
            "platform_code": "10",
        },
        {
            "row_number": 3,
            "datetime": datetime.datetime.fromisoformat("2025-09-15T13:00:00.000000"),
            "sample_latitude_dd": "56.2",
            "sample_longitude_dd": "18",
            "parameter": "NTRA",
            "value": "4.5",
            "DEPH": "1.0",
            "platform_code": "10",
        },
        {
            "row_number": 4,
            "datetime": datetime.datetime.fromisoformat("2025-09-15T13:00:00.000000"),
            "sample_latitude_dd": "56.2",
            "sample_longitude_dd": "18",
            "parameter": "NTRI",
            "value": "1.0",
            "DEPH": "5.0",
            "platform_code": "10",
        },
        {
            "row_number": 4,
            "datetime": datetime.datetime.fromisoformat("2025-09-15T13:00:00.000000"),
            "sample_latitude_dd": "56.2",
            "sample_longitude_dd": "18",
            "parameter": "NTRA",
            "value": "5.0",
            "DEPH": "5.0",
            "platform_code": "10",
        },
    ]
    given_data = pl.DataFrame(given_values)

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateSpeed().validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"]


@pytest.mark.parametrize(
    "latitude_1, longitude_1, latitude_2, longitude_2",
    (
        (56, 18, 57, 19),
        (57.12, 17.67, 56.12, 16.52),
        (58.34, 11.03, 58.32, 10.94),
        (55, 13, 56, 20),
    ),
)
def test_approximate_distance_against_pyproj_within_one_percent(
    latitude_1, longitude_1, latitude_2, longitude_2
):
    data = pl.DataFrame(
        (
            {"sample_latitude_dd": latitude_1, "sample_longitude_dd": longitude_1},
            {"sample_latitude_dd": latitude_2, "sample_longitude_dd": longitude_2},
        )
    )

    wgs84_g = pyproj.Geod(ellps="WGS84")

    _, _, distance_from_pyproj = wgs84_g.inv(
        longitude_1, latitude_1, longitude_2, latitude_2
    )

    data_with_distances = approximate_distance(data)
    distance_from_haversine = data_with_distances["distance"].last()
    relative_error = (
        distance_from_haversine - distance_from_pyproj
    ) / distance_from_pyproj
    assert abs(relative_error) < 0.01
