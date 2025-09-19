from unittest.mock import patch

import polars as pl
import pytest

from sharkadm import adm_logger
from sharkadm.validators import ValidateSpeed


@pytest.mark.parametrize(
    "given_timed_positions, expected_success",
    (
        (
            (("1", "2025-09-11", "12:00:00", "6200000", "700000"),),
            True,  # Only one position
        ),
        (
            (
                ("1", "2025-09-11", "12:00:00", "6200000", "700000"),
                ("2", "2025-09-11", "12:00:00", "6200000", "700000"),
            ),
            False,  # Same position and time is not ok for different visits
        ),
        (
            (
                ("1", "2025-09-11", "12:00:00", "6200000", "700000"),
                ("1", "2025-09-11", "12:00:00", "6200000", "700000"),
            ),
            True,  # Same position and time is ok for same visit
        ),
        (
            (
                ("1", "2025-09-11", "12:00:00", "6200000", "700000"),
                ("2", "2025-09-11", "13:00:00", "6200000", "700000"),
            ),
            False,  # Two visits on the same position is interpreted as too slow
        ),
        (
            (
                ("1", "2025-09-11", "12:00:00", "6200000", "700000"),
                ("2", "2025-09-11", "13:00:00", "6255561", "700000"),
            ),
            False,  # 55.561 km north in 1 hour
        ),
        (
            (
                ("1", "2025-09-11", "12:00:00", "6200000", "700000"),
                ("2", "2025-09-11", "13:00:00", "6255560", "700000"),
            ),
            True,  # 50.560 km north in 1 hour
        ),
        (
            (
                ("1", "2025-09-11", "12:00:00", "6200000", "700000"),
                ("2", "2025-09-11", "13:00:00", "6144439", "700000"),
            ),
            False,  # 55.561 south in 1 hour
        ),
        (
            (
                ("1", "2025-09-11", "12:00:00", "6200000", "700000"),
                ("2", "2025-09-11", "13:00:00", "6144440", "700000"),
            ),
            True,  # 55.56 south in 1 hour
        ),
        (
            (
                ("1", "2025-09-11", "12:00:00", "6200000", "700000"),
                ("2", "2025-09-11", "13:00:00", "6200000", "755561"),
            ),
            False,  # 55.561 km east in 1 hour
        ),
        (
            (
                ("1", "2025-09-11", "12:00:00", "6200000", "700000"),
                ("2", "2025-09-11", "13:00:00", "6200000", "755560"),
            ),
            True,  # 55.56 km east in 1 hour
        ),
        (
            (
                ("1", "2025-09-11", "12:00:00", "6200000", "700000"),
                ("2", "2025-09-11", "13:00:00", "6200000", "644439"),
            ),
            False,  # 55.561 km west in 1 hour
        ),
        (
            (
                ("1", "2025-09-11", "12:00:00", "6200000", "700000"),
                ("2", "2025-09-11", "13:00:00", "6200000", "644440"),
            ),
            True,  # 55.56 km west in 1 hour
        ),
        (
            (
                ("1", "2025-09-11", "12:00:00", "6200000", "700000"),
                ("2", "2025-09-11", "13:00:00", "6239287", "739287"),
            ),
            False,  # 55.561 km north-east in 1 hour
        ),
        (
            (
                ("1", "2025-09-11", "12:00:00", "6200000", "700000"),
                ("2", "2025-09-11", "13:00:00", "6239286", "739286"),
            ),
            True,  # 55.56 km north-east in 1 hour
        ),
        (
            (
                ("1", "2025-09-11", "12:00:00", "6200000", "700000"),
                ("2", "2025-09-11", "13:00:00", "6160713", "660713"),
            ),
            False,  # 55.561 km south-west in 1 hour
        ),
        (
            (
                ("1", "2025-09-11", "12:00:00", "6200000", "700000"),
                ("2", "2025-09-11", "13:00:00", "6160714", "660714"),
            ),
            True,  # 55.56 km south-west in 1 hour
        ),
        (
            (
                ("1", "2025-09-11", "12:00:00", "6200000", "700000"),
                ("2", "2025-09-11", "13:00:00", "6203703", "700000"),
            ),
            False,  # 3.703 km north in 1 hour
        ),
        (
            (
                ("1", "2025-09-11", "12:00:00", "6200000", "700000"),
                ("2", "2025-09-11", "13:00:00", "6203704", "700000"),
            ),
            True,  # 3.704 km north in 1 hour
        ),
        (
            (
                ("1", "2025-09-11", "22:00:00", "6200000", "700000"),
                ("2", "2025-09-11", "23:00:00", "6210000", "700000"),
                ("3", "2025-09-12", "00:00:00", "6210000", "800000"),
                ("4", "2025-09-12", "01:00:00", "6220000", "800000"),
                ("5", "2025-09-12", "02:00:00", "6220000", "810000"),
            ),
            False,  # Multiple positions, one bad
        ),
        (
            (
                ("1", "2025-09-11", "22:00:00", "6200000", "700000"),
                ("2", "2025-09-11", "23:00:00", "6210000", "700000"),
                ("3", "2025-09-12", "00:00:00", "6210000", "710000"),
                ("4", "2025-09-12", "01:00:00", "6220000", "710000"),
                ("5", "2025-09-12", "02:00:00", "6220000", "720000"),
            ),
            True,  # Multiple positions, all good
        ),
        (
            (
                ("3", "2025-09-12", "01:00:00", "6210000", "710000"),
                ("1", "2025-09-11", "23:00:00", "6200000", "700000"),
                ("2", "2025-09-12", "00:00:00", "6210000", "700000"),
                ("2", "2025-09-12", "00:00:00", "6210000", "700000"),
                ("3", "2025-09-12", "01:00:00", "6210000", "710000"),
                ("1", "2025-09-11", "23:00:00", "6200000", "700000"),
            ),
            True,  # Multiple positions, unordered, all good
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
            "visit_date": date,
            "visit_id": visit_id,
            "sample_time": time,
            "LATIT": latitude,
            "LONGI": longitude,
        }
        for n, (visit_id, date, time, latitude, longitude) in enumerate(
            given_timed_positions, start=1
        )
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
            "visit_id": "1",
            "visit_date": "2025-09-15",
            "sample_time": "12:00:00",
            "LATIT": "6200000",
            "LONGI": "700000",
            "parameter": "NTRI",
            "value": "0.5",
            "DEPH": "1.0",
        },
        {
            "row_number": 1,
            "visit_id": "1",
            "visit_date": "2025-09-15",
            "sample_time": "12:00:00",
            "LATIT": "6200000",
            "LONGI": "700000",
            "parameter": "NTRA",
            "value": "4.5",
            "DEPH": "1.0",
        },
        {
            "row_number": 2,
            "visit_id": "1",
            "visit_date": "2025-09-15",
            "sample_time": "12:00:00",
            "LATIT": "6200000",
            "LONGI": "700000",
            "parameter": "NTRI",
            "value": "1.0",
            "DEPH": "5.0",
        },
        {
            "row_number": 2,
            "visit_id": "1",
            "visit_date": "2025-09-15",
            "sample_time": "12:00:00",
            "LATIT": "6200000",
            "LONGI": "700000",
            "parameter": "NTRA",
            "value": "5.0",
            "DEPH": "5.0",
        },
        {
            "row_number": 3,
            "visit_id": "2",
            "visit_date": "2025-09-15",
            "sample_time": "13:00:00",
            "LATIT": "6210000",
            "LONGI": "700000",
            "parameter": "NTRI",
            "value": "0.5",
            "DEPH": "1.0",
        },
        {
            "row_number": 3,
            "visit_id": "2",
            "visit_date": "2025-09-15",
            "sample_time": "13:00:00",
            "LATIT": "6210000",
            "LONGI": "700000",
            "parameter": "NTRA",
            "value": "4.5",
            "DEPH": "1.0",
        },
        {
            "row_number": 4,
            "visit_id": "2",
            "visit_date": "2025-09-15",
            "sample_time": "13:00:00",
            "LATIT": "6210000",
            "LONGI": "700000",
            "parameter": "NTRI",
            "value": "1.0",
            "DEPH": "5.0",
        },
        {
            "row_number": 4,
            "visit_id": "2",
            "visit_date": "2025-09-15",
            "sample_time": "13:00:00",
            "LATIT": "6210000",
            "LONGI": "700000",
            "parameter": "NTRA",
            "value": "5.0",
            "DEPH": "5.0",
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
