from unittest.mock import patch

import polars as pl
import pytest

from sharkadm.transformers.add_pressure import PolarsAddPressure


@pytest.mark.parametrize(
    "given_visit_key, given_latitude, given_longitude, given_depths, expected_success",
    (
        ("ABC123", 58.2, 10.1, 5.0, True),  # ok data
        ("DEF456", 57.1, 11.0, 5.7, True),  # ok data
        ("GHJ789", 56.5, 12.0, -100, False),  # erroneous depth
    ),
)
@patch("sharkadm.config.get_all_data_types", return_value=[])
def test_validate_add_pressure(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_visit_key,
    given_latitude,
    given_longitude,
    given_depths,
    expected_success,
):
    # Arrange
    given_data = pl.DataFrame(
        {
            "visit_key": given_visit_key,
            "sample_latitude_dd": given_latitude,
            "sample_longitude_dd": given_longitude,
            "sample_depth_m": given_depths,
        }
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # There should be no column with pressure
    # before application of transformer
    assert "pressure" not in given_data_holder.data.columns, (
        "Pressure column not added yet"
    )

    # Transforming the data
    PolarsAddPressure().transform(given_data_holder)

    # After transformation the pressure column
    # should exist
    assert "pressure" in given_data_holder.data.columns, "Pressure column was not added"

    pressure_value = given_data_holder.data["pressure"][0]

    # The calculated pressure will either be a float
    # or None if in-data are incorrect
    if expected_success:
        assert pressure_value is not None, "Expected pressure, but got None"
    else:
        assert pressure_value is None, f"Expected None, but got value {pressure_value}"
