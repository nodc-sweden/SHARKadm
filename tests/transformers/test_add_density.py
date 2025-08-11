from unittest.mock import patch

import polars as pl
import pytest

from sharkadm.transformers.add_density import PolarsAddDensity, PolarsAddDensityWide


@pytest.mark.parametrize(
    "given_visit_key, given_latitude, given_longitude, given_depths, "
    "given_salinity_psu, given_temperature, expected_success",
    [
        (
            [
                "ABC123",
                "ABC123",
                "DEF456",
                "DEF456",
                "GHJ789",
                "GHJ789",
                "GHJ789",
                "GHJ789",
                "GHJ789",
            ],
            [58.2, 58.2, 57.1, 57.1, 56.5, 56.5, 56.5, 56.5, None],
            [10.1, 10.1, 11.0, 11.0, 12.0, 12.0, 12.0, 12.0, 12.0],
            [5.0, 10.1, 5.7, 15.2, 5.5, 100.0, 200.1, 500.2, 1000.1],
            [35.01, 35.2, 0, 5.2, 10.3, 20.8, 30.33, 33.2, 35.25],
            [0.1, -1.7, 5.2, 10.1, 10.1, 34, None, 20.1, 15.2],
            [True, True, True, True, True, True, False, True, False],
        ),
    ],
)
@patch("sharkadm.config.get_all_data_types", return_value=[])
def test_validate_add_density_wide(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_visit_key,
    given_latitude,
    given_longitude,
    given_depths,
    given_salinity_psu,
    given_temperature,
    expected_success,
):
    # Arrange
    given_data = pl.DataFrame(
        {
            "visit_key": given_visit_key,
            "sample_latitude_dd": given_latitude,
            "sample_longitude_dd": given_longitude,
            "sample_depth_m": given_depths,
            "Salinity CTD": given_salinity_psu,
            "Temperature CTD": given_temperature,
        }
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    assert "in_situ_density" not in given_data_holder.data.columns, (
        "Density column not added yet"
    )

    # Transforming the data
    PolarsAddDensityWide().transform(given_data_holder)

    assert "in_situ_density" in given_data_holder.data.columns, (
        "Density column was not added"
    )

    result = given_data_holder.data

    for i, success_expected in enumerate(expected_success):
        density_value = result["in_situ_density"][i]
        if success_expected:
            assert density_value is not None, f"Expected density at row {i}, but got None"
        else:
            assert density_value is None, (
                f"Expected None at row {i}, but got value {density_value}"
            )


@pytest.mark.parametrize(
    "given_visit_key, given_latitude, given_longitude, given_depths, "
    "given_parameter, given_value, expected_success",
    [
        (
            [
                "ABC123",
                "ABC123",
                "DEF456",
                "DEF456",
                "GHJ789",
                "GHJ789",
                "GHJ789",
                "GHJ789",
            ],
            [58.2, 58.2, 57.1, 57.1, 56.5, 56.5, 56.5, 56.5],
            [10.1, 10.1, 11.0, 11.0, 12.0, 12.0, 12.0, 12.0],
            [5.0, 5.0, 5.7, 5.7, 10, 10, 20, 20],
            [
                "Salinity CTD",
                "Temperature CTD",
                "Salinity CTD",
                "Temperature CTD",
                "Salinity CTD",
                "Temperature CTD",
                "Salinity CTD",
                "Temperature CTD",
            ],
            [5.0, -1.7, 35, 10.1, 15, 20, None, 20.1],
            [True, True, True, True, True, True, False, False],
        ),
    ],
)
@patch("sharkadm.config.get_all_data_types", return_value=[])
def test_validate_add_density(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_visit_key,
    given_latitude,
    given_longitude,
    given_depths,
    given_parameter,
    given_value,
    expected_success,
):
    # Arrange
    given_data = pl.DataFrame(
        {
            "visit_key": given_visit_key,
            "sample_latitude_dd": given_latitude,
            "sample_longitude_dd": given_longitude,
            "sample_depth_m": given_depths,
            "parameter": given_parameter,
            "value": given_value,
        }
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    assert "in_situ_density" not in given_data_holder.data.columns, (
        "Density column not added yet"
    )

    # Transforming the data
    PolarsAddDensity().transform(given_data_holder)

    assert "in_situ_density" in given_data_holder.data.columns, (
        "Density column was not added"
    )

    result = given_data_holder.data

    for i, success_expected in enumerate(expected_success):
        density_value = result["in_situ_density"][i]
        if success_expected:
            assert density_value is not None, f"Expected density at row {i}, but got None"
        else:
            assert density_value is None, (
                f"Expected None at row {i}, but got value {density_value}"
            )
