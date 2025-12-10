from unittest.mock import patch

import polars as pl
import pytest

from sharkadm.transformers.add_gsw_parameters import (
    PolarsAddOxygenSaturation,
    PolarsAddOxygenSaturationWide,
)


@pytest.mark.parametrize(
    "given_latitude, given_longitude, given_depth, given_salinity,"
    " given_temperature, given_oxygen, given_density, expected_o2_at_sat, "
    "expected_o2_sat",
    (
        (58.34, 11.03, 102, 34.56, 8.27, 6.4, 1027.3593565, 6.583, 97.226),
        (58.26, 11.44, 1, 26.74, 4.58, 7.4, 1021.194037235, 7.551, 97.994),
    ),
)
@patch(
    "sharkadm.transformers.base.PolarsTransformer.is_valid_data_holder", return_value=True
)
def test_validate_add_oxygen_wide(
    mocked_valid_data_holder,
    polars_data_frame_holder_class,
    given_latitude,
    given_longitude,
    given_depth,
    given_salinity,
    given_temperature,
    given_oxygen,
    given_density,
    expected_o2_at_sat,
    expected_o2_sat,
):
    # Arrange
    given_data = pl.DataFrame(
        {
            "sample_latitude_dd": given_latitude,
            "sample_longitude_dd": given_longitude,
            "sample_depth_m": given_depth,
            "COPY_VARIABLE.Salinity CTD.o/oo psu": given_salinity,
            "COPY_VARIABLE.Temperature CTD.C": given_temperature,
            "COPY_VARIABLE.Dissolved oxygen O2 CTD.ml/l": given_oxygen,
            "COPY_VARIABLE.Derived in situ density CTD.kg/m3": given_density,
        }
    )
    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)

    # There should be no columns with derived oxygen
    # before application of transformer
    assert (
        "COPY_VARIABLE.Derived oxygen at saturation CTD.ml/l"
        not in given_data_holder.data.columns
    ), "Oxygen at saturation column already exist"
    assert (
        "COPY_VARIABLE.Derived oxygen saturation CTD.%"
        not in given_data_holder.data.columns
    ), "Oxygen saturation column already exist"

    print(given_data_holder.data.columns)

    PolarsAddOxygenSaturationWide("CTD").transform(given_data_holder)

    print(given_data_holder.data.columns)

    # After transformation the derived oxygen columns
    # should exist
    assert (
        "COPY_VARIABLE.Derived oxygen at saturation CTD.ml/l"
        in given_data_holder.data.columns
    ), "Oxygen at saturation was not added"

    assert (
        "COPY_VARIABLE.Derived oxygen saturation CTD.%" in given_data_holder.data.columns
    ), "Oxygen saturation was not added"

    oxygen_at_sat_value = given_data_holder.data[
        "COPY_VARIABLE.Derived oxygen at saturation CTD.ml/l"
    ][0]
    # The calculated oxygen at saturation should match the expected value
    assert round(oxygen_at_sat_value, 3) == expected_o2_at_sat, (
        "Oxygen at saturation does not match the expected value"
    )

    oxygen_sat_value = given_data_holder.data[
        "COPY_VARIABLE.Derived oxygen saturation CTD.%"
    ][0]
    # The calculated oxygen at saturation should match the expected value
    assert round(oxygen_sat_value, 3) == expected_o2_sat, (
        "Oxygen saturation does not match the expected value"
    )


@pytest.mark.parametrize(
    "given_visit_key, given_latitude, given_longitude, given_depth, "
    "given_parameter, given_value, given_density, "
    "expected_o2_at_saturation, expected_o2_saturation",
    (
        (
            ["ABC123", "ABC123", "ABC123"],
            [57.63, 57.63, 57.63],
            [
                11.77,
                11.77,
                11.77,
            ],
            [0.5, 0.5, 0.5],
            ["Salinity CTD", "Temperature CTD", "Dissolved oxygen O2 CTD"],
            [17.48, 14.77, 11.3],
            [1012.591539, 1012.591539, 1012.591539],
            [6.369676, 6.369676, 6.369676],
            [177.40305, 177.40305, 177.40305],
        ),  # ok data
        (
            ["DEF456", "DEF456", "DEF456"],
            [57.58, 57.58, 57.58],
            [11.91, 11.91, 11.91],
            [0.5, 0.5, 0.5],
            ["Salinity CTD", "Temperature CTD", "Dissolved oxygen O2 CTD"],
            [20.65, 15.00, 7.52],
            [1014.973459, 1014.973459, 1014.973459],
            [6.217121, 6.217121, 6.217121],
            [120.9563, 120.9563, 120.9563],
        ),  # ok data
        (
            ["DEF456", "DEF456", "DEF456"],
            [57.58, 57.58, 57.58],
            [11.91, 11.91, 11.91],
            [5, 5, 5],
            ["Salinity CTD", "Temperature CTD", "Dissolved oxygen O2 CTD"],
            [-5.2, 13.16, 7.2],
            [1017.492126, 1017.492126, 1017.492126],
            [None, None, None],
            [None, None, None],
        ),  # erroneous salinity
    ),
)
@patch("sharkadm.config.get_all_data_types", return_value=[])
def test_validate_add_oxygen(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_visit_key,
    given_latitude,
    given_longitude,
    given_depth,
    given_parameter,
    given_value,
    given_density,
    expected_o2_at_saturation,
    expected_o2_saturation,
):
    # Arrange
    given_data = pl.DataFrame(
        {
            "visit_key": given_visit_key,
            "sample_latitude_dd": given_latitude,
            "sample_longitude_dd": given_longitude,
            "sample_depth_m": given_depth,
            "parameter": given_parameter,
            "value": given_value,
            "Derived in situ density CTD": given_density,
        }
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # There should be no column with in situ density
    # before application of transformer
    for col in ["Derived oxygen at saturation CTD", "Derived oxygen saturation CTD"]:
        assert col not in given_data_holder.data.columns, (
            f"Column for calculated {col} exists before transformation"
        )

    print(given_data_holder.data.columns)
    # Transforming the data
    PolarsAddOxygenSaturation("CTD").transform(given_data_holder)
    print(given_data_holder.data.columns)

    # After transformation the calculated oxygen properties
    # should exist
    for col in ["Derived oxygen at saturation CTD", "Derived oxygen saturation CTD"]:
        assert col in given_data_holder.data.columns, (
            f"Column for calculated {col} was not added"
        )

    oxygen_value = given_data_holder.data["Derived oxygen at saturation CTD"][0]
    oxygen_saturation_value = given_data_holder.data["Derived oxygen saturation CTD"][0]

    # The calculated oxygen at saturation should match the expected value
    if expected_o2_at_saturation[0]:
        assert round(oxygen_value, 3) == round(expected_o2_at_saturation[0], 3), (
            "The added value to the column differs from the expected value"
        )
    else:
        assert oxygen_value is None, f"Expected None, but got value {oxygen_value}"

    # The oxygen saturation of the water body should match the expected value
    if expected_o2_saturation[0]:
        assert round(oxygen_saturation_value, 3) == round(expected_o2_saturation[0], 3), (
            "The added value to the column differs from the expected value"
        )
    else:
        assert oxygen_saturation_value is None, (
            f"Expected None, but got value {oxygen_saturation_value}"
        )
