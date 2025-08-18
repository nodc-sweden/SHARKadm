from unittest.mock import patch

import numpy as np
import pandas as pd
import polars as pl
import pytest

from sharkadm.transformers.add_lmqnt import AddLmqnt, PolarsAddLmqnt


@pytest.mark.parametrize(
    "given_visit_key, given_latitude, given_longitude, given_depth, "
    "given_parameter, given_value, given_unit, given_quantification_limit, "
    "expected_value",
    (
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Temperature bottle",
            10.2,
            "C",
            "(-3) °C",
            -3,
        ),  # example TEMP_BTL from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Temperature CTD",
            10.2,
            "C",
            "(-5 °C)",
            -5,
        ),  # example TEMP_CTD from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Salinity bottle",
            5.2,
            "o/oo psu",
            "2",
            2,
        ),  # example SALT_BTL from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Salinity CTD",
            35.1,
            "o/oo psu",
            "2",
            2,
        ),  # example SALT_CTD from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Dissolved oxygen O2 bottle",
            5.2,
            "ml/l",
            "0.15 ml/L",
            0.15,
        ),  # example DOXY_BTL from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Dissolved oxygen O2 CTD",
            35.1,
            "ml/l",
            "",
            np.nan,
        ),  # example DOXY_CTD from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Dissolved oxygen O2 CTD",
            35.1,
            "ml/l",
            None,
            np.nan,
        ),  # example DOXY_CTD from UMSC
        ("ABC123", 57.3, 10.1, 5.0, "pH", 7.82, "", "7", 7),  # example PH from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Alkalinity",
            2.301,
            "mmol/kg",
            "0.020 mmol/kg",
            0.02,
        ),  # example ALKY from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Phosphate PO4-P",
            1.01,
            "umol/l",
            "0.4 µg/L",
            0.012914,
        ),  # example PHOS from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Total phosphorus Tot-P",
            1.2,
            "umol/l",
            "0.7 µg/L",
            0.0225995,
        ),  # example PTOT from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Nitrite NO2-N",
            0.05,
            "umol/l",
            "0.3 µg/L",
            0.0214182,
        ),  # example NTRI from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Nitrate NO3-N",
            5.7,
            "umol/l",
            "0.6 µg/L",
            0.0428364,
        ),  # example NTRA from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Ammonium NH4-N",
            2.1,
            "umol/l",
            "0.9 µg/L",
            0.0642546,
        ),  # example AMON from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Total Nitrogen Tot-N",
            7.3,
            "umol/l",
            "1.5 µg/L",
            0.107091,
        ),  # example NTOT from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Silicate SiO3-Si",
            6.7,
            "umol/l",
            "10 µg/L",
            0.35606,
        ),  # example SIO2 from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Humus",
            3.2,
            "ug/l",
            "0,4 µg/L kininsulfat",
            0.4,
        ),  # example HUMUS from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Dissolved organic carbon DOC",
            57.2,
            "umol/l",
            "0.13 mg/L",
            10.82341,
        ),  # example DOC from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Chlorophyll-a bottle",
            4.7,
            "ug/l",
            "0.1 µg/l",
            0.1,
        ),  # example CPHL from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Coloured dissolved organic matter CDOM",
            3.2,
            "1/m",
            "",
            np.nan,
        ),  # example CDOM from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Coloured dissolved organic matter CDOM",
            3.2,
            "1/m",
            None,
            np.nan,
        ),  # example CDOM from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Turbidity TURB",
            2.2,
            "FNU",
            "0.01 FNU",
            0.01,
        ),  # example TURB from UMSC
    ),
)
@patch("sharkadm.config.get_valid_data_types", return_value=[])
def test_validate_add_lmqnt(
    mocked_data_types,
    pandas_data_frame_holder_class,
    given_visit_key,
    given_latitude,
    given_longitude,
    given_depth,
    given_parameter,
    given_value,
    given_unit,
    given_quantification_limit,
    expected_value,
):
    # Arrange
    given_data = pd.DataFrame(
        {
            "visit_key": [given_visit_key],
            "sample_latitude_dd": [given_latitude],
            "sample_longitude_dd": [given_longitude],
            "sample_depth_m": [given_depth],
            "parameter": [given_parameter],
            "value": [given_value],
            "unit": [given_unit],
            "quantification_limit": [given_quantification_limit],
        }
    )

    # Given a valid data holder
    given_data_holder = pandas_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (
        given_data_holder.data_type_internal,
        given_data_holder.data_type,
    )

    # There should be no column "LMQNT_VAL" in the dataframe
    # before application of transformer
    assert "LMQNT_VAL" not in given_data_holder.data.columns, (
        "The column LMQNT_VAL already exist."
    )

    # Transforming the data
    AddLmqnt().transform(given_data_holder)

    # After transformation there should be no nan values
    # in the dataframe
    assert "LMQNT_VAL" in given_data_holder.data.columns, (
        "The column LMQNT_VAL was not added to the dataframe"
    )

    # The value in the kolumn LMQNT_VAL should match the expected value
    if given_data_holder.data["LMQNT_VAL"][0] and not np.isnan(
        given_data_holder.data["LMQNT_VAL"][0]
    ):
        assert round(given_data_holder.data["LMQNT_VAL"][0], 5) == round(
            expected_value, 5
        ), "The added value to the column LMQNT_VAL differs from the expected value"
    elif np.isnan(given_data_holder.data["LMQNT_VAL"][0]):
        assert np.isnan(expected_value), (
            "The added value to the column LMQNT_VAL differs from the expected value"
        )
    else:
        assert expected_value is None, (
            "The added value to the column LMQNT_VAL differs from the expected value"
        )


@pytest.mark.parametrize(
    "given_visit_key, given_latitude, given_longitude, given_depth, "
    "given_parameter, given_value, given_unit, given_quantification_limit, "
    "expected_value",
    (
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Temperature bottle",
            10.2,
            "C",
            "(-3) °C",
            -3,
        ),  # example TEMP_BTL from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Temperature CTD",
            10.2,
            "C",
            "(-5 °C)",
            -5,
        ),  # example TEMP_CTD from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Salinity bottle",
            5.2,
            "o/oo psu",
            "2",
            2,
        ),  # example SALT_BTL from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Salinity CTD",
            35.1,
            "o/oo psu",
            "2",
            2,
        ),  # example SALT_CTD from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Dissolved oxygen O2 bottle",
            5.2,
            "ml/l",
            "0.15 ml/L",
            0.15,
        ),  # example DOXY_BTL from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Dissolved oxygen O2 CTD",
            35.1,
            "ml/l",
            "",
            None,
        ),  # example DOXY_CTD from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Dissolved oxygen O2 CTD",
            35.1,
            "ml/l",
            None,
            None,
        ),  # example DOXY_CTD from UMSC
        ("ABC123", 57.3, 10.1, 5.0, "pH", 7.82, "", "7", 7),  # example PH from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Alkalinity",
            2.301,
            "mmol/kg",
            "0.020 mmol/kg",
            0.02,
        ),  # example ALKY from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Phosphate PO4-P",
            1.01,
            "umol/l",
            "0.4 µg/L",
            0.012914,
        ),  # example PHOS from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Total phosphorus Tot-P",
            1.2,
            "umol/l",
            "0.7 µg/L",
            0.0225995,
        ),  # example PTOT from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Nitrite NO2-N",
            0.05,
            "umol/l",
            "0.3 µg/L",
            0.0214182,
        ),  # example NTRI from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Nitrate NO3-N",
            5.7,
            "umol/l",
            "0.6 µg/L",
            0.0428364,
        ),  # example NTRA from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Ammonium NH4-N",
            2.1,
            "umol/l",
            "0.9 µg/L",
            0.0642546,
        ),  # example AMON from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Total Nitrogen Tot-N",
            7.3,
            "umol/l",
            "1.5 µg/L",
            0.107091,
        ),  # example NTOT from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Silicate SiO3-Si",
            6.7,
            "umol/l",
            "10 µg/L",
            0.35606,
        ),  # example SIO2 from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Humus",
            3.2,
            "ug/l",
            "0,4 µg/L kininsulfat",
            0.4,
        ),  # example HUMUS from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Dissolved organic carbon DOC",
            57.2,
            "umol/l",
            "0.13 mg/L",
            10.82341,
        ),  # example DOC from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Chlorophyll-a bottle",
            4.7,
            "ug/l",
            "0.1 µg/l",
            0.1,
        ),  # example CPHL from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Coloured dissolved organic matter CDOM",
            3.2,
            "1/m",
            "",
            None,
        ),  # example CDOM from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Coloured dissolved organic matter CDOM",
            3.2,
            "1/m",
            None,
            None,
        ),  # example CDOM from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Turbidity TURB",
            2.2,
            "FNU",
            "0.01 FNU",
            0.01,
        ),  # example TURB from UMSC
    ),
)
@patch("sharkadm.config.get_valid_data_types", return_value=[])
def test_validate_polars_add_lmqnt(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_visit_key,
    given_latitude,
    given_longitude,
    given_depth,
    given_parameter,
    given_value,
    given_unit,
    given_quantification_limit,
    expected_value,
):
    # Arrange
    given_data = pl.DataFrame(
        {
            "visit_key": [given_visit_key],
            "sample_latitude_dd": [given_latitude],
            "sample_longitude_dd": [given_longitude],
            "sample_depth_m": [given_depth],
            "parameter": [given_parameter],
            "value": [given_value],
            "unit": [given_unit],
            "quantification_limit": [given_quantification_limit],
        }
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (
        given_data_holder.data_type_internal,
        given_data_holder.data_type,
    )

    # There should be no column "LMQNT_VAL" in the dataframe
    # before application of transformer
    assert "LMQNT_VAL" not in given_data_holder.data.columns, (
        "The column LMQNT_VAL already exist."
    )

    # Transforming the data
    PolarsAddLmqnt().transform(given_data_holder)

    # After transformation there should be no nan values
    # in the dataframe
    assert "LMQNT_VAL" in given_data_holder.data.columns, (
        "The column LMQNT_VAL was not added to the dataframe"
    )

    lmqnt_val = given_data_holder.data.select("LMQNT_VAL").to_series()[0]
    # The value in the kolumn LMQNT_VAL should match the expected value
    if lmqnt_val is not None:
        assert round(lmqnt_val, 5) == round(expected_value, 5), (
            "The added value to the column LMQNT_VAL differs from the expected value"
        )
    else:
        assert expected_value is None, (
            "The added value to the column LMQNT_VAL differs from the expected value"
        )
