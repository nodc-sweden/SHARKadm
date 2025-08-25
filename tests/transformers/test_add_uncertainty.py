from unittest.mock import patch

import numpy as np
import pandas as pd
import polars as pl
import pytest

from sharkadm.transformers.add_uncertainty import AddUncertainty, PolarsAddUncertainty


@pytest.mark.parametrize(
    "given_visit_key, given_latitude, given_longitude, given_depth, "
    "given_parameter, given_value, given_unit, given_estimation_uncertainty, "
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
            "",
            np.nan,
        ),  # example TEMP_BTL from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Temperature CTD",
            10.2,
            "C",
            "0.01 °C",
            0.01,
        ),  # example TEMP_CTD from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Salinity bottle",
            5.2,
            "o/oo psu",
            "0,056%",
            0.002912,
        ),  # example SALT_BTL from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Salinity CTD",
            35.1,
            "o/oo psu",
            "0,014",
            0.014,
        ),  # example SALT_CTD from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Dissolved oxygen O2 bottle",
            5.2,
            "ml/l",
            "1%",
            0.052,
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
        ("ABC123", 57.3, 10.1, 5.0, "pH", 7.82, "", "0.05", 0.05),  # example PH from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Alkalinity",
            2.301,
            "mmol/kg",
            "0,8%",
            0.018408,
        ),  # example ALKY from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Phosphate PO4-P",
            0.5,
            "umol/l",
            "9.5 % (nivå 20 µg/L), 32 % (nivå 2)µg/L",
            0.0475,
        ),  # example PHOS from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Total phosphorus Tot-P",
            1.2,
            "umol/l",
            "18 % (nivå 20 µg/L), 36 % (nivå 2 µg/L)",
            np.nan,
        ),  # example PTOT from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Nitrite NO2-N",
            0.05,
            "umol/l",
            "22 % (nivå 20 µg/L), 27 % (nivå 2 µg/L)",
            0.0135,
        ),  # example NTRI from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Nitrate NO3-N",
            2.7,
            "umol/l",
            "8.4 % (nivå 50 µg/L), 36 % (nivå 5 µg/L)",
            0.2268,
        ),  # example NTRA from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Ammonium NH4-N",
            2.1,
            "umol/l",
            "22 %(nivå 30 µg/L), 34 %(nivå 3 µg/L)",
            0.462,
        ),  # example AMON from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Total Nitrogen Tot-N",
            3.3,
            "umol/l",
            "9.4 % (nivå 50 µg/L), 36 % (nivå 10 µg/L)",
            0.3102,
        ),  # example NTOT from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Silicate SiO3-Si",
            6.7,
            "umol/l",
            "9.2 % (nivå 200 µg/L)",
            0.6164,
        ),  # example SIO2 from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Silicate SiO3-Si",
            10,
            "umol/l",
            "9.2 % (nivå 200 µg/L)",
            np.nan,
        ),  # example SIO2 extreme
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Humus",
            3.2,
            "ug/l",
            "7%",
            0.224,
        ),  # example HUMUS from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Dissolved organic carbon DOC",
            57.2,
            "umol/l",
            "9%",
            5.148,
        ),  # example DOC from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Chlorophyll-a bottle",
            4.7,
            "ug/l",
            "19%",
            0.893,
        ),  # example CPHL from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Coloured dissolved organic matter CDOM",
            3.2,
            "1/m",
            "5%",
            0.16,
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
            "21%",
            0.462,
        ),  # example TURB from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Dissolved oxygen O2 bottle",
            3.2,
            "ml/l",
            "0.3 mg/l",
            0.21,
        ),  # example DOXY_BTL from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Dissolved oxygen O2 CTD",
            2.2,
            "ml/l",
            "0.7 mg/l",
            0.49,
        ),  # example DOXY_CTD from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Hydrogen sulphide H2S",
            4.2,
            "umol/l",
            "0.1 (0.1-0.2 mg/l), 12% (>0.2 mg/l)",
            0.1,
        ),  # example DOXY_CTD from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Alkalinity",
            2.301,
            "mmol/kg",
            "4%",
            0.09204,
        ),  # example ALKY from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Phosphate PO4-P",
            0.5,
            "umol/l",
            "0.5 µg/l (<2 µg/l), 1.0 µg/l (2-25 µg/l), 5% (>25 µg/l)",
            0.032285,
        ),  # example PHOS from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Total phosphorus Tot-P",
            1.2,
            "umol/l",
            "2.0 µg/l (<25 µg/l), 7% (>25 µg/l)",
            0.084,
        ),  # example PTOT from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Nitrite+Nitrate NO2+NO3-N",
            1.2,
            "umol/l",
            "0.3 µg/l (<2 µg/l), 1.3 µg/l (2-20 µg/l), 4% (>20 µg/l)",
            0.0928122,
        ),  # example NTRZ from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Ammonium NH4-N",
            2.2,
            "umol/l",
            "0.5 µg/l (<3 µg/l), 1.7 µg/l (3-30 µg/l), 5% (>30 µg/l)",
            0.11,
        ),  # example AMON from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Total Nitrogen Tot-N",
            3.3,
            "umol/l",
            "9%",
            0.297,
        ),  # example NTOT from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Silicate SiO3-Si",
            1.2,
            "umol/l",
            "2.5 µg/l (<60 µg/l), 4% (>60 µg/l)",
            0.089015,
        ),  # example SIO2 from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Chlorophyll-a bottle",
            4.7,
            "ug/l",
            "0.5 (<2 µg/l), 35% (≥2 µg/l)",
            1.645,
        ),  # example CPHL from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Chlorophyll-a bottle",
            2,
            "ug/l",
            "0.5 (<2 µg/l), 35% (≥2 µg/l)",
            0.7,
        ),  # example CPHL from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Temperature CTD",
            10.2,
            "C",
            "0.04 °C",
            0.04,
        ),  # example TEMP_CTD from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Salinity bottle",
            5.2,
            "o/oo psu",
            "0.04 psu",
            0.04,
        ),  # example SALT_BTL from DEEP
    ),
)
@patch("sharkadm.config.get_valid_data_types", return_value=[])
def test_validate_add_uncertainty(
    mocked_data_types,
    pandas_data_frame_holder_class,
    given_visit_key,
    given_latitude,
    given_longitude,
    given_depth,
    given_parameter,
    given_value,
    given_unit,
    given_estimation_uncertainty,
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
            "estimation_uncertainty": [given_estimation_uncertainty],
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
    assert "UNCERT_VAL" not in given_data_holder.data.columns, (
        "The column UNCERT_VAL already exist."
    )

    # Transforming the data
    AddUncertainty().transform(given_data_holder)

    # After transformation there should be no nan values
    # in the dataframe
    assert "UNCERT_VAL" in given_data_holder.data.columns, (
        "The column UNCERT_VAL was not added to the dataframe"
    )

    # The value in the kolumn LMQNT_VAL should match the expected value
    if given_data_holder.data["UNCERT_VAL"][0] and not np.isnan(
        given_data_holder.data["UNCERT_VAL"][0]
    ):
        assert round(given_data_holder.data["UNCERT_VAL"][0], 4) == round(
            expected_value, 4
        ), "The added value to the column UNCERT_VAL differs from the expected value"
    elif np.isnan(given_data_holder.data["UNCERT_VAL"][0]):
        assert np.isnan(expected_value), (
            "The added value to the column UNCERT_VAL differs from the expected value"
        )
    else:
        assert expected_value is None, (
            "The added value to the column UNCERT_VAL differs from the expected value"
        )


@pytest.mark.parametrize(
    "given_visit_key, given_latitude, given_longitude, given_depth, "
    "given_parameter, given_value, given_unit, given_estimation_uncertainty, "
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
            "",
            None,
        ),  # example TEMP_BTL from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Temperature CTD",
            10.2,
            "C",
            "0.01 °C",
            0.01,
        ),  # example TEMP_CTD from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Salinity bottle",
            5.2,
            "o/oo psu",
            "0,056%",
            0.002912,
        ),  # example SALT_BTL from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Salinity CTD",
            35.1,
            "o/oo psu",
            "0,014",
            0.014,
        ),  # example SALT_CTD from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Dissolved oxygen O2 bottle",
            5.2,
            "ml/l",
            "1%",
            0.052,
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
        ("ABC123", 57.3, 10.1, 5.0, "pH", 7.82, "", "0.05", 0.05),  # example PH from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Alkalinity",
            2.301,
            "mmol/kg",
            "0,8%",
            0.018408,
        ),  # example ALKY from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Phosphate PO4-P",
            0.5,
            "umol/l",
            "9.5 % (nivå 20 µg/L), 32 % (nivå 2)µg/L",
            0.0475,
        ),  # example PHOS from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Total phosphorus Tot-P",
            1.2,
            "umol/l",
            "18 % (nivå 20 µg/L), 36 % (nivå 2 µg/L)",
            None,
        ),  # example PTOT from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Nitrite NO2-N",
            0.05,
            "umol/l",
            "22 % (nivå 20 µg/L), 27 % (nivå 2 µg/L)",
            0.0135,
        ),  # example NTRI from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Nitrate NO3-N",
            2.7,
            "umol/l",
            "8.4 % (nivå 50 µg/L), 36 % (nivå 5 µg/L)",
            0.2268,
        ),  # example NTRA from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Ammonium NH4-N",
            2.1,
            "umol/l",
            "22 %(nivå 30 µg/L), 34 %(nivå 3 µg/L)",
            0.462,
        ),  # example AMON from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Total Nitrogen Tot-N",
            3.3,
            "umol/l",
            "9.4 % (nivå 50 µg/L), 36 % (nivå 10 µg/L)",
            0.3102,
        ),  # example NTOT from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Silicate SiO3-Si",
            6.7,
            "umol/l",
            "9.2 % (nivå 200 µg/L)",
            0.6164,
        ),  # example SIO2 from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Silicate SiO3-Si",
            10,
            "umol/l",
            "9.2 % (nivå 200 µg/L)",
            None,
        ),  # example SIO2 extreme
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Humus",
            3.2,
            "ug/l",
            "7%",
            0.224,
        ),  # example HUMUS from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Dissolved organic carbon DOC",
            57.2,
            "umol/l",
            "9%",
            5.148,
        ),  # example DOC from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Chlorophyll-a bottle",
            4.7,
            "ug/l",
            "19%",
            0.893,
        ),  # example CPHL from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Coloured dissolved organic matter CDOM",
            3.2,
            "1/m",
            "5%",
            0.16,
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
            "21%",
            0.462,
        ),  # example TURB from UMSC
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Dissolved oxygen O2 bottle",
            3.2,
            "ml/l",
            "0.3 mg/l",
            0.21,
        ),  # example DOXY_BTL from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Dissolved oxygen O2 CTD",
            2.2,
            "ml/l",
            "0.7 mg/l",
            0.49,
        ),  # example DOXY_CTD from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Hydrogen sulphide H2S",
            4.2,
            "umol/l",
            "0.1 (0.1-0.2 mg/l), 12% (>0.2 mg/l)",
            0.1,
        ),  # example DOXY_CTD from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Alkalinity",
            2.301,
            "mmol/kg",
            "4%",
            0.09204,
        ),  # example ALKY from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Phosphate PO4-P",
            0.5,
            "umol/l",
            "0.5 µg/l (<2 µg/l), 1.0 µg/l (2-25 µg/l), 5% (>25 µg/l)",
            0.032285,
        ),  # example PHOS from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Total phosphorus Tot-P",
            1.2,
            "umol/l",
            "2.0 µg/l (<25 µg/l), 7% (>25 µg/l)",
            0.084,
        ),  # example PTOT from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Nitrite+Nitrate NO2+NO3-N",
            1.2,
            "umol/l",
            "0.3 µg/l (<2 µg/l), 1.3 µg/l (2-20 µg/l), 4% (>20 µg/l)",
            0.0928122,
        ),  # example NTRZ from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Ammonium NH4-N",
            2.2,
            "umol/l",
            "0.5 µg/l (<3 µg/l), 1.7 µg/l (3-30 µg/l), 5% (>30 µg/l)",
            0.11,
        ),  # example AMON from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Total Nitrogen Tot-N",
            3.3,
            "umol/l",
            "9%",
            0.297,
        ),  # example NTOT from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Silicate SiO3-Si",
            1.2,
            "umol/l",
            "2.5 µg/l (<60 µg/l), 4% (>60 µg/l)",
            0.089015,
        ),  # example SIO2 from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Chlorophyll-a bottle",
            4.7,
            "ug/l",
            "0.5 (<2 µg/l), 35% (≥2 µg/l)",
            1.645,
        ),  # example CPHL from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Chlorophyll-a bottle",
            2,
            "ug/l",
            "0.5 (<2 µg/l), 35% (≥2 µg/l)",
            0.7,
        ),  # example CPHL from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Temperature CTD",
            10.2,
            "C",
            "0.04 °C",
            0.04,
        ),  # example TEMP_CTD from DEEP
        (
            "ABC123",
            57.3,
            10.1,
            5.0,
            "Salinity bottle",
            5.2,
            "o/oo psu",
            "0.04 psu",
            0.04,
        ),  # example SALT_BTL from DEEP
    ),
)
@patch("sharkadm.config.get_valid_data_types", return_value=[])
def test_validate_polars_add_uncertainty(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_visit_key,
    given_latitude,
    given_longitude,
    given_depth,
    given_parameter,
    given_value,
    given_unit,
    given_estimation_uncertainty,
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
            "estimation_uncertainty": [given_estimation_uncertainty],
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
    assert "UNCERT_VAL" not in given_data_holder.data.columns, (
        "The column UNCERT_VAL already exist."
    )

    # Transforming the data
    PolarsAddUncertainty().transform(given_data_holder)

    # After transformation there should be no nan values
    # in the dataframe
    assert "UNCERT_VAL" in given_data_holder.data.columns, (
        "The column UNCERT_VAL was not added to the dataframe"
    )

    uncert_val = given_data_holder.data.select("UNCERT_VAL").to_series()[0]

    # The value in the kolumn UNCERT_VAL should match the expected value
    if uncert_val is not None:
        assert round(uncert_val, 4) == round(expected_value, 4), (
            "The added value to the column UNCERT_VAL differs from the expected value"
        )
    else:
        assert expected_value is None, (
            "The added value to the column UNCERT_VAL differs from the expected value"
        )
