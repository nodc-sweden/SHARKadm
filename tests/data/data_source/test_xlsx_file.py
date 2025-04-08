import polars as pl

from sharkadm.data.data_source.xlsx_file import XlsxFormatDataFilePolars
from tests.data.data_source.conftest import xlsx_file_from_dict


def test_xlsx_file_can_be_parsed_to_polars(tmp_path):
    # Given a csv file
    given_xlsx_data = [
        {"id": 1, "parameter": "arbitrary_number", "value": 1.23},
        {"id": 2, "parameter": "arbitrary_number", "value": 2.48},
        {"id": 3, "parameter": "arbitrary_number", "value": 3.14},
    ]
    given_data_path = tmp_path / "data.xlsx"
    xlsx_file_from_dict(given_xlsx_data, given_data_path)

    # When loading the file
    data_file = XlsxFormatDataFilePolars(given_data_path, sheet_name="Sheet1")

    # Then the data is loaded in a polars data frame
    data = data_file.get_data()
    assert len(data) == len(given_xlsx_data)

    # And all values are parsed as strings
    assert data.dtypes
    assert all(dtype == pl.String for dtype in data.dtypes)
