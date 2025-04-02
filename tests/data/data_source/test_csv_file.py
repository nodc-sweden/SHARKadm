import pytest
import polars as pl

from sharkadm.data.data_source.txt_file import CsvRowFormatDataFilePolars
from tests.data.data_source.conftest import csv_file_from_dict


@pytest.mark.parametrize("given_delimiter", (",", ";", "\t"))
def test_csv_files_can_be_parsed_to_polars(tmp_path, given_delimiter):
    # Given a csv file
    given_csv_data = [
        {"id": 1, "parameter": "arbitrary_number", "value": 1.23},
        {"id": 2, "parameter": "arbitrary_number", "value": 2.48},
        {"id": 3, "parameter": "arbitrary_number", "value": 3.14},
    ]
    given_data_path = tmp_path / "data.txt"
    csv_file_from_dict(given_csv_data, given_data_path, delimiter=given_delimiter)

    # When loading the file
    data_file = CsvRowFormatDataFilePolars(given_data_path, delimiter=given_delimiter)

    # Then the data is loaded in a polars data frame
    data = data_file.get_data()
    assert len(data) == len(given_csv_data)

    # And all values are parsed as strings
    assert data.dtypes
    assert all(dtype == pl.String for dtype in data.dtypes)


def test_empty_values_are_parsed_as_empty_strings(tmp_path):
    # Given a csv file with empty values
    given_csv_data = [
        {"id": 1, "parameter": "arbitrary_number"},
        {"id": 2, "value": 2.48},
        {"parameter": "arbitrary_number", "value": 3.14},
    ]
    given_data_path = tmp_path / "data.txt"
    csv_file_from_dict(given_csv_data, given_data_path)

    # When loading the file
    data_file = CsvRowFormatDataFilePolars(given_data_path)

    # Then the data is loaded in a polars data frame
    data = data_file.get_data()
    assert not data.is_empty()

    # And all individual items are strings (i.e. not None/Null)
    for row in data.iter_rows():
        assert row
        assert all(isinstance(item, str) for item in row)


@pytest.mark.parametrize(
    "given_column_names, expected_column_names",
    (
        (
            (" outside spaces ", "   left space", "right space     "),
            {"outside spaces", "left space", "right space"},
        ),
        (("no outside spaces",), {"no outside spaces"}),
    ),
)
def test_parsed_column_names_are_stripped(
    tmp_path, given_column_names: tuple, expected_column_names: set
):
    # Given a csv file
    given_csv_data = [
        {column: 10 + n for n, column in enumerate(given_column_names)},
        {column: 20 + n for n, column in enumerate(given_column_names)},
        {column: 30 + n for n, column in enumerate(given_column_names)},
    ]
    given_data_path = tmp_path / "data.txt"
    csv_file_from_dict(given_csv_data, given_data_path)

    # When loading the file
    data_file = CsvRowFormatDataFilePolars(given_data_path)

    # Then the column names are as expected
    data = data_file.get_data()
    column_names = set(data.columns) - {"source"}
    assert column_names == expected_column_names


@pytest.mark.parametrize(
    "given_filename",
    (
        "data.txt",
        "ocean_somthing_data_thing.txt",
    ),
)
def test_filename_is_added_as_source(tmp_path, given_filename):
    # Given a csv file with a specific name
    given_csv_data = [
        {"id": 1, "parameter": "arbitrary_number", "value": 1.23},
        {"id": 2, "parameter": "arbitrary_number", "value": 2.48},
        {"id": 3, "parameter": "arbitrary_number", "value": 3.14},
    ]
    given_data_path = tmp_path / given_filename
    csv_file_from_dict(given_csv_data, given_data_path)

    # When loading the file
    data_file = CsvRowFormatDataFilePolars(given_data_path)

    # Then data_file has stored the file path
    assert data_file.source == str(given_data_path)

    # And each row has stored the file path
    data = data_file.get_data()
    assert all(data["source"] == str(given_data_path))


@pytest.mark.parametrize(
    "given_columns",
    (
        ("A", "B", "C"),
        ("parameter", "value"),
    ),
)
def test_original_column_names_are_stored(tmp_path, given_columns):
    # Given a csv file with a specific column names
    given_csv_data = [
        {column: 10 + n for n, column in enumerate(given_columns)},
        {column: 20 + n for n, column in enumerate(given_columns)},
        {column: 30 + n for n, column in enumerate(given_columns)},
    ]
    given_data_path = tmp_path / "data.txt"
    csv_file_from_dict(given_csv_data, given_data_path)

    # Given the file is loaded
    data_file = CsvRowFormatDataFilePolars(given_data_path)

    # When adding columns
    data = data_file.get_data()
    data_file._data = data.with_columns(not_an_original_column=pl.lit("value"))

    expected_columns = set(given_columns)
    expected_columns.add("source")

    # Then the columns are no longer the same as the original columns
    assert set(data_file.get_data().columns) != expected_columns

    # But the original columns can still be retrieved
    assert set(data_file._original_header) == expected_columns
