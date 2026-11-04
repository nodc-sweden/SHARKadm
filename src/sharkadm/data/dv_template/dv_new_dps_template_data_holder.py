import pathlib

import openpyxl
import polars as pl

from sharkadm.config import get_all_data_types
from sharkadm.config.import_matrix import ImportMatrixConfig, ImportMatrixMapper
from sharkadm.data.data_holder import PolarsDataHolder
from sharkadm.data.data_source.base import PolarsDataFile
from sharkadm.data.data_source.xlsx_file import XlsxFormatPolarsDataFile


class PolarsDvNewDPSTemplateDataHolder(PolarsDataHolder):
    _data_structure = "column"
    # _date_str_format = "%Y-%m-%d"

    def __init__(self, template_path: str | pathlib.Path | None = None, **kwargs):
        super().__init__(**kwargs)
        # OM man hamnar här så VET man att det är det nya formatet

        # TODO make root path?
        self._template_path = pathlib.Path(template_path)

        self._data: pl.DataFrame = pl.DataFrame()
        self._dataset_name: str | None = None

        self._import_matrix: ImportMatrixConfig | None = None
        self._import_matrix_mapper: ImportMatrixMapper | None = None

        self._data_sources: dict[str, PolarsDataFile] = {}

        self._data_type_synonym: str | None = None
        self._data_format: str | None = None

        self._initiate()
        self._set_data_type()
        self._set_data_format()
        self._load_import_matrix()
        self._load_data()

    @staticmethod
    def get_data_holder_description() -> str:
        return """Holds data from a Data host delivery template"""

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    def _initiate(self) -> None:
        parent_folder = self._template_path.stem
        if parent_folder == "received_data":
            self._dataset_name == parent_folder.stem
        else:
            self._dataset_name = parent_folder

    @property
    def header_mapper(self):
        return self._import_matrix_mapper

    @property
    def template_path(self) -> pathlib.Path:
        return self._template_path

    @property
    def import_matrix_mapper(self) -> ImportMatrixMapper:
        return self._import_matrix_mapper

    @property
    def datetime_without_none(self) -> pl.Series:
        return self.data.select(pl.col("datetime").drop_nulls())["datetime"]

    @property
    def min_year(self) -> str:
        return str(min(self.datetime_without_none).year)

    @property
    def max_year(self) -> str:
        return str(max(self.datetime_without_none).year)

    @property
    def min_date(self) -> str:
        return min(self.datetime_without_none).strftime(self._date_str_format)

    @property
    def max_date(self) -> str:
        return max(self.datetime_without_none).strftime(self._date_str_format)

    @property
    def min_longitude(self) -> str:
        if "sample_longitude_dd" in self._data.columns:
            return str(min(self.data["sample_longitude_dd"].cast(float)))
        return str(min(self.data["visit_reported_longitude"].cast(float)))

    @property
    def max_longitude(self) -> str:
        if "sample_longitude_dd" in self._data.columns:
            return str(min(self.data["sample_longitude_dd"].cast(float)))
        return str(max(self.data["visit_reported_longitude"].cast(float)))

    @property
    def min_latitude(self) -> str:
        if "sample_latitude_dd" in self._data.columns:
            return str(min(self.data["sample_latitude_dd"].cast(float)))
        return str(min(self.data["visit_reported_latitude"].cast(float)))

    @property
    def max_latitude(self) -> str:
        if "sample_latitude_dd" in self._data.columns:
            return str(min(self.data["sample_latitude_dd"].cast(float)))
        return str(max(self.data["visit_reported_latitude"].cast(float)))

    def _load_import_matrix(self) -> None:
        """Loads the import matrix for the given data type and provider found in
        delivery note"""

        self._import_matrix = self.data_type_obj.import_matrix
        self._import_matrix_mapper = self.data_type_obj.get_mapper(self._data_format)

    def _get_overview_data(self) -> dict:
        wb = openpyxl.load_workbook(self._template_path, read_only=True, data_only=True)

        try:
            if "Översikt" not in wb.sheetnames:
                raise ValueError("Could not find sheet 'Översikt'")

            ws = wb["Översikt"]
            data = {}
            for key, value in ws.iter_rows(min_col=1, max_col=2, values_only=True):
                if key is not None:
                    data[key] = value

            return data

        finally:
            wb.close()

    def _set_data_type(self) -> None:
        overview_data = self._get_overview_data()
        existing_dtypes = get_all_data_types(self.config)
        dtype = overview_data.get("Intern datatyp")
        if dtype not in existing_dtypes:
            raise ValueError(f"Datatype '{dtype}' in template is not a valid datatype.")
        self._data_type_synonym = dtype

    def _set_data_format(self) -> None:
        overview_data = self._get_overview_data()
        version = overview_data.get("Version")
        self._data_format = f"DPS{version}"

    def _load_data(self) -> None:
        wb = openpyxl.load_workbook(self._template_path, read_only=True)
        sheet_name = "Data"
        try:
            if sheet_name not in wb.sheetnames:
                raise ValueError(
                    f"Could not find sheet '{sheet_name}' in file: {self._template_path}"
                )
        finally:
            wb.close()

        d_source = XlsxFormatPolarsDataFile(
            nodc_conf=self.config,
            path=self._template_path,
            sheet_name=sheet_name,
            data_type=self._data_type_synonym,
        )
        if self._import_matrix_mapper:
            d_source.map_header(self._import_matrix_mapper)

        self._set_data_source(d_source)
