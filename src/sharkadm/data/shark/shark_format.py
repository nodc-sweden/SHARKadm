import pathlib

import polars as pl

from sharkadm import config

from ...config import ImportMatrixConfig, ImportMatrixMapper
from .. import PolarsDataHolder
from ..data_source.base import DataFile
from ..data_source.txt_file import CsvRowFormatPolarsDataFile


class PolarsSharkDataHolder(PolarsDataHolder):
    _data_type: str | None = None
    _data_type_internal: str | None = None
    _data_format: str | None = "all"

    _date_str_format = "%Y-%m-%d"

    def __init__(self, path: str | pathlib.Path | None = None, **kwargs):
        super().__init__()
        self._path = pathlib.Path(path)
        self._encoding = kwargs.get("encoding", "cp1252")
        self._separator = kwargs.get("separator", kwargs.get("delimiter", "\t"))

        self._data: pl.DataFrame = pl.DataFrame()
        self._dataset_name: str | None = None

        self._import_matrix: ImportMatrixConfig | None = None
        self._import_matrix_mapper: ImportMatrixMapper | None = None
        # self._data_type_mapper = get_data_type_mapper()

        self._data_sources: dict[str, DataFile] = {}

        self._initiate()
        self._load_data()

    def _load_import_matrix(self) -> None:
        """Loads the import matrix for the given data type and provider found in
        delivery note"""
        self._import_matrix = config.get_import_matrix_config(
            data_type=self.data_type_internal
        )
        if not self._import_matrix:
            return
        self._import_matrix_mapper = self._import_matrix.get_mapper(self.data_format)

    def _load_data(self) -> None:
        d_source = CsvRowFormatPolarsDataFile(path=self._path, encoding=self._encoding)
        for col in ["Datatyp", "Data type", "DTYPE", "delivery_datatype", "data_type"]:
            if col in d_source.data:
                all_data_types = set(d_source.data[col])
                # Not sure if we want to raise exception if multiple datatypes are found
                # if len(all_data_types) > 1:
                #     raise sharkadm_exceptions.ToManyDatatypesError(str(all_data_types))
                d_source._data_type = all_data_types.pop()
                break

        if d_source._data_type:
            self._data_type = d_source._data_type.lower().replace(" ", "")
            data_type_internal = self._data_type
        else:
            data_type_internal = ""

        if "physical" in data_type_internal:
            data_type_internal = "physicalchemical"
        elif "imaging" in data_type_internal:
            data_type_internal = "plankton_imaging"
        elif "dropvideo" in data_type_internal:
            data_type_internal = "epibenthos_dropvideo"
        elif "grey" in data_type_internal:
            data_type_internal = "greyseal"
        elif "ringed" in data_type_internal:
            data_type_internal = "ringedseal"
        elif "porpoise" in data_type_internal:
            data_type_internal = "harbourporpoise"
        elif "barcoding" in data_type_internal:
            data_type_internal = "plankton_barcoding"
        elif "production" in data_type_internal:
            data_type_internal = "primaryproduction"
        elif "production" in data_type_internal:
            data_type_internal = "primaryproduction"

        self._data_type_internal = data_type_internal

        self._load_import_matrix()
        if self.import_matrix_mapper:
            d_source.map_header(self.import_matrix_mapper)

        self._set_data_source(d_source)

    @staticmethod
    def get_data_holder_description() -> str:
        return """Holds data from shark export"""

    def _initiate(self) -> None:
        self._dataset_name = f"SHARK_export_{self._path.stem}"

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def data_format(self) -> str:
        return self._data_format

    @property
    def header_mapper(self):
        # TODO: Change
        return self._import_matrix_mapper

    @property
    def data_type(self) -> str:
        return self._data_type

    @property
    def data_type_internal(self) -> str:
        # return self._data_type_mapper.get(self.data_format)
        return self._data_type_internal

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @property
    def import_matrix(self) -> ImportMatrixConfig:
        return self._import_matrix

    @property
    def import_matrix_mapper(self) -> ImportMatrixMapper:
        return self._import_matrix_mapper

    @property
    def min_year(self) -> str:
        return str(min(self.data["datetime"]).year)

    @property
    def max_year(self) -> str:
        return str(max(self.data["datetime"]).year)

    @property
    def min_date(self) -> str:
        return min(self.data["datetime"]).strftime(self._date_str_format)

    @property
    def max_date(self) -> str:
        return max(self.data["datetime"]).strftime(self._date_str_format)

    @property
    def min_longitude(self) -> str:
        return str(min(self.data["sample_longitude_dd"].cast(float)))

    @property
    def max_longitude(self) -> str:
        return str(max(self.data["sample_longitude_dd"].cast(float)))

    @property
    def min_latitude(self) -> str:
        return str(min(self.data["sample_latitude_dd"].cast(float)))

    @property
    def max_latitude(self) -> str:
        return str(max(self.data["sample_latitude_dd"].cast(float)))
