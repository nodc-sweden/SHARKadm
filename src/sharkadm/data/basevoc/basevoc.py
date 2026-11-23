import pathlib

import polars as pl
from openpyxl import load_workbook

from sharkadm.data.data_holder import PolarsDataHolder
from sharkadm.data.data_source.txt_file import CsvRowFormatPolarsDataFile
from sharkadm.data.data_source.xlsx_file import XlsxFormatPolarsDataFile
from sharkadm.sharkadm_logger import adm_logger

# No import matrix for this data
INTERNAL_COLUMN_NAMES = {
    "datetime_utc": "sample_iso_datetime",
    "start_date": "visit_date",
    "start_time": "sample_time",
    "end_date": "sample_enddate",
    "end_time": "sample_endtime",
    "longitude": "visit_reported_longitude",
    "latitude": "visit_reported_latitude",
    "station_name": "reported_station_name",
}


class BasevocDataHolder(PolarsDataHolder):
    _data_structure = "column"
    _indexing_column = "sample_iso_datetime"

    def __init__(
        self,
        data_file: str | pathlib.Path | None = None,
        metadata_file: str | pathlib.Path | None = None,
        selected_compounds_file: str | pathlib.Path | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data_file = pathlib.Path(data_file)
        self._metadata_file = pathlib.Path(metadata_file)
        self._selected_compounds_file = pathlib.Path(selected_compounds_file)

        self._data: pl.DataFrame = pl.DataFrame()
        self._data_columns: list[str] = []
        self._selected_compund = {}
        self._dataset_name = self.data_file.stem

        self._load_selected_compounds()
        self._load_data()
        self._update_data_columns()
        self._update_qc_columns()
        self._load_metadata()

    @staticmethod
    def get_data_holder_description() -> str:
        return """Holds data from a time series."""

    @property
    def dataset_name(self) -> str:
        return self._dataset_name or "Unknown"

    @property
    def data_file(self) -> pathlib.Path:
        return self._data_file

    @property
    def metadata_file(self) -> pathlib.Path:
        return self._metadata_file

    @property
    def selected_compounds_file(self) -> pathlib.Path:
        return self._selected_compounds_file

    @property
    def selected_compound(self) -> dict[str, str]:
        return self._selected_compound

    @property
    def indexing_column(self) -> str:
        """The column that indexes the series."""
        return self._indexing_column

    @property
    def data_columns(self) -> list[str]:
        """Columns with data."""
        return self._data_columns

    def _load_data(self) -> None:
        data_source = CsvRowFormatPolarsDataFile(
            path=self.data_file,
            data_type=self.data_type,
            nodc_conf=self.config,
            delimiter=",",
        )
        data_source.map_header(INTERNAL_COLUMN_NAMES)
        self._set_data_source(data_source)

    def _update_data_columns(self) -> None:
        """Store the names of the data columns."""
        # In the future there might be more than one data column per file
        self._data_columns = [self._selected_compound["Value column in data file"]]

    def _update_qc_columns(self) -> None:
        """Store a mapping to corresponding qc column for each data column."""

        # Mixing ratio i uncertain, because no mixing data available yet
        parameter_to_qc_column = {
            "Flux": "flux_qflag_L20",
            "Mixing ratio": "mixing_ratio_qflag_L20",
        }
        parameter = self.selected_compound["Parameter"]
        qc_column = parameter_to_qc_column[parameter]

        indexing_qc_column = f"qc_sdn_{self.indexing_column}"
        self.data = self.data.with_columns(pl.lit(1).alias(indexing_qc_column))
        self._qc_columns = {self.indexing_column: indexing_qc_column}
        for col in self.data_columns:
            self._qc_columns[col] = qc_column

    def _load_metadata(self) -> None:
        """Load metadata from either CSV or XLSX file and add to the dataframe."""

        if not self.metadata_file.exists():
            raise FileNotFoundError(
                f"The selected metadata file '{self.metadata_file}' cannot be found."
            )

        if self.metadata_file.suffix.lower() == ".xlsx":
            workbook = load_workbook(self.metadata_file, read_only=True, data_only=True)
            sheet_name = workbook.sheetnames[0]
            workbook.close()
            metadata_source = XlsxFormatPolarsDataFile(
                path=self.metadata_file,
                sheet_name=sheet_name,
                data_type=self.data_type,
                nodc_conf=self.config,
            )
        elif self.metadata_file.suffix.lower() in [".csv", ".txt"]:
            metadata_source = CsvRowFormatPolarsDataFile(
                path=self.metadata_file, data_type=self.data_type, nodc_conf=self.config
            )
        else:
            adm_logger.log_workflow(
                f"Unsupported metadata file format: {self.metadata_file.suffix}",
                level=adm_logger.ERROR,
            )
            raise ValueError(
                f"Unsupported metadata file format: {self.metadata_file.suffix}"
            )

        metadata_source.map_header(INTERNAL_COLUMN_NAMES)
        metadata_df = self._get_data_from_data_source(metadata_source)

        # The relevant metadata row is matched through the local_cdi_id
        compound = self.selected_compound["Compound label in data file"]
        measurement_type = self.selected_compound["Parameter"]
        if measurement_type.lower() == "flux":
            metadata_filtered = metadata_df.filter(
                pl.col("local_cdi_id")
                .str.to_lowercase()
                .str.contains(compound.lower().strip())
                & pl.col("local_cdi_id")
                .str.to_lowercase()
                .str.contains(measurement_type.lower().strip())
            ).head(1)
        elif measurement_type.lower == "mixing ratio":
            type_match = "mix"
            metadata_filtered = metadata_df.filter(
                pl.col("local_cdi_id")
                .str.to_lowercase()
                .str.contains(compound.lower().strip())
                & pl.col("local_cdi_id").str.to_lowercase().str.contains(type_match)
            ).head(1)
        else:
            raise ValueError("Cannot match file to a local cdi id")

        adm_logger.log_workflow(
            f"Connected file '{self.data_file}' to local_cdi_id = "
            f"'{metadata_filtered['local_cdi_id'][0]}'"
        )
        metadata_dict = metadata_filtered[0, :].to_dicts()[
            0
        ]  # Antar endast en rad här, flera av filerna ha flera rader

        for col_name, value in metadata_dict.items():
            if col_name not in self._data.columns:
                self._data = self._data.with_columns(pl.lit(value).alias(col_name))

    def _load_selected_compounds(self) -> None:
        selected_compounds = pl.read_excel(
            self.selected_compounds_file,
            engine="calamine",
            read_options={"header_row": 3, "dtypes": "string"},
            has_header=True,
        )

        selected_compounds = selected_compounds.with_columns(
            pl.col("Data file")
            .cast(pl.String)
            .str.replace_all(r"\\", "/")
            .str.split("/")
            .list.last()
            .alias("_filename")
        )
        matches = selected_compounds.filter(pl.col("_filename") == self.data_file.name)

        if matches.height == 0:
            raise ValueError(f"No selected-compounds row for {self.data_file}")

        if matches.height > 1:
            # In the future there a single file might contain multiple measurements,
            # and this case should be handled.
            raise ValueError(f"Multiple selected-compounds rows for {self.data_file}")

        self._selected_compound = matches.drop("_filename").row(0, named=True)
