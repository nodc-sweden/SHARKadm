import polars as pl

from sharkadm.sharkadm_logger import adm_logger
from .. import data_source
from ..data_source.base import PolarsDataDataFrame
from .archive_data_holder import PolarsArchiveDataHolder


class PolarsHarbourSealArchiveDataHolder(PolarsArchiveDataHolder):
    _data_type_synonym = "harbourseal"
    _data_format = "Harbourseal"

    def _load_data(self) -> None:
        d_source = self._load_skv_data_source()
        if not d_source:
            d_source = self._load_txt_data_source()
        d_source.map_header(self.import_matrix_mapper)
        self._set_data_source(d_source)

    def _load_skv_data_source(self):
        data_file_path = self.processed_data_directory / "data.skv"
        if not data_file_path.exists():
            return False
        lokaler_file_path = self.processed_data_directory / "lokaler.skv"
        if not lokaler_file_path.exists():
            return False

        d_source = data_source.SkvDataFile(
            path=data_file_path, data_type=self.delivery_note.data_type
        )
        l_source = data_source.SkvDataFile(
            path=lokaler_file_path, data_type=self.delivery_note.data_type
        )
        ddf = d_source.data.filter(pl.col("STATN") != "")
        ldf = l_source.data.filter(pl.col("Lokal") != "")

        for col in ["LATIT", "LONGI"]:
            if col in ddf.columns[:]:
                ddf = ddf.drop(col)

        df = ddf.join(ldf, left_on="STATN", right_on="Lokal", how="inner")

        d_source = PolarsDataDataFrame(
            df, data_type=self.data_type, source=self.archive_root_directory
        )

        return d_source

    def _load_txt_data_source(self):
        data_file_path = self.processed_data_directory / "data.txt"
        if not data_file_path.exists():
            data_file_path = None
            adm_logger.log_workflow(
                f"No data file found in {self.processed_data_directory}. "
                f"Looking for file with keyword 'data'..."
            )
            for path in self.processed_data_directory.iterdir():
                if "data" in path.stem and path.suffix == ".txt":
                    data_file_path = path
                    adm_logger.log_workflow("Will use data file: {path}")
                    break
        if not data_file_path:
            adm_logger.log_workflow(
                f"Could not find any data file in delivery: {self.archive_root_directory}"
            )
            return

        d_source = data_source.CsvRowFormatPolarsDataFile(
            path=data_file_path, data_type=self.delivery_note.data_type
        )
        return d_source
