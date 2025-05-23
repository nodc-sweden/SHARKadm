import logging

from sharkadm.data import data_source

from .archive_data_holder import ArchiveDataHolder

logger = logging.getLogger(__name__)


class EpibenthosArchiveDataHolder(ArchiveDataHolder):
    _data_type = "Epibenthos"
    _data_format = "Epibenthos"

    def _load_data(self) -> None:
        d_source = self._load_skv_data_source()
        if not d_source:
            d_source = self._load_txt_data_source()
        d_source.remove_columns(
            "SMPNO"
        )  # Thera are soemtimes duplicate columns mapping to sample_id.
        # Note sure if this breaks anything else
        d_source.map_header(self.import_matrix_mapper)
        self._set_data_source(d_source)

    def _load_skv_data_source(self):
        data_file_path = self.processed_data_directory / "data.skv"
        if not data_file_path.exists():
            return False
        d_source = data_source.SkvDataFile(
            path=data_file_path, data_type=self.delivery_note.data_type
        )
        return d_source

    def _load_txt_data_source(self):
        data_file_path = self.processed_data_directory / "data.txt"
        if not data_file_path.exists():
            data_file_path = None
            logger.info(
                f"No data file found in {self.processed_data_directory}. "
                f"Looking for file with keyword 'data'..."
            )
            for path in self.processed_data_directory.iterdir():
                if "data" in path.stem and path.suffix == ".txt":
                    data_file_path = path
                    logger.info(f"Will use data file: {path}")
                    break
        if not data_file_path:
            logger.error(
                f"Could not find any data file in delivery: {self.archive_root_directory}"
            )
            return

        d_source = data_source.TxtRowFormatDataFile(
            path=data_file_path, data_type=self.delivery_note.data_type
        )
        return d_source


class EpibenthosMartransArchiveDataHolder(ArchiveDataHolder):
    _data_type = "Epibenthos"
    _data_format = "EpibenthosMartrans"

    def _load_data(self) -> None:
        data_file_path = self.processed_data_directory / "data.xml"
        if not data_file_path.exists():
            logger.info(
                f"No data file found in {self.processed_data_directory}. "
                f"Looking for file with keyword 'data'..."
            )
            for path in self.processed_data_directory.iterdir():
                if "data" in path.stem and path.suffix == ".xml":
                    data_file_path = path
                    logger.info(f"Will use data file: {path}")
                    break
        if not data_file_path:
            logger.error(
                f"Could not find any data file in delivery: {self.archive_root_directory}"
            )
            return

        d_source = data_source.XmlMartransDataFile(
            path=data_file_path, data_type=self.delivery_note.data_type
        )
        d_source.map_header(self.import_matrix_mapper)

        self._set_data_source(d_source)
