import logging

from sharkadm.data import data_source
from .archive_data_holder import ArchiveDataHolder

logger = logging.getLogger(__name__)


class ChlorophyllArchiveDataHolder(ArchiveDataHolder):
    _data_type = "Chlorophyll"
    _data_format = "Chlorophyll"

    def _load_data(self) -> None:
        data_file_path = self.processed_data_directory / "data.txt"
        if not data_file_path.exists():
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

        d_source = data_source.TxtColumnFormatDataFile(
            path=data_file_path, data_type=self.delivery_note.data_type
        )
        d_source.map_header(self.import_matrix_mapper)

        self._set_data_source(d_source)
