import pathlib
import logging

from .base import ArchiveBase
from SHARKadm.data import data_source

logger = logging.getLogger(__name__)


class PhytoplanktonArchive(ArchiveBase):
    _data_type = 'Epibenthos'

    def _load_data(self) -> None:
        data_file_path = self.processed_data_directory / 'data.xml'
        if not data_file_path.exists():
            logger.info(f'No data file found in {self.processed_data_directory}. Looking for file with keyword '
                        f'"data"...')
            for path in self.processed_data_directory.iterdir():
                if 'data' in path.stem and path.suffix == '.xml':
                    data_file_path = path
                    logger.info(f'Will use data file: {path}')
                    break
        if not data_file_path:
            logger.error(f'Could not find any data file in delivery: {self.archive_root_directory}')
            return

        d_source = data_source.XmlDataFile(path=data_file_path, data_type=self.delivery_note.data_type)
        d_source.map_header(self.import_matrix_mapper)

        self._concat_data_source(d_source)
