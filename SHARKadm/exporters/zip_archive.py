import datetime
import logging
import shutil
import os
import pathlib
from SHARKadm.data.archive import ArchiveBase
from SHARKadm import exporters
from SHARKadm import utils

from .base import Exporter, DataHolderProtocol


class ZipArchive(Exporter):
    """Class to export zip package that are sent to SHARKdata"""

    def __init__(self, directory: str | pathlib.Path, **kwargs):
        super().__init__(**kwargs)
        self._save_to_directory = pathlib.Path(directory)
        if not self._save_to_directory.iterdir():
            raise NotADirectoryError(self._save_to_directory)
        self._encoding = kwargs.get('encoding', 'cp1252')
        self._data_holder: ArchiveBase | None = None
        self._metadata_auto: exporters.SHARKMetadataAuto | None = None

    @staticmethod
    def get_exporter_description() -> str:
        return 'Creates the SHARKadm zip package'

    @property
    def _temp_zip_directory(self) -> pathlib.Path:
        return utils.get_temp_directory(f'{self._metadata_auto.dataset_file_name.split(".")[0]}')
        # return pathlib.Path.home() / '_temp_sharkadm' / f'{self._metadata_auto.dataset_file_name.split(".")[0]}'

    @property
    def _save_zip_directory(self) -> pathlib.Path:
        return self._save_to_directory / self._temp_zip_directory.name

    def _reset_temp_zip_directory(self):
        self._temp_zip_directory.mkdir(parents=True, exist_ok=True)
        for path in self._temp_zip_directory.iterdir():
            try:
                if path.is_file():
                    os.remove(path)
                elif path.is_dir():
                    shutil.rmtree(path)
            except Exception as e:
                logging.debug(f'Failed to delete {path}. Reason: {e}')

    def _export(self, data_holder: ArchiveBase) -> None:
        self._data_holder = data_holder
        self._load_metadata_auto_object()
        self._reset_temp_zip_directory()
        self._copy_received_files()
        self._copy_processed_files()

        self._add_readme_en()
        self._add_readme_sv()
        self._add_shark_metadata()
        self._create_shark_metadata_auto()
        self._create_data_file()

    def _copy_received_files(self) -> None:
        target_dir = self._temp_zip_directory / self._data_holder.received_data_directory.name
        target_dir.mkdir(parents=True, exist_ok=True)
        for path in self._data_holder.received_data_directory.iterdir():
            target_path = target_dir / path.name
            shutil.copy2(path, target_path)

    def _copy_processed_files(self) -> None:
        target_dir = self._temp_zip_directory / self._data_holder.processed_data_directory.name
        target_dir.mkdir(parents=True, exist_ok=True)
        for path in self._data_holder.processed_data_directory.iterdir():
            target_path = target_dir / path.name
            shutil.copy2(path, target_path)

    def _load_metadata_auto_object(self) -> None:
        self._metadata_auto = exporters.SHARKMetadataAuto(None)
        self._metadata_auto.set_data_holder(data_holder=self._data_holder)

    def _add_readme_en(self) -> None:
        pass

    def _add_readme_sv(self) -> None:
        pass

    def _add_shark_metadata(self) -> None:
        pass

    def _create_shark_metadata_auto(self) -> None:
        self._metadata_auto.create_file(path=self._temp_zip_directory / 'shark_metadata_auto.txt')

    def _create_data_file(self) -> None:
        exporter = exporters.SHARKdataTxt(path=self._temp_zip_directory / 'shark_data.txt')
        exporter.export(self._data_holder)

