import logging
import os
import pathlib
import shutil

from sharkadm import exporters, sharkadm_logger, utils
from sharkadm.data import PandasDataHolder
from sharkadm.sharkadm_logger import adm_logger

from .base import FileExporter


class ZipArchive(FileExporter):
    """Class to export zip package that are sent to SHARKdata"""

    def __init__(
        self,
        export_directory: str | pathlib.Path | None = None,
        export_file_name: str | pathlib.Path | None = None,
        **kwargs,
    ):
        super().__init__(export_directory, export_file_name, **kwargs)

        self._data_holder: PandasDataHolder | None = None
        self._metadata_auto: exporters.SHARKMetadataAuto | None = None

    @staticmethod
    def get_exporter_description() -> str:
        return "Creates the SHARKadm zip package"

    @property
    def _temp_target_directory(self) -> pathlib.Path:
        return utils.get_temp_directory(f"{self._data_holder.zip_archive_name}")

    @property
    def _save_zip_path(self) -> pathlib.Path:
        return self._export_directory / f"{self._temp_target_directory.name}.zip"

    def _reset_temp_target_directory(self):
        self._temp_target_directory.mkdir(parents=True, exist_ok=True)
        for path in self._temp_target_directory.iterdir():
            try:
                if path.is_file():
                    os.remove(path)
                elif path.is_dir():
                    shutil.rmtree(path)
            except Exception as e:
                logging.debug(f"Failed to delete {path}. Reason: {e}")

    def _export(self, data_holder: PandasDataHolder) -> None:
        self._data_holder = data_holder
        self._load_metadata_auto_object()
        self._reset_temp_target_directory()
        self._copy_received_files()
        self._copy_processed_files()

        self._add_shark_metadata()
        self._add_readme_en()
        self._add_readme_sv()
        self._create_shark_metadata_auto()
        self._create_data_file()
        self._create_changelog_file()

        self._create_zip_package()

    def _copy_received_files(self) -> None:
        if not hasattr(self._data_holder, "received_data_files"):
            adm_logger.log_export(
                f"No attribute for received_data_files for data_holder "
                f"{self._data_holder.data_holder_name}",
                level=adm_logger.DEBUG,
            )
            return
        target_dir = self._temp_target_directory / "received_data"
        target_dir.mkdir(parents=True, exist_ok=True)
        for path in self._data_holder.received_data_files:
            target_path = target_dir / path.name
            shutil.copy2(path, target_path)

    def _copy_processed_files(self) -> None:
        if not hasattr(self._data_holder, "processed_data_files"):
            adm_logger.log_export(
                f"No attribute for processed_data_files for data_holder "
                f"{self._data_holder.data_holder_name}",
                level=adm_logger.DEBUG,
            )
            return
        target_dir = self._temp_target_directory / "processed_data"
        target_dir.mkdir(parents=True, exist_ok=True)
        for path in self._data_holder.processed_data_files:
            if not path.exists():
                adm_logger.log_export(
                    f"Missing processed file: {path.name}", level=adm_logger.WARNING
                )
                continue
            target_path = target_dir / path.name
            shutil.copy2(path, target_path)

    def _load_metadata_auto_object(self) -> None:
        self._metadata_auto = exporters.SHARKMetadataAuto(None)
        self._metadata_auto.set_data_holder(data_holder=self._data_holder)

    def _add_shark_metadata(self) -> None:
        if not hasattr(self._data_holder, "shark_metadata_path"):
            adm_logger.log_export(
                f"No attribute for shark_metadata_path for data_holder "
                f"{self._data_holder.data_holder_name}",
                level=adm_logger.DEBUG,
            )
            return
        source_path = self._data_holder.shark_metadata_path
        target_path = self._temp_target_directory / source_path.name
        shutil.copy2(source_path, target_path)

    def _add_readme_en(self) -> None:
        adm_logger.log_export("Add README.txt not implemented", level=adm_logger.DEBUG)

    def _add_readme_sv(self) -> None:
        adm_logger.log_export("Add README_sv.txt not implemented", level=adm_logger.DEBUG)

    def _create_shark_metadata_auto(self) -> None:
        print(f"{self._temp_target_directory=}")
        self._metadata_auto.create_file(
            export_directory=self._temp_target_directory,
            export_file_name="shark_metadata_auto.txt",
        )

    def _create_data_file(self) -> None:
        exporter = exporters.SHARKdataTxt(
            export_directory=self._temp_target_directory,
            export_file_name="shark_data.txt",
        )
        exporter.export(self._data_holder)

    def _create_changelog_file(self) -> None:
        sharkadm_logger.create_changelog_file(
            export_directory=self._temp_target_directory
        )

    def _create_zip_package(self):
        shutil.make_archive(
            str(self._save_zip_path.with_suffix("")),
            "zip",
            str(self._temp_target_directory),
        )
