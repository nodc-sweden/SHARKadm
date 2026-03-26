import pathlib

from .archive_data_holder import PolarsArchiveDataHolder


class PolarsProfileArchiveDataHolder(PolarsArchiveDataHolder):
    _data_type_synonym = "profile"
    _data_format = "PROFILE"

    @property
    def processed_data_files(self) -> list[pathlib.Path]:
        paths = [path for path in self.processed_data_directory.iterdir()]
        for path in self.processed_data_directory.iterdir():
            paths.append(path)
        return paths
