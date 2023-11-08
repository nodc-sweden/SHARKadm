import datetime
import pathlib

from SHARKadm.data.archive import ArchiveDataHolder
from .base import Exporter
from SHARKadm import utils


class SHARKMetadataAuto(Exporter):
    """Creates the shark_metadata_auto file"""
    date_str_format = '%Y-%m-%d'

    def __init__(self,
                 export_directory: str | pathlib.Path | None,
                 export_file_name: str | pathlib.Path | None,
                 **kwargs):
        super().__init__()
        if not export_directory:
            export_directory = utils.get_export_directory()
        self._export_directory = pathlib.Path(export_directory)
        if not export_file_name:
            export_file_name = 'export_metadata_auto.txt'
        self._export_file_name = export_file_name

        self._encoding = kwargs.get('encoding', 'cp1252')
        self._data_holder: ArchiveDataHolder | None = None

    @property
    def export_file_path(self):
        return pathlib.Path(self._export_directory, self._export_file_name)

    @staticmethod
    def get_exporter_description() -> str:
        return 'Creates the shark_metadata_auto file'

    @property
    def data_holder(self) -> ArchiveDataHolder:
        return self._data_holder

    @property
    def dataset_name(self) -> str:
        return self.data_holder.dataset_name

    @property
    def dataset_category(self) -> str:
        return self.data_holder.data_type

    @property
    def dataset_version(self) -> str:
        return datetime.datetime.now().strftime(self.date_str_format)

    @property
    def dataset_file_name(self) -> str:
        return f'{self.dataset_name}_version_{self.dataset_version}.zip'

    @property
    def min_year(self) -> str:
        return self.data_holder.min_year

    @property
    def max_year(self) -> str:
        return self.data_holder.max_year

    @property
    def min_date(self) -> str:
        return self.data_holder.min_date

    @property
    def max_date(self) -> str:
        return self.data_holder.max_date

    @property
    def min_longitude(self) -> str:
        return self.data_holder.min_longitude

    @property
    def max_longitude(self) -> str:
        return self.data_holder.max_longitude

    @property
    def min_latitude(self) -> str:
        return self.data_holder.min_latitude

    @property
    def max_latitude(self) -> str:
        return self.data_holder.max_latitude

    def set_data_holder(self, data_holder: ArchiveDataHolder) -> None:
        self._data_holder = data_holder

    def create_file(self,
                    export_directory: pathlib.Path | str = None,
                    export_file_name: pathlib.Path | str = None) -> None:
        """Creates the file using the loaded data_holder"""
        if export_directory:
            self._export_directory = pathlib.Path(export_directory)
        if export_file_name:
            self._export_file_name = export_file_name
        lines = list()
        lines.append(f'dataset_name: {self.dataset_name}')
        lines.append(f'dataset_category: {self.dataset_category}')
        lines.append(f'dataset_version: {self.dataset_version}')
        lines.append(f'dataset_file_name: {self.dataset_file_name}')
        lines.append(f'min_year: {self.min_year}')
        lines.append(f'max_year: {self.max_year}')
        lines.append(f'min_date: {self.min_date}')
        lines.append(f'max_date: {self.max_date}')
        lines.append(f'min_longitude: {self.min_longitude}')
        lines.append(f'max_longitude: {self.max_longitude}')
        lines.append(f'min_latitude: {self.min_latitude}')
        lines.append(f'max_latitude: {self.max_latitude}')
        with open(self.export_file_path, 'w') as fid:
            fid.write('\n'.join(lines))

    def _export(self, data_holder: ArchiveDataHolder) -> None:
        self.set_data_holder(data_holder=data_holder)
        self.create_file()


