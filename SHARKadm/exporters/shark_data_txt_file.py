import pathlib

from .base import Exporter, DataHolderProtocol

from SHARKadm.config import get_column_views_config
from SHARKadm import utils


class SHARKdataTxt(Exporter):
    """Writes data to file filtered by the columns specified for the given data type in column_views."""

    def __init__(self,
                 export_directory: str | pathlib.Path | None = None,
                 export_file_name: str | pathlib.Path | None = None,
                 **kwargs):
        super().__init__()
        if not export_directory:
            export_directory = utils.get_export_directory()
        self._export_directory = pathlib.Path(export_directory)
        if not export_file_name:
            export_file_name = 'shark_data.txt'
        self._export_file_name = export_file_name
        self._encoding = kwargs.get('encoding', 'cp1252')
        self._column_views = get_column_views_config()

    @property
    def export_file_path(self):
        return pathlib.Path(self._export_directory, self._export_file_name)

    @staticmethod
    def get_exporter_description() -> str:
        return 'Writes data to file filtered by the columns specified for the given data type in column_views.'

    def _export(self, data_holder: DataHolderProtocol) -> None:
        column_list = self._column_views.get_columns_for_view(view=data_holder.data_type)
        data = data_holder.data[column_list]
        data.to_csv(self.export_file_path, encoding=self._encoding, sep='\t', index=False)
