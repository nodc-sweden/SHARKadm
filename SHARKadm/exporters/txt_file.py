import pathlib

from .base import Exporter, DataHolderProtocol
from SHARKadm import utils


class TxtAsIs(Exporter):
    """Test class to export data 'as is' to a text file"""

    def __init__(self,
                 export_directory: str | pathlib.Path | None = None,
                 export_file_name: str | pathlib.Path | None = None,
                 **kwargs):
        super().__init__()
        if not export_directory:
            export_directory = utils.get_export_directory()
        self._export_directory = pathlib.Path(export_directory)
        if not export_file_name:
            export_file_name = 'simple_export.txt'
        self._export_file_name = export_file_name
        self._encoding = kwargs.get('encoding', 'cp1252')

    @property
    def export_file_path(self):
        return pathlib.Path(self._export_directory, self._export_file_name)

    @staticmethod
    def get_exporter_description() -> str:
        return 'Writes data "as is" to the specified file.'

    def _export(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data.to_csv(self.export_file_path, encoding=self._encoding, sep='\t', index=False)
