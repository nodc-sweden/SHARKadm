import pathlib

from .base import Exporter, DataHolderProtocol
from SHARKadm import utils


class SimpleStandardFormat(Exporter):
    def __init__(self,
                 export_directory: str | pathlib.Path | None = None,
                 export_file_name: str | pathlib.Path | None = None,
                 **kwargs):
        super().__init__(**kwargs)
        if not export_directory:
            export_directory = utils.get_export_directory()
        self._export_directory = pathlib.Path(export_directory)
        self._export_file_name = export_file_name
        self._encoding = kwargs.get('encoding', 'cp1252')

    @staticmethod
    def get_exporter_description() -> str:
        return ''

    def _export(self, data_holder: DataHolderProtocol) -> None:
        pass
