import pathlib

from .base import Exporter, DataHolderProtocol


class TxtAsIs(Exporter):
    """Test class to export data 'as is' to a text file"""

    def __init__(self, path: str | pathlib.Path, **kwargs):
        super().__init__(**kwargs)
        self._path = pathlib.Path(path)
        self._encoding = kwargs.get('encoding', 'cp1252')

    @staticmethod
    def get_exporter_description() -> str:
        return 'Writes data "as is" to the specified file.'

    def _export(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data.to_csv(self._path, encoding=self._encoding, sep='\t', index=False)
