import pathlib

from .base import Exporter, DataHolderProtocol


class TxtAsIs(Exporter):
    """Test class to export data 'as is' to a text file"""

    def __init__(self, path: str | pathlib.Path, **kwargs):
        self._path = pathlib.Path(path)
        self._encoding = kwargs.get('encoding', 'cp1252')

    def __repr__(self) -> str:
        return f'Exporter: {self.__class__.__name__}'

    def _export(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data.to_csv(self._path, encoding=self._encoding, sep='\t', index=False)
