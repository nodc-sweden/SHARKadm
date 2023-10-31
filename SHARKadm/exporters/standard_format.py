import pathlib

from .base import Exporter, DataHolderProtocol


class SimpleStandardFormat(Exporter):
    def __init__(self, path: str | pathlib.Path, **kwargs):
        super().__init__(**kwargs)
        self._path = pathlib.Path(path)
        self._encoding = kwargs.get('encoding', 'cp1252')

    @staticmethod
    def get_exporter_description() -> str:
        return ''

    def _export(self, data_holder: DataHolderProtocol) -> None:
        pass
