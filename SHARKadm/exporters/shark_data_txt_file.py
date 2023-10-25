import pathlib

from .base import Exporter, DataHolderProtocol

from SHARKadm.config import get_column_views_config


class SHARKdataTxt(Exporter):
    """Writes data to file filtered by the columns specified for the given data type in column_views."""

    def __init__(self, path: str | pathlib.Path, **kwargs):
        super().__init__()
        self._path = pathlib.Path(path)
        if self._path.is_dir():
            self._path = self._path / 'shark_data.txt'
        self._encoding = kwargs.get('encoding', 'cp1252')
        self._column_views = get_column_views_config()

    @staticmethod
    def get_exporter_description() -> str:
        return 'Writes data to file filtered by the columns specified for the given data type in column_views.'

    def _export(self, data_holder: DataHolderProtocol) -> None:
        column_list = self._column_views.get_columns_for_view(view=data_holder.data_type)
        data = data_holder.data[column_list]
        data.to_csv(self._path, encoding=self._encoding, sep='\t', index=False)
