import pathlib

from .base import FileExporter, DataHolderProtocol
from sharkadm import utils
from sharkadm.config import get_import_matrix_mapper


class TxtAsIs(FileExporter):
    """Test class to export data 'as is' to a text file"""

    def __init__(self,
                 export_directory: str | pathlib.Path | None = None,
                 export_file_name: str | pathlib.Path | None = None,
                 header_as: str | None = None,
                 **kwargs):
        super().__init__(export_directory=export_directory,
                         export_file_name=export_file_name,
                         **kwargs)
        self._header_as = header_as

    @staticmethod
    def get_exporter_description() -> str:
        return 'Writes data "as is" to the specified file.'

    def _export(self, data_holder: DataHolderProtocol) -> None:
        df = data_holder.data.copy(deep=True)
        if self._header_as:
            mapper = get_import_matrix_mapper(data_type=data_holder.data_type, import_column=self._header_as)
            new_column_names = [mapper.get_external_name(col) for col in df.columns]
            df.columns = new_column_names
        if not self._export_file_name:
            self._export_file_name = f'data_as_is_{data_holder.dataset_name}.txt'
        df.to_csv(self.export_file_path, encoding=self._encoding, sep='\t', index=False)

