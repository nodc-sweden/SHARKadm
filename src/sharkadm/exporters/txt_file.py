import pathlib

from sharkadm.data import PolarsDataHolder
from sharkadm.exporters.base import PolarsFileExporter
from sharkadm.utils.paths import get_next_incremented_file_path


class PolarsTxtAsIs(PolarsFileExporter):
    def __init__(
        self,
        export_directory: str | pathlib.Path | None = None,
        export_file_name: str | pathlib.Path | None = None,
        **kwargs,
    ):
        super().__init__(
            export_directory=export_directory,
            export_file_name=export_file_name,
            **kwargs,
        )

    @staticmethod
    def get_exporter_description() -> str:
        return 'Writes data "as is" to the specified file.'

    def _export(self, data_holder: PolarsDataHolder) -> None:
        df = self._get_mapped_header_dataframe(data_holder)
        if not self._export_file_name:
            self._export_file_name = f"data_as_is_{data_holder.dataset_name}.txt"

        pandas_df = df.to_pandas()
        try:
            pandas_df.to_csv(
                self.export_file_path, encoding=self._encoding, sep="\t", index=False
            )
        except PermissionError:
            self._export_file_name = get_next_incremented_file_path(self.export_file_path)
            pandas_df.to_csv(
                self.export_file_path, encoding=self._encoding, sep="\t", index=False
            )


class PolarsTxtWithImportedColumns(PolarsFileExporter):
    def __init__(
        self,
        export_directory: str | pathlib.Path | None = None,
        export_file_name: str | pathlib.Path | None = None,
        additional_columns: list[str] | None = None,
        header_as: str | None = None,
        **kwargs,
    ):
        super().__init__(
            export_directory=export_directory,
            export_file_name=export_file_name,
            **kwargs,
        )
        self._additional_columns = additional_columns or []
        self._header_as = header_as

    @staticmethod
    def get_exporter_description() -> str:
        return (
            "Writes data to txt file. "
            "Data includes columns that were imported plus additional columns "
            'given in "additional_columns". '
            'Option also to translate column via "header_as"'
        )

    def _export(self, data_holder: PolarsDataHolder) -> None:
        imported_columns = data_holder.imported_columns[:]
        columns = imported_columns + [
            col for col in self._additional_columns if col not in imported_columns
        ]
        columns = [col for col in columns if col in data_holder.data.columns]
        df = data_holder.data[columns]
        df = self._get_mapped_header_dataframe(data_holder, data=df)
        pandas_df = df.to_pandas()
        try:
            pandas_df.to_csv(
                self.export_file_path, encoding=self._encoding, sep="\t", index=False
            )
        except PermissionError:
            self._export_file_name = get_next_incremented_file_path(self.export_file_path)
            pandas_df.to_csv(
                self.export_file_path, encoding=self._encoding, sep="\t", index=False
            )
