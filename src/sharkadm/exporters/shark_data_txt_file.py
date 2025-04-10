import pathlib

from sharkadm.config import get_column_views_config

from .base import DataHolderProtocol, FileExporter
from ..transformers.base import PolarsDataHolderProtocol


class SHARKdataTxt(FileExporter):
    """Writes data to file filtered by the columns specified for the given data type in
    column_views."""

    def __init__(
        self,
        export_directory: str | pathlib.Path | None = None,
        export_file_name: str | pathlib.Path | None = None,
        **kwargs,
    ):
        if not export_file_name:
            export_file_name = "shark_data.txt"
        super().__init__(export_directory, export_file_name, **kwargs)
        self._column_views = get_column_views_config()

    @staticmethod
    def get_exporter_description() -> str:
        return (
            "Writes data to file filtered by the columns specified for the given data "
            "type in column_views."
        )

    def _export(self, data_holder: DataHolderProtocol) -> None:
        column_list = self._column_views.get_columns_for_view(
            view=data_holder.data_type_internal
        )
        data = data_holder.data[column_list]
        data.to_csv(self.export_file_path, encoding=self._encoding, sep="\t", index=False)


class SHARKdataTxtAsGiven(FileExporter):
    exclude_columns = ("source",)

    def __init__(
        self,
        export_directory: str | pathlib.Path | None = None,
        export_file_name: str | pathlib.Path | None = None,
        exclude_columns: list[str] = None,
        **kwargs,
    ):
        super().__init__(export_directory, export_file_name, **kwargs)
        if not export_file_name:
            export_file_name = "shark_data.txt"
        self._export_file_name = export_file_name
        exclude_columns = exclude_columns or []
        self.exclude_columns = tuple(list(self.exclude_columns) + exclude_columns)

    @property
    def export_file_path(self):
        return pathlib.Path(self._export_directory, self._export_file_name)

    @staticmethod
    def get_exporter_description() -> str:
        exclude_columns_string = ", ".join(SHARKdataTxtAsGiven.exclude_columns)
        return (
            "Writes data to file with all given columns except the these: "
            f"{exclude_columns_string}."
        )

    def _export(self, data_holder: DataHolderProtocol) -> None:
        columns = [
            col for col in data_holder.data.columns if col not in self.exclude_columns
        ]
        df = data_holder.data[columns]
        df.to_csv(self.export_file_path, encoding=self._encoding, sep="\t", index=False)


class PolarsSHARKdataTxtAsGiven(FileExporter):
    exclude_columns = ("source",)

    def __init__(
        self,
        export_directory: str | pathlib.Path | None = None,
        export_file_name: str | pathlib.Path | None = None,
        exclude_columns: list[str] = None,
        **kwargs,
    ):
        super().__init__(export_directory, export_file_name, **kwargs)
        if not export_file_name:
            export_file_name = "shark_data.txt"
        self._export_file_name = export_file_name
        exclude_columns = exclude_columns or []
        self.exclude_columns = tuple(list(self.exclude_columns) + exclude_columns)

    @property
    def export_file_path(self):
        return pathlib.Path(self._export_directory, self._export_file_name)

    @staticmethod
    def get_exporter_description() -> str:
        exclude_columns_string = ", ".join(PolarsSHARKdataTxtAsGiven.exclude_columns)
        return (
            "Writes data to file with all given columns except the these: "
            f"{exclude_columns_string}."
        )

    def _export(self, data_holder: PolarsDataHolderProtocol) -> None:
        columns = [
            col for col in data_holder.data.columns if col not in self.exclude_columns
        ]
        df = data_holder.data[columns]
        df.write_csv(self.export_file_path, encoding=self._encoding, separator="\t")
