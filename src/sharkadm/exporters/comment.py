import polars as pl

from ..data import PolarsDataHolder
from .base import PolarsFileExporter


class ExportComment(PolarsFileExporter):
    def __init__(self, unique: bool = True, **kwargs):
        self._unique = unique
        super().__init__(**kwargs)

    @staticmethod
    def get_exporter_description() -> str:
        return "Creates a summary file of comment columns in data"

    def _export(self, data_holder: PolarsDataHolder) -> None:
        if not self._export_file_name:
            self._export_file_name = f"comment_summary_{data_holder.dataset_name}.txt"
        lines = list()
        lines.append("\t".join(["column", "comment", "row_number"]))
        for col in self._get_comment_columns(data_holder):
            df = data_holder.data.filter(pl.col(col) != "")
            if self._unique:
                df = df.unique(col)
            for (row_nr, com), data in df.group_by(["row_number", col]):
                line = [col, com, str(row_nr)]
                lines.append("\t".join(line))

        with open(self.export_file_path, "w") as fid:
            fid.write("\n".join(lines))

    @staticmethod
    def _get_comment_columns(data_holder: PolarsDataHolder):
        return [col for col in data_holder.data.columns if "comment" in col]
