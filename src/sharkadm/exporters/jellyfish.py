from ..data import PolarsDataHolder
from .base import PolarsFileExporter


class ExportJellyfishRowsFromLimsExport(PolarsFileExporter):
    valid_data_holders = ("LimsDataHolder",)

    def __init__(self, header_as: str | None = None, **kwargs):
        super().__init__(**kwargs)

        self._header_as = header_as

    @staticmethod
    def get_exporter_description() -> str:
        return "Creates a LIMS jellyfish txt file"

    def _export(self, data_holder: PolarsDataHolder) -> None:
        if not self._export_file_name:
            self._export_file_name = f"data_jellyfish_{data_holder.dataset_name}.txt"
        df = self._get_mapped_header_dataframe(data_holder)
        df = df.drop("row_number", "source")
        df = df.to_pandas()
        df.to_csv(self.export_file_path, encoding=self._encoding, sep="\t", index=False)
