from ..config import get_header_mapper_from_data_holder
from ..data import PolarsDataHolder
from .base import FileExporter


class ExportJellyfishRowsFromLimsExport(FileExporter):
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

        df = data_holder.data.with_columns()
        if self._header_as:
            mapper = get_header_mapper_from_data_holder(
                data_holder, import_column=self._header_as
            )
            if not mapper:
                self._log(f"Could not find mapper using header_as = {self._header_as}")
                return
            # new_column_names = [mapper.get_external_name(col) for col in df.columns]
            df = df.rename(mapper.reverse_mapper, strict=False)
            # df.columns = new_column_names

        df = df.drop("row_number", "source")
        df = df.to_pandas()
        df.to_csv(self.export_file_path, encoding=self._encoding, sep="\t", index=False)
