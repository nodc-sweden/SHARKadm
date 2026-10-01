from ..data import PolarsDataHolder
from .base import PolarsFileExporter


class ExportSharklogArchiveDiff(PolarsFileExporter):
    @staticmethod
    def get_exporter_description() -> str:
        return "Creates a LIMS jellyfish txt file"

    def _export(self, data_holder: PolarsDataHolder) -> None:
        pass
