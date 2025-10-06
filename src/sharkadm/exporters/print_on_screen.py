from sharkadm.data import PandasDataHolder

from .base import Exporter, PolarsExporter


class PrintDataFrame(PolarsExporter):
    """Prints the dataframe on screen"""

    @staticmethod
    def get_exporter_description() -> str:
        return "Prints data on screen"

    def _get_default_file_name(self):
        return ""

    def _export(self, data_holder: PandasDataHolder) -> None:
        print(data_holder.data)
