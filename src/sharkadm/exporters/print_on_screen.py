import pathlib

from .base import Exporter, DataHolderProtocol


class PrintDataFrame(Exporter):
    """Prints the dataframe on screen"""

    @staticmethod
    def get_exporter_description() -> str:
        return 'Prints data on screen'

    def _export(self, data_holder: DataHolderProtocol) -> None:
        print(data_holder.data)
