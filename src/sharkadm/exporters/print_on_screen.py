import pathlib

from .base import Exporter
from sharkadm.data import DataHolder
from sharkadm.utils import statistics


class PrintDataFrame(Exporter):
    """Prints the dataframe on screen"""

    @staticmethod
    def get_exporter_description() -> str:
        return 'Prints data on screen'

    def _export(self, data_holder: DataHolder) -> None:
        print(data_holder.data)

