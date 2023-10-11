import pathlib

from .base import Exporter, DataHolderProtocol


class PrintDataFrame(Exporter):
    """Prints the dataframe on screen"""

    def __repr__(self) -> str:
        return f'Exporter: {self.__class__.__name__}'

    def _export(self, data_holder: DataHolderProtocol) -> None:
        print(data_holder.data)
