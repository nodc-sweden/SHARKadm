import pathlib

from sharkadm.data import PolarsDataHolder
from sharkadm.utils import statistics

from ..utils.paths import get_next_incremented_file_path
from .base import PolarsFileExporter, PolarsExporter


class PolarsPrintStatistics(PolarsExporter):
    @staticmethod
    def get_exporter_description() -> str:
        return "Print statistics on screen"

    def _export(self, data_holder: PolarsDataHolder) -> None:
        mark_len = 100
        print("=" * mark_len)
        print(f"Statistics for: {data_holder.dataset_name}")
        print("-" * mark_len)
        for key, value in statistics.get_data_holder_statistics(
            data_holder=data_holder
        ).items():
            print(f"{key.ljust(40)}: {value}")
        print("-" * mark_len)


class PolarsStatisticsToTxt(PolarsFileExporter):
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
        return "Writes statistics to txt file"

    def _export(self, data_holder: PolarsDataHolder) -> None:
        if not self._export_file_name:
            self._export_file_name = f"statistics_{data_holder.dataset_name}.txt"
        stats = statistics.get_data_holder_statistics(data_holder=data_holder)

        try:
            with open(self.export_file_path, "w", encoding=self._encoding) as fid:
                for key, value in stats.items():
                    fid.write(f"{key.ljust(40)}\t{value}\n")
        except PermissionError:
            self._export_file_name = get_next_incremented_file_path(self.export_file_path)
            with open(self.export_file_path, "w", encoding=self._encoding) as fid:
                for key, value in stats.items():
                    fid.write(f"{key.ljust(40)}\t{value}\n")
