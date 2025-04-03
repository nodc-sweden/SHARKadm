from sharkadm.data import PandasDataHolder
from .base import FileExporter
from sharkadm import transformers
from sharkadm import validators
from sharkadm import exporters


class TransformersSummaryFile(FileExporter):
    @staticmethod
    def get_exporter_description() -> str:
        return "Creates a summary of all transformers available in the system"

    def _export(self, data_holder: PandasDataHolder) -> None:
        if not self.export_file_name:
            self._export_file_name = "sharkadm_transformers_summary.txt"
        transformers.write_transformers_description_to_file(self.export_file_path)


class ValidatorsSummaryFile(FileExporter):
    @staticmethod
    def get_exporter_description() -> str:
        return "Creates a summary of all validators available in the system"

    def _export(self, data_holder: PandasDataHolder) -> None:
        if not self.export_file_name:
            self._export_file_name = "sharkadm_validators_summary.txt"
        validators.write_validators_description_to_file(self.export_file_path)


class ExportersSummaryFile(FileExporter):
    @staticmethod
    def get_exporter_description() -> str:
        return "Creates a summary of all exporters available in the system"

    def _export(self, data_holder: PandasDataHolder) -> None:
        if not self.export_file_name:
            self._export_file_name = "sharkadm_exporters_summary.txt"
        exporters.write_exporters_description_to_file(self.export_file_path)
