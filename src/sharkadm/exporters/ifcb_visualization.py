import pathlib

from sharkadm import utils
from sharkadm.ifcb import create_ifcb_visualization_files
from .base import FileExporter, DataHolderProtocol


class IfcbVisualizationFiles(FileExporter):

    def __init__(self,
                 export_directory: str | pathlib.Path | None = None,
                 append: bool = True,
                 **kwargs):
        self._append = append
        if not export_directory:
            export_directory = utils.get_export_directory('ifcb_visualization')
        super().__init__(export_directory=export_directory,
                         **kwargs)

    @staticmethod
    def get_exporter_description() -> str:
        return 'Creates IFC visualization json-files'

    def _export(self, data_holder: DataHolderProtocol) -> None:
        create_ifcb_visualization_files(data_holder.data, directory=self.export_directory, append=self._append)
