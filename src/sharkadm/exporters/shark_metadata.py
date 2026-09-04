import json
import pathlib

from sharkadm.data import PolarsDataHolder
from sharkadm.sharkadm_logger import adm_logger

from .base import PolarsFileExporter

delivery_data = None
try:
    from shark_metadata import delivery_data
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. '
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class PolarsSHARKMetadata(PolarsFileExporter):
    """
    Creates the files:
    shark_metadata.json
    readme.txt
    """

    def __init__(
        self,
        export_directory: str | pathlib.Path | None = None,
        export_file_name: str | pathlib.Path | None = None,
        **kwargs,
    ):
        super().__init__(export_directory=export_directory, **kwargs)
        if not export_file_name:
            export_file_name = "shark_metadata.json"
        self._export_file_name = export_file_name

    @staticmethod
    def get_exporter_description() -> str:
        return "Creates the shark_metadata.txt file"

    def _export(self, data_holder: PolarsDataHolder) -> None:
        if not delivery_data:
            self._log(
                "Could export shark_metadata_auto.txt. "
                "Package shark-metadata not found/installed!",
                level=adm_logger.ERROR,
            )
            return
        ddata = delivery_data.DeliveryData(data_holder.data)
        meta = ddata.generate_metadata()
        with open(self.export_file_path, "w") as fid:
            json.dump(meta, fid)
        readme = ddata.generate_readme()
        with open(
            self.export_file_path.parent / "readme.txt", "w", encoding="utf-8"
        ) as f:
            f.write(readme)
