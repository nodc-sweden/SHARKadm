import pathlib

from sharkadm.data import PolarsDataHolder
from sharkadm.exporters.base import PolarsFileExporter
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.utils.paths import get_next_incremented_file_path

generate_csr = None
CsrUserInputs = None
try:
    from nodc_csr import CsrUserInputs, generate_csr
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. '
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class CruiseSummaryReport(PolarsFileExporter):
    """Exporter of Cruise Summary Report in XML format."""

    def __init__(
        self,
        identifier: str,
        projects: list[str],
        specific_ocean_areas: list[str],
        objective_of_cruise: str,
        export_directory: str | pathlib.Path | None = None,
        export_file_name: str | pathlib.Path | None = None,
        cruise_expedition_leader: str | None = None,
        port_of_departure: str | None = None,
        port_of_return: str | None = None,
        **kwargs,
    ):
        super().__init__(
            export_directory=export_directory,
            export_file_name=export_file_name,
            **kwargs,
        )
        self._identifier = identifier
        self._projects = projects
        self._specific_ocean_areas = specific_ocean_areas
        self._objective_of_cruise = objective_of_cruise
        self._cruise_expedition_leader = cruise_expedition_leader
        self._port_of_departure = port_of_departure
        self._port_of_return = port_of_return

    @staticmethod
    def get_exporter_description() -> str:
        return "Creates a Cruise Summary Report in XML format based on the input data."

    def _export(self, data_holder: PolarsDataHolder) -> None:
        if not generate_csr:
            self._log(
                "Could not export Cruise Summary Report. "
                "Package nodc_csr not found/installed!",
                level=adm_logger.ERROR,
            )
            return
        if not self._export_file_name:
            self._export_file_name = f"csr_{data_holder.dataset_name}.xml"

        metadata = CsrUserInputs(
            identifier=self._identifier,
            projects=self._projects,
            specific_ocean_areas=self._specific_ocean_areas,
            objective_of_cruise=self._objective_of_cruise,
            cruise_expedition_leader=self._cruise_expedition_leader,
            port_of_departure=self._port_of_departure,
            port_of_return=self._port_of_return,
        )
        xml = generate_csr(data_holder.data, metadata, data_holder.config)

        try:
            xml.write(self.export_file_path, encoding="utf-8", xml_declaration=True)
        except PermissionError:
            self._export_file_name = get_next_incremented_file_path(self.export_file_path)
            xml.write(self.export_file_path, encoding="utf-8", xml_declaration=True)
