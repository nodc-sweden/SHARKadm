import pathlib

from nodc_csr import CsrMetadata, generate_csr

from sharkadm.data import PolarsDataHolder
from sharkadm.exporters.base import PolarsFileExporter


class CruiseSummaryReport(PolarsFileExporter):
    """Exporter of Cruise Summary Report in XML format."""

    def __init__(
        self,
        identifier: str,
        projects: list[str],
        general_ocean_areas: list[str],
        specific_ocean_areas: list[str],
        objective_of_cruise: str,
        export_directory: str | pathlib.Path | None = None,
        export_file_name: str | pathlib.Path | None = None,
        cruise_expedition_leader: str | None = None,
        port_of_departure: str | None = None,
        port_of_return: str | None = None,
        **kwargs,
    ):
        super().__init__(export_directory, export_file_name, **kwargs)

        self.identifier = identifier
        self.projects = projects
        self.general_ocean_areas = general_ocean_areas
        self.specific_ocean_areas = specific_ocean_areas
        self.objective_of_cruise = objective_of_cruise

        if cruise_expedition_leader:
            self.cruise_expedition_leader = cruise_expedition_leader

        if port_of_departure:
            self.port_of_departure = port_of_departure

        if port_of_return:
            self.port_of_return = port_of_return

    @staticmethod
    def get_exporter_description():
        return "Creates a Cruise Summary Report in XML format based on the input data."

    def _export(
        self,
        data_holder: PolarsDataHolder,
    ) -> None:
        df = data_holder.data
        metadata = CsrMetadata(
            identifier=self.identifier,
            projects=self.projects,
            general_ocean_areas=self.general_ocean_areas,
            specific_ocean_areas=self.specific_ocean_areas,
            objective_of_cruise=self.objective_of_cruise,
            cruise_expedition_leader=self.cruise_expedition_leader,
            port_of_departure=self.port_of_departure,
            port_of_return=self.port_of_return,
        )

        xml = generate_csr(df, metadata)

        xml.write(
            self.export_file_path,
            encoding="utf-8",
            xml_declaration=True,
        )
