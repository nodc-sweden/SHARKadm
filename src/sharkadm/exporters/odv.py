import pathlib
from typing import ClassVar

import polars as pl

from sharkadm.data import NewDataHolder
from sharkadm.exporters.base import PolarsFileExporter


class OdvExporter(PolarsFileExporter):
    """Exporter for Ocean Data Viewer (ODV) format"""

    LOCAL_CODE_MAPPING: ClassVar[dict[str, str]] = {"63.027": "DMS"}

    P01_CODE_MAPPING: ClassVar[dict[str, str]] = {"63.027": "FLUXDMS1"}

    P06_CODE_MAPPING: ClassVar[dict[str, str]] = {"63.027": "UUUD"}

    # Metadata fields in ODV
    EXPORT_COLUMN_NAMES: ClassVar[dict[str, str]] = {
        "Cruise": "Cruise",
        "reported_station_name": "Station",
        "Type": "Type",
        "sample_iso_datetime": "yyyy-mm-ddThh:mm:ss.sss",
        "visit_reported_longitude": "Longitude [degrees_east]",
        "visit_reported_latitude": "Latitude [degrees_north]",
        "LOCAL_CDI_ID": "LOCAL_CDI_ID",
        "distributor": "EDMO_code",
        # "CDI_partner": "EDMO_code",
        "Bot. Depth [m]": "Bot. Depth [m]",
    }
    export_index_name = "time_ISO8601 [yyyy-mm-ddThh:mm:ss.sss]"

    def __init__(
        self,
        export_directory: str | pathlib.Path | None = None,
        export_file_name: str | pathlib.Path | None = None,
        **kwargs,
    ):
        if not export_file_name:
            export_file_name = "odv_export.txt"
        super().__init__(
            export_directory=export_directory,
            export_file_name=export_file_name,
            **kwargs,
        )

    @staticmethod
    def get_exporter_description() -> str:
        """Description of what this exporter does"""
        return "Exports data to Ocean Data Viewer (ODV) format"

    def _export(self, data_holder: NewDataHolder) -> None:
        """
        Args:
            data_holder: The data holder containing the data to export
        """
        self._log(f"Exporting to ODV format: {self.export_file_path}")

        data = data_holder.data

        metadata_cols = list(self.EXPORT_COLUMN_NAMES.keys())
        index_col = data_holder.indexing_column
        measurement_cols = list(data_holder.data_columns)

        required_cols = set([*metadata_cols, index_col, *measurement_cols])

        missing = required_cols - set(data.columns)

        if missing:
            if missing:
                data = data.with_columns([pl.lit(None).alias(col) for col in missing])

        expressions = []
        metadata_output_names = []
        for col in metadata_cols:
            output_name = self.EXPORT_COLUMN_NAMES.get(col, col)
            expressions.append(pl.col(col).alias(output_name))
            metadata_output_names.append(output_name)

        expressions.append(pl.col(index_col).alias(self.export_index_name))

        for col in measurement_cols:
            output_name = self.LOCAL_CODE_MAPPING.get(col, col)
            expressions.append(pl.col(col).alias(output_name))
            qc_col = f"qc_{col}"
            if qc_col not in data.columns:
                raise ValueError(
                    f"QC column '{qc_col}' not found for measurement '{col}'"
                )
            expressions.append(pl.col(qc_col).alias("QV:SEADATANET"))

        output = data.select(expressions)
        output = output.with_columns(
            [
                pl.when(pl.int_range(pl.len()) == 0)
                .then(pl.col(name))
                .otherwise(pl.lit(None))
                .alias(name)
                for name in metadata_output_names
            ]
        )

        output.write_csv(
            self.export_file_path,
            separator="\t",
            null_value="",
        )

    def _create_header():
        # TODO
        pass

    def _create_param_header(
        self, local_code: str, p01_code: str | None, p06_code: str | None
    ) -> str:
        """
        Format a single line in the ODV header.
        """
        local_code = ""
        subject = f"<subject>SDN:LOCAL:{local_code}</subject>"
        p01_code = ""
        object_part = f"<object>SDN:P01::{p01_code}</object>"
        p06_code = ""
        units_part = f"<units>SDN:P06::{p06_code}</units>"

        header_line = f"//{subject}{object_part}{units_part}"

        return header_line
