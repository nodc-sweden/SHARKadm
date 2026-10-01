import io
import pathlib
from typing import ClassVar

import polars as pl

from sharkadm.data import NewDataHolder
from sharkadm.exporters.base import PolarsFileExporter


class OdvExporter(PolarsFileExporter):
    """
    Exporter for Ocean Data Viewer (ODV) format.
    """

    # TODO get full mapping
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

        expressions.append(
            pl.col(index_col)
            .str.to_datetime(strict=True)
            .dt.strftime("%Y-%m-%dT%H:%M:%S%.3f")
            .alias(self.export_index_name)
        )

        # Use a unique internal name — ODV header will be fixed later.
        expressions.append(pl.col(f"qc_sdn_{index_col}").alias("__qc_index"))

        for i, col in enumerate(measurement_cols):
            output_name = self.LOCAL_CODE_MAPPING.get(col, col)
            expressions.append(pl.col(col).alias(output_name))

            qc_col = f"qc_sdn_{col}"
            if qc_col not in data.columns:
                raise ValueError(
                    f"QC column '{qc_col}' not found for measurement '{col}'"
                )

            # Unique name inside Polars
            expressions.append(pl.col(qc_col).alias(f"__qc_{i}"))

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

        header_lines = self._create_header(measurement_cols)
        csv_buffer = io.StringIO()
        output.write_csv(csv_buffer, separator="\t", null_value="", include_header=False)

        output_column_names = [
            *metadata_output_names,
            self.export_index_name,
            "QV:SEADATANET",  # QC for indexing column
        ]

        for col in measurement_cols:
            output_column_names.append(self.LOCAL_CODE_MAPPING.get(col, col))
            output_column_names.append("QV:SEADATANET")

        with open(
            self.export_file_path,
            "w",
            encoding="utf-8",
            newline="",
        ) as f:
            for line in header_lines:
                f.write(line + "\n")

            # ODV allows duplicate column names.
            f.write("\t".join(output_column_names) + "\n")
            f.write(csv_buffer.getvalue())

    def _create_header(self, parameters: list[str]):
        header_lines = []
        header_lines.append("//")
        header_lines.append("//SDN_parameter_mapping")
        header_lines.append(
            "//<subject>SDN:LOCAL:time_ISO8601</subject><object>SDN:P01::DTUT8601</object><units>SDN:P06::TISO</units>"
        )
        for param in parameters:
            header_lines.append(self._create_param_header(param))
        header_lines.append("//")
        return header_lines

    def _create_param_header(self, parameter) -> str:
        """
        Format a single line in the ODV header.
        """
        local_code = self.LOCAL_CODE_MAPPING[parameter]
        subject = f"<subject>SDN:LOCAL:{local_code}</subject>"
        p01_code = self.P01_CODE_MAPPING[parameter]
        object_part = f"<object>SDN:P01::{p01_code}</object>"
        p06_code = self.P06_CODE_MAPPING[parameter]
        units_part = f"<units>SDN:P06::{p06_code}</units>"

        header_line = f"//{subject}{object_part}{units_part}"

        return header_line

    @staticmethod
    def _format_odv_datetime(column: str) -> pl.Expr:
        return pl.col(column).dt.strftime("%Y-%m-%dT%H:%M:%S%.3f")
