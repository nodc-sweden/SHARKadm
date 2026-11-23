import io
import pathlib
from typing import ClassVar

import polars as pl

from sharkadm.data import BasevocDataHolder
from sharkadm.exporters.base import PolarsFileExporter


class OdvExporter(PolarsFileExporter):
    """
    Exporter for Ocean Data Viewer (ODV) format.
    """

    EXPORT_COLUMN_NAMES: ClassVar[dict[str, str]] = {
        "cruise": "Cruise",
        "reported_station_name": "Station",
        "type": "Type",
        "sample_iso_datetime": "yyyy-mm-ddThh:mm:ss.sss",
        "visit_reported_longitude": "Longitude [degrees_east]",
        "visit_reported_latitude": "Latitude [degrees_north]",
        "local_cdi_id": "LOCAL_CDI_ID",
        "distributor": "EDMO_code",
    }
    export_index_name = "time_ISO8601 [yyyy-mm-ddThh:mm:ss.sss]"
    valid_data_holders = ("BasevocDataHolder",)

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

    def _export(self, data_holder: BasevocDataHolder) -> None:
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
            if col == "sample_iso_datetime":
                expressions.append(
                    pl.col(col)
                    .cast(pl.String)
                    .str.to_datetime(strict=True)
                    .dt.strftime("%Y-%m-%dT%H:%M:%S%.3f")
                    .alias(output_name)
                )
            else:
                expressions.append(pl.col(col).alias(output_name))
            metadata_output_names.append(output_name)

        expressions.append(
            pl.col(index_col)
            .str.to_datetime(strict=True)
            .dt.strftime("%Y-%m-%dT%H:%M:%S%.3f")
            .alias(self.export_index_name)
        )

        expressions.append(pl.col(f"qc_sdn_{index_col}"))

        for i, col in enumerate(measurement_cols):
            local_name = data_holder.selected_compound["Value column in data file"]
            expressions.append(pl.col(col).alias(local_name))

            qc_col = f"{data_holder.selected_compound['Parameter'].lower()}_qflag_L20"
            if qc_col not in data.columns:
                raise ValueError(
                    f"QC column '{qc_col}' not found for measurement '{col}'"
                )
            expressions.append(pl.col(qc_col))

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

        header_lines = self._create_header(data_holder)
        csv_buffer = io.StringIO()
        output.write_csv(csv_buffer, separator="\t", null_value="", include_header=False)

        output_column_names = [
            *metadata_output_names,
            self.export_index_name,
            "QV:SEADATANET",  # QC for indexing column
        ]

        for col in measurement_cols:
            local_name = col
            unit = data_holder.selected_compound["unit_ODV_label"]
            column_name = f"{local_name} {unit}" if unit else local_name

            output_column_names.append(column_name)
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

    def _create_header(self, data_holder: BasevocDataHolder):
        header_lines = []
        header_lines.append("//")
        header_lines.append("//SDN_parameter_mapping")
        header_lines.append(
            "//<subject>SDN:LOCAL:time_ISO8601</subject><object>SDN:P01::DTUT8601</object><units>SDN:P06::TISO</units>"
        )
        parameters = data_holder.data_columns
        for param in parameters:
            subject = f"<subject>SDN:LOCAL:{param}</subject>"
            p01_code = data_holder.selected_compound["P01 vocab"]
            object_part = f"<object>SDN:P01::{p01_code}</object>"
            p06_code = data_holder.selected_compound["P06"]
            units_part = f"<units>SDN:P06::{p06_code}</units>"

            header_line = f"//{subject}{object_part}{units_part}"

            header_lines.append(header_line)
        header_lines.append("//")
        return header_lines

    @staticmethod
    def _format_odv_datetime(column: str) -> pl.Expr:
        return pl.col(column).dt.strftime("%Y-%m-%dT%H:%M:%S%.3f")
