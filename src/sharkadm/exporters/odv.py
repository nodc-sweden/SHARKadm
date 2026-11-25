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
            data_holder: Data holder containing the data to export.
        """
        self._log(f"Exporting to ODV format: {self.export_file_path}")

        data = data_holder.data
        existing_columns = set(data.columns)
        index_col = data_holder.indexing_column
        measurement_cols = data_holder.data_columns
        metadata_cols = list(self.EXPORT_COLUMN_NAMES)
        qc_columns = data_holder.qc_columns
        compound = data_holder.selected_compound

        # Validate that the required information exists in selected_compound
        required_compound_fields = (
            "Value column in data file",
            "Parameter",
            "P01 vocab",
            "P06",
            "unit_ODV_label",
        )
        missing_compound_fields = [
            field for field in required_compound_fields if field not in compound
        ]
        if missing_compound_fields:
            raise ValueError(
                f"Missing selected-compound configuration for "
                f"'{data_holder.dataset_name}': "
                f"{', '.join(missing_compound_fields)}"
            )

        # Validate that the expected columns exist in the data holder
        required_columns = {
            *metadata_cols,
            index_col,
            *measurement_cols,
        }
        missing_columns = sorted(required_columns - existing_columns)
        if missing_columns:
            raise ValueError(
                f"Cannot export '{data_holder.dataset_name}': missing required "
                f"columns: {', '.join(missing_columns)}"
            )

        # Prepare output on format
        # [metadata_columns*,index_col, index_qc, measurement_col, measurement_qx,...]
        # A separate list of the output column name is kept because the names of the QC
        # columns are duplicates, and this is not allowed in polars dataframes
        expressions = []
        output_column_names = []  # The headers used in the export

        # Metadata
        for col in metadata_cols:
            if col == "sample_iso_datetime":
                expression = (
                    pl.col(col)
                    .cast(pl.String)
                    .str.to_datetime(strict=True)
                    .dt.strftime("%Y-%m-%dT%H:%M:%S%.3f")
                )
            else:
                expression = pl.col(col)

            expressions.append(expression)
            output_column_names.append(self.EXPORT_COLUMN_NAMES[col])

        # Index and index QC
        expressions.append(
            pl.col(index_col)
            .cast(pl.String)
            .str.to_datetime(strict=True)
            .dt.strftime("%Y-%m-%dT%H:%M:%S%.3f")
            .alias(self.export_index_name)
        )
        expression.append(pl.col(f"qc_sdn_{index_col}"))
        output_column_names.extend([self.export_index_name, "QV:SEADATANET"])

        # Measurements
        for col in measurement_cols:
            if col not in qc_columns:
                raise ValueError(
                    f"Cannot export '{data_holder.dataset_name}': no QC-column "
                    f"mapping is configured for measurement '{col}'"
                )

            qc_col = qc_columns[col]

            if qc_col not in existing_columns:
                raise ValueError(
                    f"Cannot export '{data_holder.dataset_name}': missing QC "
                    f"column '{qc_col}' for measurement '{col}'"
                )

            local_name = compound["Value column in data file"]
            unit = compound.get("unit_ODV_label")
            measurement_header = f"{local_name} [{unit}]" if unit else local_name

            expressions.extend([pl.col(col), pl.col(qc_col)])
        output_column_names.extend([measurement_header, "QV:SEADATANET"])

        output = data.select(expressions)
        output = output.with_columns(
            [
                pl.when(pl.int_range(pl.len()) == 0)
                .then(pl.col(name))
                .otherwise(pl.lit(None))
                .alias(name)
                for name in list(self.EXPORT_COLUMN_NAMES)
            ]
        )

        csv_buffer = io.StringIO()
        output.write_csv(
            csv_buffer,
            separator="\t",
            null_value="",
            include_header=False,
        )

        header_lines = self._create_header(data_holder)

        with open(
            self.export_file_path,
            "w",
            encoding="utf-8",
            newline="",
        ) as fid:
            for line in header_lines:
                fid.write(line + "\n")

            fid.write("\t".join(output_column_names) + "\n")
            fid.write(csv_buffer.getvalue())

    def _create_header(self, data_holder: BasevocDataHolder):
        header_lines = ["//", "//SDN_parameter_mapping"]

        header_lines.append(
            "//<subject>SDN:LOCAL:time_ISO8601</subject>"
            "<object>SDN:P01::DTUT8601</object>"
            "<units>SDN:P06::TISO</units>"
        )

        for parameter in data_holder.data_columns:
            p01_code = data_holder.selected_compound["P01 vocab"]
            p06_code = data_holder.selected_compound["P06"]

            header_lines.append(
                f"//<subject>SDN:LOCAL:{parameter}</subject>"
                f"<object>SDN:P01::{p01_code}</object>"
                f"<units>SDN:P06::{p06_code}</units>"
            )

        header_lines.append("//")
        return header_lines
