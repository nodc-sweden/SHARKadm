import polars as pl

from sharkadm import adm_logger
from sharkadm.data import PolarsDataHolder
from sharkadm.transformers import PolarsTransformer


class FormatSerialNumber(PolarsTransformer):
    _serial_number_column = "SERNO"

    @staticmethod
    def get_transformer_description() -> str:
        return "Formatting serial number."

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if (
            column_dtype := data_holder.data.schema[self._serial_number_column]
        ) != pl.Utf8:
            self._log(
                "Could not format serial number because not string. "
                f"Was '{column_dtype}'.",
                level=adm_logger.WARNING,
            )
            return

        transformed_data = (
            data_holder.data.with_columns(
                pl.col(self._serial_number_column).alias("reported_serial_number"),
                pl.col(self._serial_number_column)
                .str.strip_chars()
                .cast(pl.Int16, strict=False)
                .alias("_tmp_integer_SERNO"),
            )
            .with_columns(
                pl.when(pl.col("_tmp_integer_SERNO").is_between(0, 9999))
                .then(
                    pl.format("000{}", pl.col("_tmp_integer_SERNO"))
                    .str.slice(-4)
                    .alias(self._serial_number_column)
                )
                .otherwise(None)
                .alias(self._serial_number_column)
            )
            .drop("_tmp_integer_SERNO")
        )

        empty_values = transformed_data.filter(
            pl.col(self._serial_number_column).is_null()
        )
        if not empty_values.is_empty():
            self._log(
                "Could not format all serial numbers.",
                level=adm_logger.WARNING,
                row_numbers=list(empty_values["row_number"]),
            )
            return

        changed_rows = transformed_data.filter(
            pl.col(self._serial_number_column) != pl.col("reported_serial_number")
        )
        if not changed_rows.is_empty():
            self._log(
                f"Formatted serial numbers in {len(changed_rows)} rows.",
                row_numbers=list(changed_rows["row_number"]),
            )

        data_holder.data = transformed_data
