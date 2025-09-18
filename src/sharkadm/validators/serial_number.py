import logging

import polars as pl

from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers.base import PolarsDataHolderProtocol
from sharkadm.validators import Validator

logger = logging.getLogger(__name__)


class ValidateSerialNumber(Validator):
    _datetime_column = "datetime"
    _serial_number_column = "visit_id"

    @staticmethod
    def get_validator_description() -> str:
        return "Check if data in chronological"

    def _validate(self, data_holder: PolarsDataHolderProtocol) -> None:
        validated_data = data_holder.data.sort(
            [self._serial_number_column, self._datetime_column]
        ).with_columns(
            (
                (
                    (
                        pl.col(self._datetime_column)
                        > pl.col(self._datetime_column).shift(1)
                    )
                    & (
                        pl.col(self._serial_number_column)
                        .str.strip_chars()
                        .cast(pl.Int16, strict=False)
                        > pl.col(self._serial_number_column)
                        .shift(1)
                        .str.strip_chars()
                        .cast(pl.Int16, strict=False)
                    )
                )
                | (
                    (
                        pl.col(self._datetime_column)
                        == pl.col(self._datetime_column).shift(1)
                    )
                    & (
                        pl.col(self._serial_number_column)
                        == pl.col(self._serial_number_column).shift(1)
                    )
                )
            ).alias("chronological_visit_id"),
            (pl.col(self._serial_number_column).str.contains(r"^\d{4}$")).alias(
                "formatted_visit_id"
            ),
        )

        error_found = False
        if not validated_data["chronological_visit_id"].drop_nans().all():
            error_found |= True
            adm_logger.log_validation_failed(
                "Not all serial numbers are strictly chronological.",
                row_numbers=list(
                    validated_data.filter(~pl.col("chronological_visit_id"))["row_number"]
                ),
            )

        if not validated_data["formatted_visit_id"].all():
            error_found |= True
            adm_logger.log_validation_failed(
                "Not all serial numbers are correctly formatted.",
                row_numbers=list(
                    validated_data.filter(~pl.col("formatted_visit_id"))["row_number"]
                ),
            )

        if not error_found:
            adm_logger.log_validation_succeeded(
                "All serial numbers are chronological and correctly formatted.",
                level="info",
            )
