import logging

import polars as pl

from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers.base import PolarsDataHolderProtocol
from sharkadm.validators import Validator

logger = logging.getLogger(__name__)


class ValidateSerialNumber(Validator):
    _display_name = "Chronology"
    _depth_column = "DEPH"
    _date_column = "visit_date"
    _time_column = "sample_time"

    @staticmethod
    def get_validator_description() -> str:
        return "Check if data in chronological"

    def _validate(self, data_holder: PolarsDataHolderProtocol) -> None:
        chronological_data = data_holder.data.sort(["SERNO", "datetime"]).with_columns(
            (
                (pl.col("datetime") > pl.col("datetime").shift(1))
                | (
                    (pl.col("datetime") == pl.col("datetime").shift(1))
                    & (pl.col("SERNO") == pl.col("SERNO").shift(1))
                )
            ).alias("chronological_serno")
        )

        if chronological_data["chronological_serno"].drop_nans().all():
            adm_logger.log_validation_succeeded(
                "All serial numbers are chronological.", level="info"
            )
        else:
            adm_logger.log_validation_failed(
                "Not all serial numbers are strictly chronological.",
                row_numbers=list(
                    chronological_data.filter(~pl.col("chronological_serno"))[
                        "row_number"
                    ]
                ),
            )
