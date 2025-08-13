import polars as pl

from sharkadm.validators import Validator
from sharkadm.validators.base import DataHolderProtocol


class ValidateVisitOrder(Validator):
    _display_name = "Visit order"
    _datetime_column = "datetime"
    _visit_id = "visit_id"

    @staticmethod
    def get_validator_description() -> str:
        return "Check if visits are chronologically."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        non_chronological_visits = (
            data_holder.data.unique(subset=[self._visit_id, self._datetime_column])
            .sort(self._visit_id)
            .with_columns(pl.col(self._datetime_column).diff().alias("time_delta"))
            .filter(pl.col("time_delta") < 0)
        )
        if non_chronological_visits.is_empty():
            self._log_success("Data order is chronological.")
        else:
            self._log_fail(
                "Data rows out of order",
                row_numbers=list(non_chronological_visits["row_number"]),
            )
