from ..data import PolarsDataHolder
from .base import Validator


class ValidateDuplicatedRows(Validator):
    log_columns = (
        "visit_date",
        "sample_time",
        "reported_station_name",
        "reported_scientific_name",
        "parameter",
        "value",
    )

    def __init__(
        self,
        include_columns: list[str] | None = None,
        exclude_columns: list[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.include_columns = include_columns or []
        self.exclude_columns = exclude_columns or []
        if "row_number" not in self.exclude_columns:
            self.exclude_columns.append("row_number")

    @staticmethod
    def get_validator_description() -> str:
        return "Check for duplicated rows"

    def _validate(self, data_holder: PolarsDataHolder) -> None:
        if self.include_columns:
            columns = [
                col for col in self.include_columns if col in data_holder.data.columns
            ]
        else:
            columns = [
                col for col in data_holder.data.columns if col not in self.exclude_columns
            ]

        data = data_holder.data.filter(data_holder.data[columns].is_duplicated())
        log_cols = [col for col in self.log_columns if col in data.columns]
        for vals, df in data.group_by(columns):
            rows = df["row_number"].to_list()
            rows_str = ", ".join(rows)
            d = df.to_dicts()[0]
            info = "; ".join([val for key, val in d.items() if key in log_cols])
            msg = f"Duplicates in rows [{rows_str}]: {info}"
            self._log_fail(msg, row_numbers=rows)
