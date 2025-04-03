from .base import Validator, DataHolderProtocol
from sharkadm import adm_logger


class MissingTime(Validator):
    source_cols = ["sample_time"]

    @staticmethod
    def get_validator_description() -> str:
        cols_str = ", ".join(MissingTime.source_cols)
        return f"Checks if values are missing in column(s): {cols_str}"

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        for col in MissingTime.source_cols:
            if col not in data_holder.data.columns:
                adm_logger.log_validation(
                    f'Missing column "{col}"', level=adm_logger.ERROR
                )
                continue
            df = data_holder.data[data_holder.data[col].str.strip() == ""]
            if df.empty:
                continue
            nr_missing = len(df)
            rows_str = ", ".join(list(df["row_number"].astype(str)))
            adm_logger.log_validation(
                f"Missing {col} if {nr_missing} rows. Att rows: {rows_str}",
                level=adm_logger.ERROR,
            )

    @staticmethod
    def check(x):
        if len(x) == 4:
            return
        adm_logger.log_validation(f'Year "{x}" is not of length 4')
