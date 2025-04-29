import logging

from sharkadm.sharkadm_logger import adm_logger

from .base import DataHolderProtocol, Validator

logger = logging.getLogger(__name__)


class AssertCombination(Validator):
    valid_data_types = ()
    _columns: list[str] = None

    def __init__(
        self,
        valid_combinations: list[str],
        columns: list[str] | None = None,
        valid_data_types: list[str] | None = None,
    ):
        super().__init__()
        self.columns = columns or self._columns or []
        self._valid_combinations = [item.replace(" ", "") for item in valid_combinations]
        self.valid_data_types = valid_data_types or self.invalid_data_types

    @staticmethod
    def get_validator_description() -> str:
        if AssertCombination._columns:
            return (
                f"Asserts that given valid_combinations are the only valid "
                f"valid_combinations of {AssertCombination._columns} "
                f'(seperate combination with "-")'
            )
        else:
            return (
                "Asserts that given valid_combinations are the only valid "
                "valid_combinations of given column "
                '(seperate combination with "-")'
            )

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        missing_cols = []
        for col in self.columns:
            if col not in data_holder.data:
                missing_cols.append(col)
        if not all([col in data_holder.data for col in self.columns]):
            adm_logger.log_validation_failed(
                f"Could not check valid_combinations. "
                f"Missing column(s) {missing_cols} in data",
                level=adm_logger.DEBUG,
            )
            return
        for vals, df in data_holder.data.groupby(self.columns):
            inter = "-".join(vals)
            if inter in self._valid_combinations:
                continue
            adm_logger.log_validation_failed(
                f"Invalid combination: {inter} ({len(df)} places)",
                level=adm_logger.WARNING,
            )


class AssertMinMaxDepthCombination(AssertCombination):
    _columns = ("sample_min_depth_m", "sample_max_depth_m")
