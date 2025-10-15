import polars as pl

from sharkadm.sharkadm_logger import adm_logger

from .base import Validator
from ..data import PolarsDataHolder

try:
    from nodc_codes import get_translate_codes_object

    _translate_codes = get_translate_codes_object()
except ModuleNotFoundError as e:
    _translate_codes = None
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. '
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class _ValidateCodes(Validator):
    columns = ("",)
    lookup_field = ""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._loaded_code_info = {}

    @staticmethod
    def get_validator_description() -> str:
        return ""

    def _validate(self, data_holder: PolarsDataHolder) -> None:

        for col in self.columns:
            if col not in data_holder.data.columns:
                self._log_fail(f"No column named {col} in data", level=adm_logger.DEBUG)
                continue
            for code in set(data_holder.data[col]):
                info = _translate_codes.get_info(self.lookup_field, code.strip())
                if info is not None:
                    continue
                for part in code.split(","):
                    part = part.strip()
                    info = _translate_codes.get_info(self.lookup_field, part)
                    if info is not None:
                        continue
                    df = data_holder.data.filter(pl.col(col) == part)
                    self._log_fail(f'Invalid value(s) ({len(df)} rows) '
                                   f'"{part}" in column {col}',
                                   row_numbers=list(df["row_number"]))


class ValidateProjectCodes(_ValidateCodes):
    lookup_field = "project"
    columns = (
        "sample_project_code",
    )

    @staticmethod
    def get_validator_description() -> str:
        col_str = ", ".join(ValidateProjectCodes.columns)
        return f"Checks so that all codes are valid in columns: {col_str}"


class ValidateLABOcodes(_ValidateCodes):
    lookup_field = "LABO"
    columns = (
        "sampling_laboratory_code",
        "analytical_laboratory_code",
        "sample_orderer_code",

    )

    @staticmethod
    def get_validator_description() -> str:
        col_str = ", ".join(ValidateLABOcodes.columns)
        return f"Checks so that all codes are valid in columns: {col_str}"



