import polars as pl

from sharkadm.sharkadm_logger import adm_logger

from ..config.translate_codes_new import get_valid_species_flag_codes
from ..data import PolarsDataHolder
from .base import Validator

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
            for (code,), df in data_holder.data.group_by(col):
                self._validate_code_and_log(
                    code,
                    col,
                    df,
                )

    def _validate_code_and_log(
        self, code: str, source_col: str, df: pl.DataFrame
    ) -> None:
        code = code.strip()
        if "," in code:
            for part in code.split(""):
                self._validate_code_and_log(part, source_col, df)
            return

        if " " in code:
            for part in code.split(" "):
                self._validate_code_and_log(part, source_col, df)
            return

        info = _translate_codes.get_info(self.lookup_field, code.strip())
        if info is not None:
            return
        self._log_fail(
            f"Invalid value {code} in column {source_col} ({len(df)} rows)",
            row_numbers=list(df["row_number"]),
        )


class ValidateProjectCodes(_ValidateCodes):
    lookup_field = "project"
    columns = ("sample_project_code",)

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


class ValidateSflag(Validator):
    col_to_check = "species_flag_code"

    @staticmethod
    def get_validator_description() -> str:
        return ""

    def _validate(self, data_holder: PolarsDataHolder) -> None:
        if self.col_to_check not in data_holder.data.columns:
            self._log_fail(
                f"No column named {self.col_to_check} in data", level=adm_logger.DEBUG
            )
            return
        self._valid_codes = get_valid_species_flag_codes()
        for (code,), df in data_holder.data.group_by(self.col_to_check):
            self._validate_code_and_log(
                code,
                self.col_to_check,
                df,
            )

    def _validate_code_and_log(
        self, code: str, source_col: str, df: pl.DataFrame
    ) -> None:
        code = code.strip()
        if "," in code:
            for part in code.split(""):
                self._validate_code_and_log(part, source_col, df)
            return

        if " " in code:
            for part in code.split(" "):
                self._validate_code_and_log(part, source_col, df)
            return

        if not code:
            return

        if code in self._valid_codes:
            return

        self._log_fail(
            f"Invalid code {code} in column {source_col} ({len(df)} rows)",
            row_numbers=list(df["row_number"]),
        )
