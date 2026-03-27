from importlib.util import find_spec

import polars as pl

from sharkadm.sharkadm_logger import adm_logger

from ..data import PolarsDataHolder
from ._codes import _translate_codes
from .base import PolarsTransformer

if not find_spec("nodc_codes"):
    adm_logger.log_workflow(
        f"Could not import package 'nodc_codes' in module {__name__}. "
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class _PolarsReportingInstitute(PolarsTransformer):
    source_cols = ("",)
    col_to_set = ""
    lookup_key = ""
    lookup_field = "LABO"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._loaded_code_info = {}

    @staticmethod
    def get_transformer_description() -> str:
        return ""

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self._set_from_data(data_holder=data_holder):
            return
        if not self._set_from_other(data_holder=data_holder):
            self._log(
                f"None of the source columns {self.source_cols} found when trying to set "
                f"{self.col_to_set}. And no other reporting institute found.",
                level=adm_logger.WARNING,
            )

    def _set_from_other(self, data_holder: PolarsDataHolder) -> bool:
        if (
            hasattr(data_holder, "reporting_institute")
            and data_holder.reporting_institute
        ):
            info = _translate_codes.get_info(
                self.lookup_field, data_holder.reporting_institute
            )
            self._log(
                "Setting reporting_institute from data_holder", level=adm_logger.DEBUG
            )
            data_holder.data = data_holder.data.with_columns(
                pl.lit(info[self.lookup_key]).alias(self.col_to_set)
            )
            return True
        return False

    def _set_from_data(self, data_holder: PolarsDataHolder) -> bool:
        source_col = ""
        for col in self.source_cols:
            if col in data_holder.data.columns:
                source_col = col
                break
        if not source_col:
            return False
        if self.col_to_set not in data_holder.data.columns:
            # data_holder.data[self.col_to_set] = ""
            data_holder.data = data_holder.data.with_columns(
                pl.lit("").alias(self.col_to_set)
            )
        # for code in set(data_holder.data[source_col]):
        for (code,), df in data_holder.data.group_by(source_col):
            code = str(code)
            names = []
            for part in code.split(","):
                part = part.strip()
                info = _translate_codes.get_info(self.lookup_field, part)
                if info:
                    names.append(info[self.lookup_key])
                else:
                    self._log(
                        f"Could not find information for {source_col}: {part}",
                        level=adm_logger.WARNING,
                    )
                    names.append("?")
            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(source_col) == code)
                .then(pl.lit(", ".join(names)))
                .otherwise(pl.col(self.col_to_set))
                .alias(self.col_to_set)
            )
        return True


class PolarsAddSwedishReportingInstitute(_PolarsReportingInstitute):
    source_cols = ("reporting_institute_code", "reporting_institute_name_en")
    col_to_set = "reporting_institute_name_sv"
    lookup_key = "swedish_name"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds reporting institute name in swedish"


class PolarsAddEnglishReportingInstitute(_PolarsReportingInstitute):
    source_cols = ("reporting_institute_code", "reporting_institute_name_sv")
    col_to_set = "reporting_institute_name_en"
    lookup_key = "english_name"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds reporting institute name in english"
