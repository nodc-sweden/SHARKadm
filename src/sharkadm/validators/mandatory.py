from typing import Protocol

import pandas as pd
import polars as pl

from sharkadm.sharkadm_logger import adm_logger

from .base import Validator  # , DataHolderProtocol
from ..data import PolarsDataHolder
from ..utils.mandatory_columns import get_mandatory_columns


class DataHolderProtocol(Protocol):
    @property
    def data(self) -> pd.DataFrame: ...

    @property
    def data_type(self) -> str: ...

    @property
    def mandatory_reg_columns(self) -> list: ...

    @property
    def mandatory_nat_columns(self) -> list: ...


class ValidateMandatoryColumns(Validator):

    @staticmethod
    def get_validator_description() -> str:
        return (
            "Checks if mandatory columns listed in sharkadm config have values."
        )

    def _validate(self, data_holder: PolarsDataHolder):
        mandatory_columns = get_mandatory_columns(data_holder.data_type_internal)
        for col in mandatory_columns:
            if col not in data_holder.data.columns:
                self._log_fail(f"Mandatory column {col} not in data")
                continue
            missing = data_holder.data.filter(pl.col(col) == "")
            if not missing.is_empty():
                self._log_fail(f"{len(missing)} missing value(s) "
                               f"in mandatory column {col}",
                               row_numbers=list(missing["row_number"]))


class ValidateValuesInMandatoryNatColumns(Validator):
    valid_data_holders = ("DvTemplateDataHolder",)

    @staticmethod
    def get_validator_description() -> str:
        return (
            "Checks if values are missing for columns "
            "that are mandatory for national data"
        )

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        for col in data_holder.mandatory_nat_columns:
            if col not in data_holder.data.columns:
                adm_logger.log_validation_failed(
                    f"Missing mandatory national column: {col}. "
                    f"Maybe it's not a national station?",
                    level="warning",
                )
                continue
            data_holder.data[col].apply(lambda x, col=col: self.check(x, col))

    @staticmethod
    def check(x, col):
        if not x:
            adm_logger.log_validation_failed(
                f"Missing value for mandatory column (national): {col}"
            )


class ValidateValuesInMandatoryRegColumns(Validator):
    valid_data_holders = ("DvTemplateDataHolder",)

    @staticmethod
    def get_validator_description() -> str:
        return (
            "Checks if values are missing for columns that are mandatory "
            "for regional data"
        )

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        for col in data_holder.mandatory_reg_columns:
            if col not in data_holder.data.columns:
                adm_logger.log_validation_failed(
                    f"Missing mandatory reg column: {col}.", level="warning"
                )
                continue
            data_holder.data[col].apply(lambda x, col=col: self.check(x, col))

    @staticmethod
    def check(x, col):
        if not x:
            adm_logger.log_validation_failed(
                f"Missing value for mandatory column (regional): {col}"
            )


class ValidateMandatoryNatColumnsExists(Validator):
    valid_data_holders = ("DvTemplateDataHolder",)

    @staticmethod
    def get_validator_description() -> str:
        return "Checks if columns that are mandatory for national data exists"

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        for col in data_holder.mandatory_nat_columns:
            if col not in data_holder.data.columns:
                adm_logger.log_validation_failed(
                    f"Missing mandatory national column: {col}. "
                    f"Maybe it's not a national station?",
                    level="warning",
                )
                continue


class ValidateMandatoryRegColumnsExists(Validator):
    valid_data_holders = ("DvTemplateDataHolder",)

    @staticmethod
    def get_validator_description() -> str:
        return "Checks if columns that are mandatory for regional data exists"

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        for col in data_holder.mandatory_reg_columns:
            if col not in data_holder.data.columns:
                adm_logger.log_validation_failed(
                    f"Missing mandatory regional column: {col}.", level="warning"
                )
                continue
