from .base import Validator #, DataHolderProtocol
from SHARKadm import adm_logger
from typing import Protocol
import pandas as pd


class DataHolderProtocol(Protocol):

    @property
    def data(self) -> pd.DataFrame:
        ...

    @property
    def data_type(self) -> str:
        ...

    @property
    def mandatory_reg_columns(self) -> list:
        ...

    @property
    def mandatory_nat_columns(self) -> list:
        ...


class ValidateValuesInMandatoryNatColumns(Validator):
    valid_data_holders = ['DvTemplateDataHolder']

    @staticmethod
    def get_validator_description() -> str:
        return 'Checks if values are missing for columns that are mandatory for national data'

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        for col in data_holder.mandatory_nat_columns:
            if col not in data_holder.data.columns:
                adm_logger.log_validation(f"Missing mandatory national column: {col}. Maybe it's not a national "
                                          f"station?",
                                          level='warning')
                continue
            data_holder.data[col].apply(lambda x, col=col: self.check(x, col))

    @staticmethod
    def check(x, col):
        if not x:
            adm_logger.log_validation(f'Missing value for mandatory column (national): {col}')


class ValidateValuesInMandatoryRegColumns(Validator):
    valid_data_holders = ['DvTemplateDataHolder']

    @staticmethod
    def get_validator_description() -> str:
        return 'Checks if values are missing for columns that are mandatory for regional data'

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        for col in data_holder.mandatory_reg_columns:
            if col not in data_holder.data.columns:
                adm_logger.log_validation(f"Missing mandatory reg column: {col}.", level='warning')
                continue
            data_holder.data[col].apply(lambda x, col=col: self.check(x, col))

    @staticmethod
    def check(x, col):
        if not x:
            adm_logger.log_validation(f'Missing value for mandatory column (regional): {col}')


class ValidateMandatoryNatColumnsExists(Validator):
    valid_data_holders = ['DvTemplateDataHolder']

    @staticmethod
    def get_validator_description() -> str:
        return 'Checks if columns that are mandatory for national data exists'

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        for col in data_holder.mandatory_nat_columns:
            if col not in data_holder.data.columns:
                adm_logger.log_validation(f"Missing mandatory national column: {col}. Maybe it's not a national station?",
                                          level='warning')
                continue


class ValidateMandatoryRegColumnsExists(Validator):
    valid_data_holders = ['DvTemplateDataHolder']

    @staticmethod
    def get_validator_description() -> str:
        return 'Checks if columns that are mandatory for regional data exists'

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        for col in data_holder.mandatory_reg_columns:
            if col not in data_holder.data.columns:
                adm_logger.log_validation(f"Missing mandatory regional column: {col}.",
                                          level='warning')
                continue

