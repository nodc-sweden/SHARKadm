import time
from abc import ABC, abstractmethod
from typing import Protocol

import pandas as pd

from sharkadm import config
from sharkadm.data import is_valid_data_holder
from sharkadm.sharkadm_logger import adm_logger


class DataHolderProtocol(Protocol):
    @property
    @abstractmethod
    def data(self) -> pd.DataFrame: ...

    @property
    @abstractmethod
    def data_type(self) -> str: ...


class Validator(ABC):
    """Abstract base class used as a blueprint to validate/tidy/check data
    in a DataHolder"""

    valid_data_types = ()
    invalid_data_types = ()

    valid_data_holders = ()
    invalid_data_holders = ()

    _display_name = None

    def __init__(self, **kwargs):
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f"Validator: {self.__class__.__name__}"

    @staticmethod
    @abstractmethod
    def get_validator_description() -> str:
        """Verbal description describing what the validator is doing"""
        ...

    @property
    def description(self) -> str:
        return self.get_validator_description()

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def display_name(self) -> str:
        return self._display_name or self.name

    def validate(self, data_holder: DataHolderProtocol) -> None:
        if data_holder.data_type_internal not in config.get_valid_data_types(
            valid=self.valid_data_types, invalid=self.invalid_data_types
        ):
            adm_logger.log_workflow(
                f"Invalid data_type {data_holder.data_type_internal} "
                f"for validator {self.name}",
                level=adm_logger.DEBUG,
            )
            return

        if not is_valid_data_holder(
            data_holder,
            valid=self.valid_data_holders,
            invalid=self.invalid_data_holders,
        ):
            self._log_workflow(
                f"Invalid data_holder {data_holder.__class__.__name__} for validator"
                f" {self.name}"
            )
            return

        adm_logger.log_workflow(
            f"Applying validator: {self.name}",
            item=self.get_validator_description(),
            level=adm_logger.DEBUG,
        )
        t0 = time.time()
        self._validate(data_holder=data_holder)
        adm_logger.log_workflow(
            f"Validator {self.name} executed in {time.time() - t0} seconds",
            level=adm_logger.DEBUG,
        )

    @abstractmethod
    def _validate(self, data_holder: DataHolderProtocol) -> None: ...

    def _log_success(self, msg: str, **kwargs):
        adm_logger.log_validation_succeeded(
            msg, validator=self.display_name, cls=self.__class__.__name__, **kwargs
        )

    def _log_fail(self, msg: str, **kwargs):
        adm_logger.log_validation_failed(
            msg, validator=self.display_name, cls=self.__class__.__name__, **kwargs
        )

    def _log_workflow(self, msg: str, **kwargs):
        adm_logger.log_workflow(msg, cls=self.__class__.__name__, **kwargs)
