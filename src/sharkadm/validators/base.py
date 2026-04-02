import time
from abc import ABC, abstractmethod
from typing import Protocol

import pandas as pd
import polars as pl

from sharkadm.data import (
    PolarsDataHolder,
)
from sharkadm.operator import OperationInfo, OperationType, Operator
from sharkadm.sharkadm_logger import adm_logger


class DataHolderProtocol(Protocol):
    @property
    @abstractmethod
    def data(self) -> pd.DataFrame | pl.DataFrame: ...

    @property
    @abstractmethod
    def data_type(self) -> str: ...


class Validator(ABC, Operator):
    """Abstract base class used as a blueprint to validate/tidy/check data
    in a DataHolder"""

    valid_data_types: tuple[str, ...] = ()
    invalid_data_types: tuple[str, ...] = ()

    valid_data_holders: tuple[str, ...] = ()
    invalid_data_holders: tuple[str, ...] = ()

    valid_data_structures: tuple[str, ...] = ()
    invalid_data_structures: tuple[str, ...] = ()

    _display_name = None

    operation_type = OperationType.VALIDATOR

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

    @classmethod
    def get_display_name(cls) -> str:
        return cls._display_name or cls.__name__

    def validate(self, data_holder: PolarsDataHolder) -> OperationInfo:
        if not self.is_valid_data_holder(data_holder):
            return OperationInfo(operator=self, valid=False)
        adm_logger.log_workflow(
            f"Applying validator: {self.name}",
            item=self.get_validator_description(),
            level=adm_logger.DEBUG,
        )
        t0 = time.time()
        info = self._validate(data_holder=data_holder)
        print(f"IN Validator.validate: {info=}")
        adm_logger.log_workflow(
            f"Validator {self.name} executed in {time.time() - t0} seconds",
            level=adm_logger.DEBUG,
        )
        if isinstance(info, OperationInfo):
            info.operator = self
            return info
        return OperationInfo(operator=self)

    @abstractmethod
    def _validate(self, data_holder: PolarsDataHolder) -> None: ...

    def _log_success(self, msg: str, level: str = adm_logger.INFO, **kwargs):
        adm_logger.log_validation(
            msg=msg,
            level=level,
            validator=self.get_display_name(),
            cls=self.__class__.__name__,
            **kwargs,
        )

    def _log_fail(self, msg: str, level: str = adm_logger.WARNING, **kwargs):
        adm_logger.log_validation(
            msg=msg,
            level=level,
            validator=self.get_display_name(),
            cls=self.__class__.__name__,
            **kwargs,
        )

    def _log_workflow(self, msg: str, **kwargs):
        adm_logger.log_workflow(msg, cls=self.__class__.__name__, **kwargs)
