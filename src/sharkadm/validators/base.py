import time
from abc import ABC, abstractmethod
from typing import Protocol

import pandas as pd
import polars as pl

from sharkadm import config
from sharkadm.data import (
    PolarsDataHolder,
    is_valid_data_holder,
)
from sharkadm.operation import OperationBase, OperationInfo
from sharkadm.sharkadm_logger import adm_logger


class DataHolderProtocol(Protocol):
    @property
    @abstractmethod
    def data(self) -> pd.DataFrame | pl.DataFrame: ...

    @property
    @abstractmethod
    def data_type(self) -> str: ...


class Validator(ABC, OperationBase):
    """Abstract base class used as a blueprint to validate/tidy/check data
    in a DataHolder"""

    valid_data_types: tuple[str, ...] = ()
    invalid_data_types: tuple[str, ...] = ()

    valid_data_holders: tuple[str, ...] = ()
    invalid_data_holders: tuple[str, ...] = ()

    valid_data_structures: tuple[str, ...] = ()
    invalid_data_structures: tuple[str, ...] = ()

    _display_name = None

    operation_type = "validator"

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

    def validate_old(self, data_holder: PolarsDataHolder) -> None:
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
        adm_logger.log_workflow(
            f"Validator {self.name} executed in {time.time() - t0} seconds",
            level=adm_logger.DEBUG,
        )
        if isinstance(info, OperationInfo):
            info.operator = self
            return info
        return OperationInfo(operator=self)

    # def _data_holder_has_valid_data_type(self, data_holder: "PolarsDataHolder")
    # -> bool:
    #     if (
    #             data_holder.data_type_internal != "unknown"
    #             and data_holder.data_type_internal
    #             in config.get_valid_data_types(
    #         valid=self.valid_data_types, invalid=self.invalid_data_types
    #     )
    #     ):
    #         return True
    #     return False
    #
    # def _data_holder_is_valid_data_holder(self, data_holder: "PolarsDataHolder")
    # -> bool:
    #     if is_valid_polars_data_holder(
    #             data_holder,
    #             valid=self.valid_data_holders,
    #             invalid=self.invalid_data_holders,
    #     ):
    #         return True
    #     return False
    #
    # def _data_holder_has_valid_data_structure(self, data_holder: "PolarsDataHolder")
    # -> bool:
    #     if data_holder.data_structure.lower() in config.get_valid_data_structures(
    #             valid=self.valid_data_structures, invalid=self.invalid_data_structures
    #     ):
    #         return True
    #     return False
    #
    # def is_valid_data_holder(self, data_holder: "PolarsDataHolder") -> bool:
    #     if not self._data_holder_has_valid_data_type(data_holder):
    #         self._log_workflow(
    #             f"Invalid data_type {data_holder.data_type_internal} for transformer"
    #             f" {self.name}",
    #             level=adm_logger.DEBUG,
    #         )
    #         return False
    #     if not self._data_holder_is_valid_data_holder(data_holder):
    #         self._log_workflow(
    #             f"Invalid data_holder {data_holder.__class__.__name__} for transformer"
    #             f" {self.name}"
    #         )
    #         return False
    #     if not self._data_holder_has_valid_data_structure(data_holder):
    #         self._log_workflow(
    #             f"Invalid data structure {data_holder.data_structure} "
    #             f"for transformer {self.name}",
    #             level=adm_logger.DEBUG,
    #         )
    #         return False
    #     return True

    @abstractmethod
    def _validate(self, data_holder: PolarsDataHolder) -> None: ...

    def _log_success(self, msg: str, **kwargs):
        adm_logger.log_validation_succeeded(
            msg, validator=self.get_display_name(), cls=self.__class__.__name__, **kwargs
        )

    def _log_fail(self, msg: str, **kwargs):
        adm_logger.log_validation_failed(
            msg, validator=self.get_display_name(), cls=self.__class__.__name__, **kwargs
        )

    def _log_workflow(self, msg: str, **kwargs):
        adm_logger.log_workflow(msg, cls=self.__class__.__name__, **kwargs)
