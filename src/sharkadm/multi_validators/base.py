import time
from abc import abstractmethod
from typing import Type

from sharkadm.data import PolarsDataHolder
from sharkadm.operator import OperationInfo
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.validators import Validator


class MultiValidator(Validator):
    """Abstract base class used as a blueprint for doing multiple changes in data
    in a DataHolder"""

    valid_data_types: tuple[str, ...] = ()
    invalid_data_types: tuple[str, ...] = ()

    valid_data_holders: tuple[str, ...] = ()
    invalid_data_holders: tuple[str, ...] = ()

    valid_data_structures: tuple[str, ...] = ()
    invalid_data_structures: tuple[str, ...] = ()

    _validators: tuple[Type[Validator], ...] = ()

    def __init__(self, **kwargs):
        super().__init__()
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f"Multi validator: {self.__class__.__name__}"

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @staticmethod
    @abstractmethod
    def get_validator_description() -> str:
        """Verbal description describing what the multi validator is doing"""
        ...

    @property
    def description(self) -> str:
        return self.get_validator_description()

    def validate(
        self, data_holder: PolarsDataHolder, return_if_cause_for_termination: bool = True
    ) -> list[OperationInfo]:
        if not self.is_valid_data_holder(data_holder):
            return [OperationInfo(operator=self)]
        adm_logger.log_workflow(
            f"Applying multi validator: {self.__class__.__name__}",
            item=self.get_validator_description(),
            level=adm_logger.DEBUG,
        )
        infos = []
        t0 = time.time()
        for vali in self._validators:
            info = vali(**self._kwargs).validate(data_holder=data_holder)
            infos.append(info)
            if return_if_cause_for_termination and info.cause_for_termination:
                return infos
        adm_logger.log_workflow(
            f"Multi validator {self.__class__.__name__} "
            f"executed in {time.time() - t0} seconds",
            level=adm_logger.DEBUG,
        )
        return infos

    # def _data_holder_has_valid_data_type(self, data_holder: "PolarsDataHolder") -> bool:
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

    def _validate(self, data_holder: PolarsDataHolder) -> None:
        # Dummy method must be present to implement MultiValidator
        pass
