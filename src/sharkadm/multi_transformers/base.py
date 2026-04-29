import time
from abc import abstractmethod
from typing import Type

from sharkadm.data.data_holder import PolarsDataHolder
from sharkadm.operator import OperatorsInfo, get_single_operators_info
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers.base import PolarsTransformer


class PolarsMultiTransformer(PolarsTransformer):
    """Abstract base class used as a blueprint for doing multiple changes in data
    in a DataHolder"""

    valid_data_types: tuple[str, ...] = ()
    invalid_data_types: tuple[str, ...] = ()

    valid_data_holders: tuple[str, ...] = ()
    invalid_data_holders: tuple[str, ...] = ()

    valid_data_structures: tuple[str, ...] = ()
    invalid_data_structures: tuple[str, ...] = ()

    operation_type: str = "multi transformer"

    _transformers: tuple[Type[PolarsTransformer], ...] = ()

    def __repr__(self) -> str:
        return f"Multi transformer: {self.__class__.__name__}"

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @staticmethod
    @abstractmethod
    def get_transformer_description() -> str:
        """Verbal description describing what the multi transformer is doing"""
        ...

    @property
    def description(self) -> str:
        return self.get_transformer_description()

    def transform(
        self,
        data_holder: "PolarsDataHolder",
        return_if_cause_for_termination: bool = True,
    ) -> OperatorsInfo:
        if not self.is_valid_data_holder(data_holder):
            return get_single_operators_info(operator=self, valid=False)
        self._log_workflow(
            f"Applying multi transformer: {self.name}",
            item=self.get_transformer_description(),
            level=adm_logger.DEBUG,
        )
        operators_info = OperatorsInfo()
        t0 = time.time()
        for trans in self._transformers:
            obj = trans(**self._kwargs)
            info = obj.transform(data_holder=data_holder)
            operators_info.add(info)
            if return_if_cause_for_termination and info.cause_for_termination:
                return operators_info
        adm_logger.log_workflow(
            f"Multi transformer {self.__class__.__name__} executed "
            f"in {time.time() - t0} seconds",
            level=adm_logger.DEBUG,
        )
        return operators_info

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        # Dummy method must be present to implement MultiTransformers
        pass
