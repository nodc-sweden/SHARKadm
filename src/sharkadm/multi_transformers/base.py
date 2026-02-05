import time
from abc import abstractmethod
from typing import TYPE_CHECKING, Protocol, Type

import pandas as pd
import polars as pl

from sharkadm import config
from sharkadm.data import get_valid_data_holders, is_valid_polars_data_holder
from sharkadm.operator import OperationInfo
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers import PolarsTransformer, Transformer

if TYPE_CHECKING:
    from sharkadm.data.data_holder import PolarsDataHolder


class DataHolderProtocol(Protocol):
    @property
    def data(self) -> pd.DataFrame | pl.DataFrame:
        return

    @property
    @abstractmethod
    def data_type(self) -> str: ...

    @property
    @abstractmethod
    def data_type_internal(self) -> str: ...

    @property
    @abstractmethod
    def dataset_name(self) -> str: ...

    @property
    @abstractmethod
    def data_structure(self) -> str: ...


class MultiTransformer(Transformer):
    """Abstract base class used as a blueprint for doing multiple changes in data
    in a DataHolder"""

    valid_data_types: tuple[str, ...] = ()
    invalid_data_types: tuple[str, ...] = ()

    valid_data_holders: tuple[str, ...] = ()
    invalid_data_holders: tuple[str, ...] = ()

    valid_data_structures: tuple[str, ...] = ()
    invalid_data_structures: tuple[str, ...] = ()

    _transformers: tuple[Type[Transformer], ...] = ()

    def __init__(self, **kwargs):
        self._kwargs = kwargs

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

    def transform_old(self, data_holder: DataHolderProtocol) -> None:
        if data_holder.data_type_internal not in config.get_valid_data_types(
            valid=self.valid_data_types, invalid=self.invalid_data_types
        ):
            adm_logger.log_workflow(
                f"Invalid data_type {data_holder.data_type_internal} "
                f"for multi transformer {self.__class__.__name__}",
                level=adm_logger.DEBUG,
            )
            return
        if data_holder.__class__.__name__ not in get_valid_data_holders(
            valid=self.valid_data_holders, invalid=self.invalid_data_holders
        ):
            adm_logger.log_workflow(
                f"Invalid data_holder {data_holder.__class__.__name__} "
                f"for multi transformer {self.__class__.__name__}"
            )
            return
        if data_holder.data_structure.lower() not in config.get_valid_data_structures(
            valid=self.invalid_data_structures, invalid=self.invalid_data_structures
        ):
            adm_logger.log_workflow(
                f"Invalid data_format {data_holder.data_structure} for multi transformer"
                f" {self.__class__.__name__}",
                level=adm_logger.DEBUG,
            )
            return

        adm_logger.log_workflow(
            f"Applying multi transformer: {self.__class__.__name__}",
            item=self.get_transformer_description(),
            level=adm_logger.DEBUG,
        )
        t0 = time.time()
        for trans in self._transformers:
            trans().transform(data_holder=data_holder)
        adm_logger.log_workflow(
            f"Multi transformer {self.__class__.__name__} executed "
            f"in {time.time() - t0} seconds",
            level=adm_logger.DEBUG,
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        # Dummy method must be present to implement MultiTransformers
        pass


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

    def __init__(self, **kwargs):
        self._kwargs = kwargs

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

    def transform_old(self, data_holder: "PolarsDataHolder") -> None:
        if (
            data_holder.data_type_internal != "unknown"
            and data_holder.data_type_internal
            not in config.get_valid_data_types(
                valid=self.valid_data_types, invalid=self.invalid_data_types
            )
        ):
            adm_logger.log_workflow(
                f"Invalid data_type {data_holder.data_type_internal} "
                f"for multi transformer {self.__class__.__name__}",
                level=adm_logger.DEBUG,
            )
            return
        if not is_valid_polars_data_holder(
            data_holder,
            valid=self.valid_data_holders,
            invalid=self.invalid_data_holders,
        ):
            adm_logger.log_workflow(
                f"Invalid data_holder {data_holder.__class__.__name__} for transformer"
                f" {self.__class__.__name__}"
            )
            return

        if data_holder.data_structure.lower() not in config.get_valid_data_structures(
            valid=self.invalid_data_structures, invalid=self.invalid_data_structures
        ):
            adm_logger.log_workflow(
                f"Invalid data_format {data_holder.data_structure} for multi transformer"
                f" {self.__class__.__name__}",
                level=adm_logger.DEBUG,
            )
            return

        adm_logger.log_workflow(
            f"Applying multi transformer: {self.__class__.__name__}",
            item=self.get_transformer_description(),
            level=adm_logger.DEBUG,
        )
        t0 = time.time()
        for trans in self._transformers:
            trans(**self._kwargs).transform(data_holder=data_holder)
        adm_logger.log_workflow(
            f"Multi transformer {self.__class__.__name__} executed "
            f"in {time.time() - t0} seconds",
            level=adm_logger.DEBUG,
        )

    def transform(
        self,
        data_holder: "PolarsDataHolder",
        return_if_cause_for_termination: bool = True,
    ) -> dict[str, OperationInfo]:
        if not self.is_valid_data_holder(data_holder):
            return [OperationInfo(operator=self)]
        self._log_workflow(
            f"Applying multi transformer: {self.name}",
            item=self.get_transformer_description(),
            level=adm_logger.DEBUG,
        )
        infos = dict()
        t0 = time.time()
        for trans in self._transformers:
            # print()
            # print(f"{trans=}")
            # print(f"{trans.name=}")
            # print(f"{str(trans.name)=}")
            obj = trans(**self._kwargs)
            info = obj.transform(data_holder=data_holder)
            infos[obj.name] = info
            if return_if_cause_for_termination and info.cause_for_termination:
                return infos
        adm_logger.log_workflow(
            f"Multi transformer {self.__class__.__name__} executed "
            f"in {time.time() - t0} seconds",
            level=adm_logger.DEBUG,
        )
        return infos

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        # Dummy method must be present to implement MultiTransformers
        pass
