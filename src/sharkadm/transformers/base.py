import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Protocol

import numpy as np
import pandas as pd
import polars as pl

from sharkadm import config
from sharkadm.data import is_valid_data_holder
from sharkadm.data_filter.base import PolarsDataFilter
from sharkadm.operator import OperationInfo, OperationType, Operator
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.utils.data_filter import DataFilter

if TYPE_CHECKING:
    from sharkadm.data.data_holder import PandasDataHolder, PolarsDataHolder


class DataHolderProtocol(Protocol):
    @property
    def data(self) -> pd.DataFrame | pl.DataFrame: ...

    @data.setter
    def data(self, df: pd.DataFrame | pl.DataFrame) -> None: ...

    @property
    @abstractmethod
    def data_type(self) -> str: ...

    @property
    @abstractmethod
    def data_type_internal(self) -> str: ...

    @property
    @abstractmethod
    def data_structure(self) -> str: ...

    @property
    @abstractmethod
    def dataset_name(self) -> str: ...


class PolarsDataHolderProtocol(Protocol):
    @property
    def data(self) -> pl.DataFrame: ...

    @data.setter
    def data(self, df: pl.DataFrame) -> None: ...

    @property
    @abstractmethod
    def data_type(self) -> str: ...

    @property
    @abstractmethod
    def data_type_internal(self) -> str: ...

    @property
    @abstractmethod
    def data_structure(self) -> str: ...

    @property
    @abstractmethod
    def dataset_name(self) -> str: ...


class Transformer(ABC, Operator):
    """Abstract base class used as a blueprint for changing data in a DataHolder"""

    valid_data_types = ()
    invalid_data_types = ()

    valid_data_holders = ()
    invalid_data_holders = ()

    valid_data_structures = ()
    invalid_data_structures = ()

    def __init__(self, data_filter: DataFilter = None, **kwargs):
        self._data_filter = data_filter
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f"Transformer: {self.__class__.__name__}"

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @staticmethod
    @abstractmethod
    def get_transformer_description() -> str:
        """Verbal description describing what the transformer is doing"""
        ...

    @property
    def description(self) -> str:
        return self.get_transformer_description()

    def transform(self, data_holder: "PandasDataHolder", **kwargs) -> None:
        if data_holder.data_type_internal not in config.get_valid_data_types(
            valid=self.valid_data_types, invalid=self.invalid_data_types
        ):
            self._log_workflow(
                f"Invalid data_type {data_holder.data_type_internal} for transformer"
                f" {self.name}",
                level=adm_logger.DEBUG,
            )
            print(f"Unknown: {data_holder.data_type_internal=}")
            return
        if not is_valid_data_holder(
            data_holder,
            valid=self.valid_data_holders,
            invalid=self.invalid_data_holders,
        ):
            self._log_workflow(
                f"Invalid data_holder {data_holder.__class__.__name__} for transformer"
                f" {self.name}"
            )
            return
        if data_holder.data_structure.lower() not in config.get_valid_data_structures(
            valid=self.invalid_data_structures, invalid=self.invalid_data_structures
        ):
            self._log_workflow(
                f"Invalid data_format {data_holder.data_structure} for transformer"
                f" {self.name}",
                level=adm_logger.DEBUG,
            )
            return

        self._log_workflow(
            f"Applying transformer: {self.__class__.__name__}",
            item=self.description,
            level=adm_logger.DEBUG,
        )
        t0 = time.perf_counter()
        info = self._transform(data_holder=data_holder)
        self._log_workflow(
            f"Transformer {self.name} executed in {time.perf_counter() - t0:.6f} seconds",
            level=adm_logger.DEBUG,
        )
        return info

    def validate(self, data_holder: "PolarsDataHolder") -> OperationInfo:
        if not self.is_valid_data_holder(data_holder):
            return OperationInfo(operator=self, valid=False)
        self._log_workflow(
            f"Applying transformer: {self.__class__.__name__}",
            item=self.description,
            level=adm_logger.DEBUG,
        )
        t0 = time.perf_counter()
        info = self._transform(data_holder=data_holder)
        self._log_workflow(
            f"Transformer {self.name} executed in {time.perf_counter() - t0:.6f} seconds",
            level=adm_logger.DEBUG,
        )
        if isinstance(info, OperationInfo):
            info.operator = self
            return info
        return OperationInfo(operator=self)

    @abstractmethod
    def _transform(self, data_holder: "PolarsDataHolder") -> None: ...

    def _get_filter_mask(self, data_holder: "PandasDataHolder") -> pd.Series | np.ndarray:
        if not self._data_filter:
            return np.ones(len(data_holder.data), dtype=bool)
        self._log_workflow(
            f"Using data filter {self._data_filter.name} on transformer {self.name}",
            level=adm_logger.WARNING,
        )
        return self._data_filter.get_filter_mask(data_holder)

    def _log(self, msg: str, **kwargs):
        adm_logger.log_transformation(msg, cls=self.__class__.__name__, **kwargs)

    def _log_workflow(self, msg: str, **kwargs):
        adm_logger.log_workflow(msg, cls=self.__class__.__name__, **kwargs)


class PolarsTransformer(ABC, Operator):
    """Abstract base class used as a blueprint for changing data in a DataHolder"""

    valid_data_types: tuple[str, ...] = ()
    invalid_data_types: tuple[str, ...] = ()

    valid_data_holders: tuple[str, ...] = ()
    invalid_data_holders: tuple[str, ...] = ()

    valid_data_structures: tuple[str, ...] = ()
    invalid_data_structures: tuple[str, ...] = ()

    source_col: str = ""
    col_to_set: str = ""

    operation_type: str = OperationType.TRANSFORMER

    def __init__(
        self,
        data_filter: PolarsDataFilter = None,
        valid_data_types: tuple[str, ...] = (),
        invalid_data_types: tuple[str, ...] = (),
        valid_data_holders: tuple[str, ...] = (),
        invalid_data_holders: tuple[str, ...] = (),
        valid_data_structures: tuple[str, ...] = (),
        invalid_data_structures: tuple[str, ...] = (),
        **kwargs,
    ):
        self._data_filter = data_filter

        self.valid_data_types = valid_data_types or self.valid_data_types
        self.invalid_data_types = invalid_data_types or self.invalid_data_types

        self.valid_data_holders = valid_data_holders or self.valid_data_holders
        self.invalid_data_holders = invalid_data_holders or self.invalid_data_holders

        self.valid_data_structures = valid_data_structures or self.valid_data_structures
        self.invalid_data_structures = (
            invalid_data_structures or self.invalid_data_structures
        )

        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f"Transformer: {self.__class__.__name__}"

    @property
    def name(self) -> str:
        print(f"{self.__class__.__name__=}")
        return self.__class__.__name__

    @staticmethod
    @abstractmethod
    def get_transformer_description() -> str:
        """Verbal description describing what the transformer is doing"""
        ...

    @property
    def description(self) -> str:
        # return self.get_transformer_description()
        info_str = self.get_transformer_description()
        # if self._data_filter:
        #     info_str = f"{info_str} (With filter {self._data_filter.description})"
        return info_str

    def transform(self, data_holder: "PolarsDataHolder", **kwargs) -> OperationInfo:
        if not self.is_valid_data_holder(data_holder):
            return OperationInfo(operator=self, valid=False)
        self._log_workflow(
            f"Applying transformer: {self.__class__.__name__}",
            item=self.description,
            level=adm_logger.DEBUG,
        )
        t0 = time.perf_counter()
        try:
            info = self._transform(data_holder=data_holder)
            self._log_workflow(
                f"Transformer {self.name} executed in "
                f"{time.perf_counter() - t0:.6f} seconds",
                level=adm_logger.DEBUG,
            )
            if isinstance(info, OperationInfo):
                info.operator = self
                return info
        except pl.exceptions.InvalidOperationError as e:
            return OperationInfo(
                operator=self, exception=e, cause_for_termination=True, success=False
            )
        return OperationInfo(operator=self)

    @abstractmethod
    def _transform(self, data_holder: "PolarsDataHolder") -> None: ...

    def _get_filter_mask(self, data_holder: "PolarsDataHolder") -> pl.Series:
        if not self._data_filter:
            return pl.Series()
        self._log_workflow(
            f"Using data filter {self._data_filter.name} on transformer {self.name}",
            level=adm_logger.WARNING,
        )
        return self._data_filter.get_filter_mask(data_holder)

    def _add_empty_col_to_set(self, data_holder: "PolarsDataHolder") -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.lit("").alias(self.col_to_set)
        )

    def _add_empty_col(
        self, data_holder: "PolarsDataHolder", col: str, _float=False
    ) -> None:
        val = None if _float else ""
        if col in data_holder.columns:
            return
        data_holder.data = data_holder.data.with_columns(pl.lit(val).alias(col))

    def _add_to_col_to_set(
        self, data_holder: "PolarsDataHolder", lookup_name, new_name: str
    ) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.when(pl.col(self.source_col) == lookup_name)
            .then(pl.lit(new_name))
            .otherwise(pl.col(self.col_to_set))
            .alias(self.col_to_set)
        )

    def _remove_columns(self, data_holder: "PolarsDataHolder", *cols) -> None:
        cols = [col for col in cols if col in data_holder.data.columns]
        data_holder.data = data_holder.data.drop(cols)

    def _log(self, msg: str, **kwargs):
        adm_logger.log_transformation(msg, cls=self.__class__.__name__, **kwargs)

    def _log_workflow(self, msg: str, **kwargs):
        adm_logger.log_workflow(msg, cls=self.__class__.__name__, **kwargs)
