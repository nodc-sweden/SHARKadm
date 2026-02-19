from __future__ import annotations

from abc import ABC, abstractmethod

import polars as pl

from sharkadm.data import PolarsDataHolder


class PolarsDataFilter(ABC):
    """Create child class to filter data in data_holder"""

    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._invert: bool = False

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def description(self) -> str:
        return "Without description"

    @abstractmethod
    def _get_filter_mask(
        self, data_holder: PolarsDataHolder
    ) -> pl.expr.expr.Expr | None: ...

    def __repr__(self):
        return f"{self.__class__.__name__}"

    def __and__(self, other):
        return PolarsCombinedDataFilter(self, _and=other)

    def __or__(self, other):
        return PolarsCombinedDataFilter(self, _or=other)

    def __invert__(self):
        obj = self.__class__(*self._args, **self._kwargs)
        obj._invert = True
        return obj

    def get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.expr.expr.Expr | None:
        mask = self._get_filter_mask(data_holder)
        if self._invert:
            return ~mask
        return mask


class PolarsCombinedDataFilter:
    def __init__(
        self,
        mask: PolarsDataFilter | PolarsCombinedDataFilter,
        _and: PolarsDataFilter | PolarsCombinedDataFilter = None,
        _or: PolarsDataFilter | PolarsCombinedDataFilter = None,
    ):
        self._mask = mask
        self._and = _and
        self._or = _or
        self._invert = False

    def __repr__(self):
        string = f"{self._mask}"
        if self._and:
            string = " and ".join([string, f"{self._and}"])
        elif self._or:
            string = " or ".join([string, f"{self._or}"])
        return f"({string})"

    def __and__(self, other):
        return PolarsCombinedDataFilter(self, _and=other)

    def __or__(self, other):
        return PolarsCombinedDataFilter(self, _or=other)

    def __invert__(self):
        obj = self.__class__(self._mask, _and=self._and, _or=self._or)
        obj._invert = True
        return obj

    @property
    def name(self) -> str:
        return str(self)

    def get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.expr.expr.Expr | None:
        if self._and:
            mask = self._mask.get_filter_mask(data_holder) & self._and.get_filter_mask(
                data_holder
            )
        elif self._or:
            mask = self._mask.get_filter_mask(data_holder) | self._or.get_filter_mask(
                data_holder
            )
        else:
            mask = self._mask.get_filter_mask(data_holder)

        if self._invert:
            return ~mask
        return mask
