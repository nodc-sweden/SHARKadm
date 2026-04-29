import pathlib
import re
from typing import Any, Self

import polars as pl

from sharkadm import (
    event,
    exporters,
    sharkadm_exceptions,
    transformers,
    utils,
    validators,
)
from sharkadm.config.data_type import DataType
from sharkadm.data import get_polars_data_holder
from sharkadm.data.data_holder import PolarsDataHolder
from sharkadm.exporters import PolarsExporter
from sharkadm.multi_transformers import PolarsMultiTransformer
from sharkadm.operator import Operator, OperatorsInfo
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers import PolarsTransformer
from sharkadm.validators import Validator

# class BaseSHARKadmController:
#     """Class to hold data from a specific data type"""


# def transform_all(self) -> list[OperationInfo]:
#     """Runs all transform objects in self._transformers"""
#     return self.transform(*self._transformers)
#
# def transform(
#     self,
#     *transformers: Transformer
#     | MultiTransformer
#     | PolarsTransformer
#     | PolarsMultiTransformer,
#     return_if_cause_for_termination: bool = True,
# ) -> list[OperationInfo]:
#     tot_nr_operators = len(transformers)
#     infos = []
#     for i, trans in enumerate(transformers):
#         info = trans.transform(
#             self._data_holder,
#             return_if_cause_for_termination=return_if_cause_for_termination,
#         )
#         event.post_event(
#             "progress",
#             dict(
#                 total=tot_nr_operators,
#                 current=i + 1,
#                 title=f"Transforming...{trans.name}",
#             ),
#         )
#         if isinstance(info, list):
#             for _info in info:
#                 infos.append(_info)
#                 if return_if_cause_for_termination and _info.cause_for_termination:
#                     return infos
#         else:
#             infos.append(info)
#             # TODO: Kolla detta!!!
#             # if return_if_cause_for_termination and info.cause_for_termination:
#             #     return infos
#     # if return_if_cause_for_termination and info.cause_for_termination:
#     #     return info
#     return infos


class SHARKadmPolarsController:
    def __init__(self) -> None:
        self._data_holder: PolarsDataHolder | None = None
        self._transformers: list[PolarsTransformer | PolarsMultiTransformer] = []
        self._validators_before: list[Validator] = []
        self._validators_after: list[Validator] = []
        self._exporters: list[PolarsExporter] = []

        utils.clear_temp_directory(7)
        utils.clear_export_directory(7)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}"

    def __add__(self, other):
        cdh = self.data_holder + other.data_holder
        new_controller = SHARKadmPolarsController()
        new_controller.set_data_holder(cdh)
        return new_controller

    @classmethod
    def get_validators(cls) -> dict[str, dict]:
        return validators.get_validators_info()

    @classmethod
    def get_transformers(cls) -> dict[str, dict]:
        return transformers.get_transformers_info()

    @classmethod
    def get_exporters(cls) -> dict[str, dict]:
        return exporters.get_exporters_info()

    @classmethod
    def get_transformer_list(
        cls,
        start_with: list[str] | None = None,
        sort_the_rest: bool = True,
    ) -> dict[str, dict]:
        return _get_fixed_list(
            start_with=_get_name_mapper(start_with or []),
            all_items=SHARKadmPolarsController.get_transformers(),
            sort_the_rest=sort_the_rest,
        )

    @classmethod
    def get_validator_list(
        cls,
        start_with: list[str] | None = None,
        sort_the_rest: bool = True,
    ) -> dict[str, dict]:
        return _get_fixed_list(
            start_with=_get_name_mapper(start_with or []),
            all_items=SHARKadmPolarsController.get_validators(),
            sort_the_rest=sort_the_rest,
        )

    @classmethod
    def get_exporter_list(
        cls,
        start_with: list[str] | None = None,
        sort_the_rest: bool = True,
    ) -> dict[str, dict]:
        return _get_fixed_list(
            start_with=_get_name_mapper(start_with or []),
            all_items=SHARKadmPolarsController.get_exporters(),
            sort_the_rest=sort_the_rest,
        )

    @property
    def data(self) -> pl.DataFrame:
        return self._data_holder.data

    @property
    def data_type_obj(self) -> DataType:
        return self._data_holder.data_type_obj

    @property
    def data_type(self) -> str:
        return self._data_holder.data_type

    @property
    def dataset_name(self) -> str:
        return self._data_holder.dataset_name

    @property
    def data_holder(self) -> PolarsDataHolder:
        return self._data_holder

    @property
    def is_filtered(self) -> bool:
        return self._data_holder.is_filtered

    def filter(self, data_filter) -> Self:
        self._data_holder.filter(data_filter)
        return self

    def reset_filter(self) -> Self:
        self._data_holder.reset_filter()
        return self

    def set_data_holder(self, data_holder: PolarsDataHolder) -> Self:
        self._data_holder = data_holder
        adm_logger.dataset_name = data_holder.dataset_name
        self.transform(transformers.PolarsAddRowNumber())
        return self

    def transform(
        self,
        *transformers: PolarsTransformer | PolarsMultiTransformer,
        return_if_cause_for_termination: bool = True,
    ) -> Self:
        if self.is_filtered:
            raise sharkadm_exceptions.DataIsFilteredError(
                "Not allowed to transform when data is filtered!"
            )
        return self.run_operators(
            *transformers, return_if_cause_for_termination=return_if_cause_for_termination
        )

    def validate(
        self,
        *validators: Validator,
    ) -> Self:
        # if self.is_filtered:
        #     raise sharkadm_exceptions.DataIsFilteredError(
        #         "Not allowed to transform when data is filtered!"
        #     )
        return self.run_operators(*validators)

    def export(self, *exporters: PolarsExporter) -> OperatorsInfo | Any:
        return self.run_operators(*exporters)

    # def validate(self, *validators: Validator) -> Self:
    #     for val in validators:
    #         val.validate(self._data_holder)
    #     return self
    #
    # def export(self, *exporters: PolarsExporter) -> Any:
    #     for exp in exporters:
    #         data = exp.export(self._data_holder)
    #         if isinstance(data, pl.DataFrame):
    #             return data
    #     return self

    def run_operators(
        self,
        *operators: Operator,
        return_if_cause_for_termination: bool = True,
    ) -> OperatorsInfo:
        tot_nr_operators = len(operators)
        operator_infos = OperatorsInfo()
        for i, oper in enumerate(operators):
            title = f"Running transformer...{oper.name}"
            if isinstance(oper, Validator):
                title = f"Running validator...{oper.name}"
            if isinstance(oper, PolarsExporter):
                title = f"Running exporter...{oper.name}"
            event.post_event(
                "progress",
                dict(
                    total=tot_nr_operators,
                    current=i + 1,
                    title=title,
                ),
            )
            if isinstance(oper, PolarsTransformer):
                info = oper.transform(
                    self._data_holder,
                    return_if_cause_for_termination=return_if_cause_for_termination,
                )
            elif isinstance(oper, Validator):
                info = oper.validate(
                    self._data_holder,
                )
            elif isinstance(oper, PolarsExporter):
                info = oper.export(
                    self._data_holder,
                )
                return info
            else:
                raise ValueError("Invalid operator")
            operator_infos.add(info)
            if not operator_infos.all_succeeded and return_if_cause_for_termination:
                return operator_infos

        return operator_infos

    def run_operator(
        self,
        operator: Operator,
        return_if_cause_for_termination: bool = True,
    ) -> OperatorsInfo:
        return self.run_operators(
            operator, return_if_cause_for_termination=return_if_cause_for_termination
        )

    def get_columns(self, regex: str = "") -> list[str]:
        cols = []
        for col in self.data.columns:
            if re.search(regex, col):
                cols.append(col)
        return cols


def _get_name_mapper(list_to_map: list[dict]) -> dict[str, dict]:
    return_dict = dict()
    for item in list_to_map:
        return_dict[item["name"]] = item
    return return_dict


def _get_fixed_list(
    start_with: dict[str, dict] | None = None,
    all_items: dict[str, dict] | None = None,
    sort_the_rest: bool = True,
) -> dict[str, dict]:
    return_dict = dict()
    if start_with:
        for name, data in start_with.items():
            return_dict[name] = data
            all_items.pop(name)
    rest_names = list(all_items)
    if sort_the_rest:
        rest_names = sorted(rest_names)
    for rest_name in rest_names:
        return_dict[rest_name] = all_items[rest_name]
    return return_dict


def get_polars_controller_with_data(
    path: pathlib.Path | str | pl.DataFrame, **kwargs
) -> SHARKadmPolarsController:
    c = SHARKadmPolarsController()
    holder = get_polars_data_holder(path, **kwargs)
    c.set_data_holder(holder)
    return c
