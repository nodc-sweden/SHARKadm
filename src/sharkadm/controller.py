import pathlib
from typing import Any, Self

import pandas as pd
import polars as pl

from sharkadm import (
    event,
    exporters,
    sharkadm_exceptions,
    transformers,
    utils,
    validators,
)
from sharkadm.data import get_data_holder, get_polars_data_holder
from sharkadm.data.data_holder import DataHolder, PandasDataHolder, PolarsDataHolder
from sharkadm.exporters import Exporter, PolarsExporter
from sharkadm.multi_transformers import MultiTransformer, PolarsMultiTransformer
from sharkadm.operator import OperationInfo, Operator
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers import PolarsTransformer, Transformer
from sharkadm.utils import data_structures
from sharkadm.validators import Validator


class BaseSHARKadmController:
    """Class to hold data from a specific data type"""

    def __init__(self) -> None:
        self._data_holder: PolarsDataHolder | None = None
        self._transformers: list[PolarsTransformer | PolarsMultiTransformer] = []
        self._validators_before: list[Validator] = []
        self._validators_after: list[Validator] = []
        self._exporters: list[Exporter] = []

        utils.clear_temp_directory(7)
        utils.clear_export_directory(7)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}"

    def __add__(self, other):
        cdh = self.data_holder + other.data_holder
        new_controller = SHARKadmController()
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
            all_items=SHARKadmController.get_transformers(),
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
            all_items=SHARKadmController.get_validators(),
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
            all_items=SHARKadmController.get_exporters(),
            sort_the_rest=sort_the_rest,
        )

    @property
    def data_type(self) -> str:
        return self._data_holder.data_type

    @property
    def dataset_name(self) -> str:
        return self._data_holder.dataset_name

    @property
    def data_holder(self) -> DataHolder:
        return self._data_holder

    def set_transformers(self, *args: Transformer | MultiTransformer) -> None:
        """Add one or more Transformers to the data holder"""
        self._transformers = args

    def transform_all(self) -> list[OperationInfo]:
        """Runs all transform objects in self._transformers"""
        return self.transform(*self._transformers)

    def transform(
        self,
        *transformers: Transformer
        | MultiTransformer
        | PolarsTransformer
        | PolarsMultiTransformer,
        return_if_cause_for_termination: bool = True,
    ) -> list[OperationInfo]:
        tot_nr_operators = len(transformers)
        infos = []
        for i, trans in enumerate(transformers):
            info = trans.transform(
                self._data_holder,
                return_if_cause_for_termination=return_if_cause_for_termination,
            )
            event.post_event(
                "progress",
                dict(
                    total=tot_nr_operators,
                    current=i + 1,
                    title=f"Transforming...{trans.name}",
                ),
            )
            if isinstance(info, list):
                for _info in info:
                    infos.append(_info)
                    if return_if_cause_for_termination and _info.cause_for_termination:
                        return infos
            else:
                infos.append(info)
                # TODO: Kolla detta!!!
                # if return_if_cause_for_termination and info.cause_for_termination:
                #     return infos
        # if return_if_cause_for_termination and info.cause_for_termination:
        #     return info
        return infos

    def set_validators_before(self, *args: Validator) -> None:
        """Sets one or more Validators to the data holder"""
        self._validators_before = args

    def validate_before_all(self) -> OperationInfo:
        """Runs all set validator objects in self._validators_before"""
        tot_nr_operators = len(self._validators_before)
        for i, val in enumerate(self._validators_before):
            info = val.validate(self._data_holder)
            event.post_event(
                "progress",
                dict(
                    total=tot_nr_operators,
                    current=i,
                    title=f"Initial validation...{val.name}",
                ),
            )
            if info.cause_for_termination:
                return info

    def validate(self, *validators: Validator) -> Self:
        for val in validators:
            val.validate(self._data_holder)
        return self

    def set_validators_after(self, *args: Validator) -> None:
        """Sets one or more Validators to the data holder"""
        self._validators_after = args

    def validate_after_all(self) -> OperationInfo:
        """Runs all set validator objects in self._validators_after"""
        tot_nr_operators = len(self._validators_after)
        for i, val in enumerate(self._validators_after):
            info = val.validate(self._data_holder)
            event.post_event(
                "progress",
                dict(
                    total=tot_nr_operators,
                    current=i,
                    title=f"Final validation...{val.name}",
                ),
            )
            if info.cause_for_termination:
                return info

    def set_exporters(self, *args: Exporter) -> None:
        """Add one or more Exporters to the data holder"""
        self._exporters = args

    def export_all(self) -> None:
        """Runs all export objects in self._exporters"""
        tot_nr_operators = len(self._exporters)
        for i, exp in enumerate(self._exporters):
            exp.export(self._data_holder)
            event.post_event(
                "progress",
                dict(total=tot_nr_operators, current=i, title=f"Exporting...{exp.name}"),
            )

    def export(self, *exporters: PolarsExporter) -> Any:
        for exp in exporters:
            data = exp.export(self._data_holder)
            if isinstance(data, (pd.DataFrame, pl.DataFrame)):
                return data
        return self

    def get_workflow_description(self):
        lines = []
        lines.append(f"Data holder: {self.data_holder}")
        lines.append("Validators before:")
        for oper in self._validators_before:
            lines.append(f"  {oper.description} ({oper.name})")
        lines.append("Transformers:")
        for oper in self._transformers:
            lines.append(f"  {oper.description} ({oper.name})")
        lines.append("Validators after:")
        for oper in self._validators_after:
            lines.append(f"  {oper.description} ({oper.name})")
        lines.append("Exporters:")
        for oper in self._exporters:
            lines.append(f"  {oper.description} ({oper.name})")
        return "\n".join(lines)

    def start_data_handling(self):
        self.validate_before_all()
        self.transform_all()
        self.validate_after_all()
        self.export_all()


class SHARKadmController(BaseSHARKadmController):
    def __init__(self):
        super().__init__()
        self._data_holder: PandasDataHolder | None = None

    @property
    def data(self) -> pd.DataFrame:
        return self._data_holder.data

    def set_data_holder(self, data_holder: DataHolder) -> Self:
        self._data_holder = data_holder
        adm_logger.dataset_name = data_holder.dataset_name
        self.transform(transformers.AddRowNumber())
        return self


class SHARKadmPolarsController(BaseSHARKadmController):
    def __init__(self):
        super().__init__()
        self._data_holder: PolarsDataHolder | None = None

    @property
    def data(self) -> pl.DataFrame:
        return self._data_holder.data

    @property
    def is_filtered(self) -> bool:
        return self._data_holder.is_filtered

    def filter(self, data_filter) -> Self:
        self._data_holder.filter(data_filter)
        return self

    def reset_filter(self) -> Self:
        self._data_holder.reset_filter()
        return self

    def set_data_holder(self, data_holder: DataHolder) -> Self:
        self._data_holder = data_holder
        adm_logger.dataset_name = data_holder.dataset_name
        self.transform(transformers.PolarsAddRowNumber())
        return self

    def transform_all(self) -> Self:
        if self.is_filtered:
            raise sharkadm_exceptions.DataIsFilteredError(
                "Not allowed to transform when data is filtered!"
            )
        super().transform_all()
        return self

    def transform(
        self,
        *transformers: PolarsTransformer | PolarsMultiTransformer,
    ) -> Self:
        if self.is_filtered:
            raise sharkadm_exceptions.DataIsFilteredError(
                "Not allowed to transform when data is filtered!"
            )
        super().transform(*transformers)
        return self

    def run_operators(
        self,
        *operators: Operator,
        return_if_cause_for_termination: bool = True,
    ) -> dict[str, utils.data_structures.IndexDict[str, OperationInfo] | bool]:
        tot_nr_operators = len(operators)
        infos = dict()
        infos["terminated"] = False
        infos["operators"] = data_structures.IndexDict()
        for i, oper in enumerate(operators):
            title = f"Running transformer...{oper.name}"
            if isinstance(oper, Validator):
                title = f"Running validator...{oper.name}"
            event.post_event(
                "progress",
                dict(
                    total=tot_nr_operators,
                    current=i + 1,
                    title=title,
                ),
            )
            info = OperationInfo()
            if isinstance(oper, PolarsTransformer):
                info = oper.transform(
                    self._data_holder,
                    return_if_cause_for_termination=return_if_cause_for_termination,
                )
            elif isinstance(oper, Validator):
                info = oper.validate(
                    self._data_holder,
                )
            if isinstance(info, dict):
                # Can be dict from multitransformers
                for name, _info in info.items():
                    infos["operators"][name] = _info
                    if return_if_cause_for_termination and _info.cause_for_termination:
                        infos["terminated"] = True
                        break
                if infos["terminated"]:
                    break
            else:
                infos["operators"][oper.name] = info
                if return_if_cause_for_termination and info.cause_for_termination:
                    infos["terminated"] = True
                    break
        return infos

    def run_operator(
        self,
        operator: Operator,
        return_if_cause_for_termination: bool = True,
    ) -> dict[str, dict[str, OperationInfo] | bool]:
        return self.run_operators(
            operator, return_if_cause_for_termination=return_if_cause_for_termination
        )


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


def get_controller_with_data(path: pathlib.Path | str) -> SHARKadmController:
    c = SHARKadmController()
    holder = get_data_holder(path)
    c.set_data_holder(holder)
    return c


def get_polars_controller_with_data(
    path: pathlib.Path | str, **kwargs
) -> SHARKadmPolarsController:
    c = SHARKadmPolarsController()
    holder = get_polars_data_holder(path, **kwargs)
    c.set_data_holder(holder)
    return c
