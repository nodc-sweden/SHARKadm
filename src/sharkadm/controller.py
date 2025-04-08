import logging
import pathlib
from typing import Any, Self

import pandas as pd
import pandas as pl

from sharkadm import event, exporters, transformers, utils, validators
from sharkadm.data import get_data_holder
from sharkadm.data.data_holder import DataHolder, PandasDataHolder, PolarsDataHolder
from sharkadm.exporters import Exporter
from sharkadm.multi_transformers import MultiTransformer
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers import Transformer
from sharkadm.validators import Validator

logger = logging.getLogger(__name__)


class BaseSHARKadmController:
    """Class to hold data from a specific data type"""

    def __init__(self) -> None:
        self._data_holder: DataHolder | None = None
        self._transformers: list[Transformer | MultiTransformer] = []
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
        # return_dict = dict()
        # for name, desc in validators.get_validators_description().items():
        #     return_dict[name] = dict(name=name, description=desc)
        # return return_dict

    @classmethod
    def get_transformers(cls) -> dict[str, dict]:
        return transformers.get_transformers_info()
        # return_dict = dict()
        # for name, desc in transformers.get_transformers_description().items():
        #     return_dict[name] = dict(name=name, description=desc)
        # return return_dict

    @classmethod
    def get_exporters(cls) -> dict[str, dict]:
        return exporters.get_exporters_info()
        # return_dict = dict()
        # for name, desc in exporters.get_exporters_description().items():
        #     return_dict[name] = dict(name=name, description=desc)
        # return return_dict

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

    def transform_all(self) -> None:
        """Runs all transform objects in self._transformers"""
        tot_nr_operators = len(self._transformers)
        for i, trans in enumerate(self._transformers):
            trans.transform(self._data_holder)
            event.post_event(
                "progress",
                dict(
                    total=tot_nr_operators,
                    current=i,
                    title=f"Transforming...{trans.name}",
                ),
            )

    def transform(
        self, *transformers: Transformer | MultiTransformer
    ) -> "SHARKadmController":
        for trans in transformers:
            trans.transform(self._data_holder)
        return self

    def set_validators_before(self, *args: Validator) -> None:
        """Sets one or more Validators to the data holder"""
        self._validators_before = args

    def validate_before_all(self) -> None:
        """Runs all set validator objects in self._validators_before"""
        tot_nr_operators = len(self._validators_before)
        for i, val in enumerate(self._validators_before):
            val.validate(self._data_holder)
            event.post_event(
                "progress",
                dict(
                    total=tot_nr_operators,
                    current=i,
                    title=f"Initial validation...{val.name}",
                ),
            )

    def validate(self, *validators: Validator) -> "SHARKadmController":
        for val in validators:
            val.validate(self._data_holder)
        return self

    def set_validators_after(self, *args: Validator) -> None:
        """Sets one or more Validators to the data holder"""
        self._validators_after = args

    def validate_after_all(self) -> None:
        """Runs all set validator objects in self._validators_after"""
        tot_nr_operators = len(self._validators_after)
        for i, val in enumerate(self._validators_after):
            val.validate(self._data_holder)
            event.post_event(
                "progress",
                dict(
                    total=tot_nr_operators,
                    current=i,
                    title=f"Final validation...{val.name}",
                ),
            )

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

    def export(self, *exporters: Exporter) -> Any:
        for exp in exporters:
            data = exp.export(self._data_holder)
            if isinstance(data, pd.DataFrame):
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

    def set_data_holder(self, data_holder: DataHolder) -> Self:
        self._data_holder = data_holder
        adm_logger.dataset_name = data_holder.dataset_name
        self.transform(transformers.PolarsAddRowNumber())
        return self


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
