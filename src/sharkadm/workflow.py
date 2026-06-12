import pathlib
from typing import Any

import yaml

from sharkadm import (
    data_filter,
    exporters,
    multi_transformers,
    operator,
    sharkadm_exceptions,
    transformers,
    utils,
    validators,
)
from sharkadm.config import adm_config_paths
from sharkadm.controller import SHARKadmPolarsController, get_polars_controller_with_data
from sharkadm.exporters import PolarsExporter
from sharkadm.exporters.base import PolarsFileExporter
from sharkadm.operator import Operator
from sharkadm.sharkadm_logger import adm_logger, get_exporter
from sharkadm.transformers import PolarsTransformer
from sharkadm.validators import Validator
from sharkadm.config.data_type import DataType, data_type_handler

# VALIDATOR_DESCRIPTIONS = validators.get_validators_description()
# TRANSFORMER_DESCRIPTIONS = transformers.get_transformers_description()
OPERATOR_DESCRIPTIONS = validators.get_validators_description()
OPERATOR_DESCRIPTIONS.update(transformers.get_transformers_description())
# OPERATOR_DESCRIPTIONS.update(exporters.get_exporters_description())
EXPORTER_DESCRIPTIONS = exporters.get_exporters_description()


class _Operators(list):
    def get(self, item: str) -> Operator | None:
        item = item.upper()
        for oper in self:
            if oper.name.upper() == item:
                return oper


class SHARKadmWorkflow:
    def __init__(
        self,
        data_sources: list[str | pathlib.Path] | None = None,
        operators: list[dict[str, str | dict[str, str]]] | None = None,
        # validators_before: list[dict[str, str | dict[str, str]]] | None = None,
        # transformers: list[dict[str, str | dict[str, str]]] | None = None,
        # validators_after: list[dict[str, str | dict[str, str]]] | None = None,
        exporters: list[dict[str, str | dict[str, str]]] | None = None,
        workflow_config: dict[str, str] | None = None,
        adm_logger_config: dict[str, str | list] | None = None,
        **kwargs,
    ) -> None:

        data_sources = data_sources or []

        self._data_sources: list[str] = []
        self._controller = SHARKadmPolarsController()
        self._operators_info = operators or []
        self._exporters_info = exporters or []

        self._all_validator_objects: _Operators[Validator] = _Operators()
        self._all_transformer_objects: _Operators[PolarsTransformer] = _Operators()
        self._all_exporter_objects: _Operators[PolarsExporter] = _Operators()

        self._operator_objects: _Operators[Operator] = _Operators()
        self._exporter_objects: _Operators[PolarsExporter] = _Operators()

        self._export_paths: dict[str, dict[str, pathlib.Path]] = {}

        self._file_path: pathlib.Path = kwargs.get("file_path")
        self._kwargs = kwargs

        self._workflow_config = dict(
            export_directory=utils.get_export_directory(),
            file_name=False,
            open_export_directory=False,
            save_config=True,
        )
        self._workflow_config.update(workflow_config or {})

        self._adm_logger_config = adm_logger_config

        self.set_data_sources(*data_sources)

        self.initiate_workflow()

    def __repr__(self) -> str:
        return f"{__class__.__name__}: {self.path}"

    def get(self, item: str, default: Any = None) -> Any:
        return self._kwargs.get(item, default)

    @property
    def path(self) -> pathlib.Path | None:
        return self._file_path

    @property
    def data_type(self) -> DataType:
        return data_type_handler.get_data_type_obj(self._workflow_config.get("name",
                                                           self._workflow_config.get("data_type",
                                                                                     "unknown")))

    def _get_directory(self, path: str | pathlib.Path) -> pathlib.Path:
        path = pathlib.Path(path)
        if not path.exists():
            raise NotADirectoryError(f"Invalid export directory: {path}")
        if not path.is_dir():
            raise NotADirectoryError(f"Path is not a directory: {path}")
        return path

    def initiate_workflow(self) -> None:
        if self._adm_logger_config.get("reset_before_workflow"):
            adm_logger.reset_log()
        adm_logger.log_workflow("Initiating workflow")

        self._all_validator_objects: list[Validator] = _Operators()
        self._all_transformer_objects: list[PolarsTransformer] = _Operators()
        self._all_exporter_objects: list[PolarsExporter] = _Operators()
        self._operator_objects: list[Operator] = _Operators()
        self._exporter_objects: list[PolarsExporter] = _Operators()

        self._update_operators()
        self._update_exporters()

    def _get_transformer_object(self, tran) -> transformers.PolarsTransformer | None:
        if tran.get("data_filter"):
            tran["data_filter"] = self._get_data_filter(tran["data_filter"])
        tran_obj = transformers.get_transformer_object(**tran)
        if not tran_obj:
            tran_obj = multi_transformers.get_multi_transformer_object(**tran)
        return tran_obj

    def _get_validator_object(self, val) -> validators.Validator | None:
        return validators.get_validator_object(**val)

    def _get_exporter_object(self, exp) -> exporters.PolarsExporter | None:
        return exporters.get_exporter_object(**exp)

    def _get_data_filter(self, filt):
        return data_filter.get_data_filter_object(**filt)

    def _update_operators(self):
        for oper in self._operators_info:
            if not oper.get("active", True):
                continue
            obj = self._get_operator_object(oper)
            self._save_operator_object(obj)

    def _get_operator_object(self, oper: dict) -> Operator:
        obj = self._get_transformer_object(oper)
        if not obj:
            obj = self._get_validator_object(oper)
        if not obj:
            obj = self._get_exporter_object(oper)
        if not obj:
            raise sharkadm_exceptions.InvalidOperator(obj)
        return obj

    def _save_operator_object(self, obj: Operator):
        self._operator_objects.append(obj)
        if isinstance(obj, Validator):
            self._all_validator_objects.append(obj)
        elif isinstance(obj, PolarsTransformer):
            self._all_transformer_objects.append(obj)
        elif isinstance(obj, PolarsExporter):
            self._all_exporter_objects.append(obj)

    def _update_exporters(self) -> None:
        for exp in self._exporters_info or []:
            if not exp.get("active", True):
                continue
            exporter = exporters.get_exporter_object(**exp)
            if isinstance(exporter, PolarsFileExporter):
                self._export_paths[exporter.name] = dict(
                    directory=exporter.export_directory,
                    file_name=exporter.export_file_name,
                    file_path=exporter.export_file_path,
                )
            self._exporter_objects.append(exporter)
            self._all_exporter_objects.append(exporter)

    def export(self, **exp):
        exporter = exporters.get_exporter_object(**exp)
        self._controller.run_operator(exporter)

    def start_workflow(self) -> None | operator.OperatorInfo:
        """Sets upp the workflow in the controller and starts it"""
        for data_source in self._data_sources:
            if self._adm_logger_config.get("reset_between_data_sources"):
                adm_logger.reset_log()
            self._controller = get_polars_controller_with_data(data_source)
            info = self._controller.run_operators(*self._operator_objects)
            if info.terminated:
                return info[-1]
            self._controller.run_operators(*self._exporter_objects)
            self._do_adm_logger_stuff()
        self.save_config()

    def _do_adm_logger_stuff(self) -> None:
        # Other options for log here later?
        for adm_logger_config in self._adm_logger_config.get("exporters", []):
            self._filter_log(adm_logger_config.get("filter"))
            exporter = get_exporter(**adm_logger_config)
            exporter.export(adm_logger)
            adm_logger.reset_filter()

    def _filter_log(self, log_filter: dict | None) -> None:
        if not log_filter:
            return
        adm_logger.filter(**log_filter)

    def _paths_to_string(self, info: dict | list) -> dict:
        if isinstance(info, list):
            new_info = []
            for item in info:
                if isinstance(item, (dict, list)):
                    item = self._paths_to_string(item)
                new_info.append(item)
        else:
            new_info = {}
            for key, value in info.items():
                if isinstance(value, pathlib.Path):
                    value = str(value)
                if isinstance(value, dict):
                    value = self._paths_to_string(value)
                new_info[key] = value
        return new_info

    def save_config(
        self,
        path: str | pathlib.Path | None = None,
    ) -> None:
        if path:
            config_save_path = pathlib.Path(path)
        else:
            if not self._workflow_config["save_config"]:
                return
            file_name = self._workflow_config["file_name"]
            if not file_name:
                if self._file_path:
                    name_str = self._file_path.stem
                else:
                    name_str = "-".join(
                        [pathlib.Path(source).stem for source in self._data_sources]
                    )
                file_name = f"config_{name_str}.yaml"
            config_save_path = self._workflow_config["export_directory"] / file_name
        if not config_save_path.suffix == ".yaml":
            raise UserWarning(f"Export file name is not a yaml-file: {config_save_path}")
        data = dict(
            workflow_config=self._paths_to_string(self._workflow_config),
            adm_logger_config=self._paths_to_string(self._adm_logger_config),
            data_source_paths=self._paths_to_string(self._data_sources),
            # validators_before=self._paths_to_string(self._validators_before),
            # validators_after=self._paths_to_string(self._validators_after),
            # transformers=self._paths_to_string(self._transformers),
            operators=self._paths_to_string(self._operators_info),
            exporters=self._paths_to_string(self._exporters_info),
        )

        with open(config_save_path, "w") as fid:
            yaml.safe_dump(data, fid)

    def get_operator_descriptions(self) -> dict[str, str]:
        return {
            oper["name"]: OPERATOR_DESCRIPTIONS[oper["name"]]
            for oper in self._operators_info
        }

    def get_exporter_descriptions(self) -> dict[str, str]:
        return {
            exp["name"]: EXPORTER_DESCRIPTIONS[exp["name"]]
            for exp in self._exporters_info
        }

    def set_data_sources(self, *paths: str | pathlib.Path) -> None:
        self._data_sources = [str(path) for path in paths]

    @property
    def operators_info(self) -> list[dict]:
        return self._operators_info

    @property
    def exporters_info(self) -> list[dict]:
        return self._exporters_info

    @property
    def operators(self) -> list[Operator]:
        return self._operator_objects

    @property
    def exporters(self) -> list[PolarsExporter]:
        return self._exporter_objects

    @property
    def all_validators(self) -> list[Validator]:
        return self._all_validator_objects

    @property
    def all_transformers(self) -> list[PolarsTransformer]:
        return self._all_transformer_objects

    @property
    def all_exporters(self) -> list[PolarsExporter]:
        return self._all_exporter_objects

    @property
    def export_paths(self) -> dict[str, dict[str, pathlib.Path]]:
        return self._export_paths

    @classmethod
    def from_yaml_config(cls, path: str | pathlib.Path) -> "SHARKadmWorkflow":
        with open(path) as fid:
            config = yaml.safe_load(fid)
        workflow = SHARKadmWorkflow(file_path=pathlib.Path(path), **config)
        return workflow


def get_workflows() -> dict[str, pathlib.Path]:
    return {path.stem: path for path in adm_config_paths["workflow"].iterdir()}


def get_workflow(workflow_name: str) -> SHARKadmWorkflow:
    workflows = get_workflows()
    if not workflows.get(workflow_name):
        raise sharkadm_exceptions.InvalidWorkflow
    return SHARKadmWorkflow.from_yaml_config(workflows.get(workflow_name))


def get_dv_workflow_for_data_type(
    data_type: str, default_if_missing: bool = True
) -> SHARKadmWorkflow:
    name = f"workflow_dv_{data_type.lower()}"
    workflows = get_workflows()
    if workflows.get(name):
        return get_workflow(name)
    if default_if_missing:
        return get_workflow("workflow_dv")


def get_dv_validation_workflow_for_data_type(
    data_type: str, default_if_missing: bool = True
) -> SHARKadmWorkflow | None:
    name = f"workflow_dv_validation_{data_type.lower()}"
    workflows = get_workflows()
    if workflows.get(name):
        return get_workflow(name)
    if default_if_missing:
        return get_workflow("workflow_dv_validation")
