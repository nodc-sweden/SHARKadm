import pathlib

import yaml

from sharkadm import (
    exporters,
    multi_transformers,
    sharkadm_exceptions,
    transformers,
    utils,
    validators,
)
from sharkadm.config import adm_config_paths
from sharkadm.controller import SHARKadmPolarsController
from sharkadm.data import get_polars_data_holder
from sharkadm.exporters.base import PolarsFileExporter
from sharkadm.sharkadm_logger import adm_logger, get_exporter

VALIDATOR_DESCRIPTIONS = validators.get_validators_description()
TRANSFORMER_DESCRIPTIONS = transformers.get_transformers_description()
EXPORTER_DESCRIPTIONS = exporters.get_exporters_description()


class SHARKadmWorkflow:
    def __init__(
        self,
        data_sources: list[str | pathlib.Path] | None = None,
        validators_before: list[dict[str, str | dict[str, str]]] | None = None,
        transformers: list[dict[str, str | dict[str, str]]] | None = None,
        validators_after: list[dict[str, str | dict[str, str]]] | None = None,
        exporters: list[dict[str, str | dict[str, str]]] | None = None,
        workflow_config: dict[str, str] | None = None,
        adm_logger_config: dict[str, str | list] | None = None,
        **kwargs,
    ) -> None:
        self._controller = SHARKadmPolarsController()
        self._data_sources = data_sources or []
        self._validators_before = validators_before or []
        self._transformers = transformers or []
        self._validators_after = validators_after or []
        self._exporters = exporters or []

        self.export_paths = {}

        self._file_path: pathlib.Path = kwargs.get("file_path")

        self._workflow_config = dict(
            export_directory=utils.get_export_directory(),
            file_name=False,
            open_export_directory=False,
            save_config=True,
        )
        self._workflow_config.update(workflow_config or {})

        self._adm_logger_config = adm_logger_config

    @property
    def path(self) -> pathlib.Path | None:
        return self._file_path

    def _get_directory(self, path: str | pathlib.Path) -> pathlib.Path:
        path = pathlib.Path(path)
        if not path.exists():
            raise NotADirectoryError(f"Invalid export directory: {path}")
        if not path.is_dir():
            raise NotADirectoryError(f"Path is not a directory: {path}")
        return path

    def _initiate_workflow(self) -> None:
        adm_logger.log_workflow("Initiating workflow")
        self._set_validators_before()
        self._set_transformers()
        self._set_validators_after()
        self._set_exporters()

    def _set_validators_before(self) -> None:
        vals_list = []
        for val in self._validators_before or []:
            if not val.get("active", True):
                continue
            vals_list.append(validators.get_validator_object(**val))
        self._controller.set_validators_before(*vals_list)

    def _set_validators_after(self) -> None:
        vals_list = []
        for val in self._validators_after or []:
            if not val.get("active", True):
                continue
            vals_list.append(validators.get_validator_object(**val))
        self._controller.set_validators_after(*vals_list)

    def _set_transformers(self) -> None:
        trans_list = []
        for tran in self._transformers or []:
            if not tran.get("active", True):
                continue
            tran_obj = transformers.get_transformer_object(**tran)
            if not tran_obj:
                tran_obj = multi_transformers.get_multi_transformer_object(**tran)
            if not tran_obj:
                raise sharkadm_exceptions.InvalidTransformer(tran)
            trans_list.append(tran_obj)
        self._controller.set_transformers(*trans_list)

    def _set_exporters(self) -> None:
        exporter_list = []
        self.export_paths = {}
        for exp in self._exporters or []:
            if not exp.get("active", True):
                continue
            exporter = exporters.get_exporter_object(**exp)
            if isinstance(exporter, PolarsFileExporter):
                self.export_paths[exporter.name] = dict(
                    directory=exporter.export_directory,
                    file_name=exporter.export_file_name,
                    file_path=exporter.export_file_path,
                )
            exporter_list.append(exporter)
        self._controller.set_exporters(*exporter_list)

    def start_workflow(self) -> None:
        """Sets upp the workflow in the controller and starts it"""
        self._initiate_workflow()
        for data_source in self._data_sources:
            adm_logger.reset_log()
            d_holder = get_polars_data_holder(**data_source)
            self._controller.set_data_holder(d_holder)
            self._controller.start_data_handling()
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

    # def _open_log_reports(self) -> None:
    #     for path in self._open_report_paths:
    #         utils.open_file_with_default_program(path)

    # def _open_export_directory(self) -> None:
    #     for directory in self._open_directory_paths:
    #         utils.open_directory(directory)
    #     if self._workflow_config.get(
    #         'open_export_directory', self._workflow_config.get('open_directory')
    #     ):
    #         utils.open_directory(
    #             self._get_directory(self._workflow_config['export_directory'])
    #         )

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
                        [
                            pathlib.Path(source["path"]).stem
                            for source in self._data_sources
                        ]
                    )
                file_name = f"config_{name_str}.yaml"
            config_save_path = self._workflow_config["export_directory"] / file_name
        if not config_save_path.suffix == ".yaml":
            raise UserWarning(f"Export file name is not a yaml-file: {config_save_path}")
        data = dict(
            workflow_config=self._paths_to_string(self._workflow_config),
            adm_logger_config=self._paths_to_string(self._adm_logger_config),
            data_source_paths=self._paths_to_string(self._data_sources),
            validators_before=self._paths_to_string(self._validators_before),
            validators_after=self._paths_to_string(self._validators_after),
            transformers=self._paths_to_string(self._transformers),
            exporters=self._paths_to_string(self._exporters),
        )

        with open(config_save_path, "w") as fid:
            yaml.safe_dump(data, fid)

    def get_transformer_descriptions(self) -> dict[str, str]:
        return {
            tran["name"]: TRANSFORMER_DESCRIPTIONS[tran["name"]]
            for tran in self._transformers
        }

    def get_validator_before_descriptions(self) -> dict[str, str]:
        return {
            val["name"]: VALIDATOR_DESCRIPTIONS[val["name"]]
            for val in self._validators_before
        }

    def get_validator_after_descriptions(self) -> dict[str, str]:
        return {
            val["name"]: VALIDATOR_DESCRIPTIONS[val["name"]]
            for val in self._validators_after
        }

    def get_exporter_descriptions(self) -> dict[str, str]:
        print(f"{self._exporters=}")
        print(f"{EXPORTER_DESCRIPTIONS.keys()=}")
        return {
            exp["name"]: EXPORTER_DESCRIPTIONS[exp["name"]] for exp in self._exporters
        }

    def set_data_sources(self, *paths: str | pathlib.Path) -> None:
        sources = [dict(path=str(path)) for path in paths]
        # self._workflow_config['data_source_paths'] = sources
        self._data_sources = sources

    @property
    def exporters(self) -> list[dict]:
        return self._exporters

    @exporters.setter
    def exporters(self, exporters: list[dict]):
        self._exporters = exporters

    def export(self, **kwargs):
        exp = exporters.get_exporter_object(**kwargs)
        if not self._controller.data_holder:
            raise sharkadm_exceptions.DataHolderError("No data holder set")
        self._controller.export(exp)

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
) -> SHARKadmWorkflow | None:
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
