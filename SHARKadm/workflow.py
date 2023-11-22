import yaml
import pathlib
from SHARKadm.controller import SHARKadmController
from SHARKadm import validators
from SHARKadm import transformers
from SHARKadm import exporters
from SHARKadm import adm_logger
from SHARKadm.data.archive import get_archive_data_holder
from SHARKadm.data import get_data_holder


class SHARKadmWorkflow:
    def __init__(self,
                 data_source_paths: list[str | pathlib.Path] = [],
                 validators_before: list[dict[str, str | dict[str, str]]] = [],
                 transformers: list[dict[str, str | dict[str, str]]] = [],
                 validators_after: list[dict[str, str | dict[str, str]]] = [],
                 exporters: list[dict[str, str | dict[str, str]]] = [],
                 save_config_path: str | pathlib.Path | None = None
                 ) -> None:

        self._controller = SHARKadmController()
        self.data_source_paths = data_source_paths
        self.validators_before = validators_before
        self.transformers = transformers
        self.validators_after = validators_after
        self.exporters = exporters
        self.save_config_path = save_config_path

    def _initiate_workflow(self) -> None:
        adm_logger.log_workflow('Initiating workflow')
        self._set_validators_before()
        self._set_transformers()
        self._set_validators_after()
        self._set_exporters()

    def _set_validators_before(self) -> None:
        vals_list = []
        for val in self.validators_before or []:
            vals_list.append(validators.get_validator_object(val['name'], **val.get('kwargs', {})))
        self._controller.set_validators_before(*vals_list)

    def _set_validators_after(self) -> None:
        vals_list = []
        for val in self.validators_after or []:
            vals_list.append(validators.get_validator_object(val['name'], **val.get('kwargs', {})))
        self._controller.set_validators_after(*vals_list)

    def _set_transformers(self) -> None:
        trans_list = []
        for tran in self.transformers or []:
            trans_list.append(transformers.get_transformer_object(tran['name'], **tran.get('kwargs', {})))
        self._controller.set_transformers(*trans_list)

    def _set_exporters(self) -> None:
        exporter_list = []
        print(f'{self.exporters=}')
        for exp in self.exporters or []:
            exporter_list.append(exporters.get_exporter_object(exp['name'], **exp.get('kwargs', {})))
        self._controller.set_exporters(*exporter_list)

    def start_workflow(self) -> None:
        """Sets upp the workflow in the controller and starts it"""
        self._initiate_workflow()
        for path in self.data_source_paths:
            d_holder = get_data_holder(path)
            self._controller.set_data_holder(d_holder)
            self._controller.start_data_handling()
        self.save_config()

    def get_report(self):
        pass

    def save_config(self):
        if not self.save_config_path:
            return
        if self.save_config_path.suffix != '.yaml':
            adm_logger.log_workflow(f'Could not save config file. The file is not a valid config file: {self.save_config_path}')
            return
        data = dict(
            data_source_paths = self.data_source_paths,
            validators_before = self.validators_before,
            validators_after = self.validators_after,
            transformers = self.transformers,
            exporters = self.exporters,
        )

        with open(self.save_config_path, 'w') as fid:
            yaml.safe_dump(data, self.save_config_path)

    @classmethod
    def from_yaml_config(cls, path: str | pathlib.Path):
        with open(path) as fid:
            config = yaml.safe_load(fid)
        workflow = SHARKadmWorkflow(
            data_source_paths=config.get('data_source_paths', []),
            validators_before=config.get('validators_before', []),
            validators_after=config.get('validators_after', []),
            transformers=config.get('transformers', []),
            exporters=config.get('exporters', []),
            save_config_path=config.get('save_config_path', []),
        )

        return workflow


class SHARKadmArchiveWorkflow:
    archive_paths: list[str | pathlib.Path] = []
    validators_before: list[dict[str, str | dict[str, str]]] = []
    transformers: list[dict[str, str | dict[str, str]]] = []
    validators_after: list[dict[str, str | dict[str, str]]] = []
    exporters: list[dict[str, str | dict[str, str]]] = []

    def __init__(self, **kwargs) -> None:
        self._controller = SHARKadmController()
        for key, value in kwargs.items():
            setattr(self, key, value)

    def _initiate_workflow(self) -> None:
        adm_logger.log_workflow('Initiating workflow')
        self._set_validators_before()
        self._set_transformers()
        self._set_validators_after()
        self._set_exporters()

    def _set_validators_before(self) -> None:
        vals_list = []
        for val in self.validators_before or []:
            vals_list.append(validators.get_validator_object(val['name'], **val.get('kwargs', {})))
        self._controller.set_validators_before(*vals_list)

    def _set_validators_after(self) -> None:
        vals_list = []
        for val in self.validators_after or []:
            vals_list.append(validators.get_validator_object(val['name'], **val.get('kwargs', {})))
        self._controller.set_validators_after(*vals_list)

    def _set_transformers(self) -> None:
        trans_list = []
        for tran in self.transformers or []:
            trans_list.append(transformers.get_transformer_object(tran['name'], **tran.get('kwargs', {})))
        self._controller.set_transformers(*trans_list)

    def _set_exporters(self) -> None:
        exporter_list = []
        print(f'{self.exporters=}')
        for exp in self.exporters or []:
            exporter_list.append(exporters.get_exporter_object(exp['name'], **exp.get('kwargs', {})))
        self._controller.set_exporters(*exporter_list)

    def start_workflow(self) -> None:
        """Sets upp the workflow in the controller and starts it"""
        self._initiate_workflow()
        for path in self.archive_paths:
            d_holder = get_archive_data_holder(path)
            self._controller.set_data_holder(d_holder)
            self._controller.start_data_handling()

    def get_report(self):
        pass

    @classmethod
    def from_yaml_config(cls, path: str | pathlib.Path):
        with open(path) as fid:
            config = yaml.safe_load(fid)
        workflow = SHARKadmArchiveWorkflow(
            archive_paths=config.get('archive_paths', []),
            validators_before=config.get('validators_before', []),
            validators_after=config.get('validators_after', []),
            transformers=config.get('transformers', []),
            exporters=config.get('exporters', []),
        )

        return workflow

