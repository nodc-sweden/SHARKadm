import yaml
import pathlib
from SHARKadm.controller import SHARKadmController
from SHARKadm import validators
from SHARKadm import transformers
from SHARKadm import exporters
from SHARKadm import adm_logger
from SHARKadm.data.archive import get_archive_data_holder


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

