# -*- coding: utf-8 -*-

import pathlib
import sys

from .base import Transformer, DataHolderProtocol
from sharkadm.data.archive import ArchiveDataHolder
from sharkadm.utils import yaml_data
from sharkadm import adm_config_paths
from sharkadm import adm_logger


if getattr(sys, 'frozen', False):
    THIS_DIR = pathlib.Path(sys.executable).parent
else:
    THIS_DIR = pathlib.Path(__file__).parent


class AddStatus(Transformer):
    physical_chemical_keys = [
        'PhysicalChemical'.lower(),
        'Physical and Chemical'.lower()
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._config = yaml_data.load_yaml(adm_config_paths('delivery_note_status'))

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds status columns'

    def _transform(self, data_holder: ArchiveDataHolder) -> None:
        if not hasattr(data_holder, 'delivery_note'):
            adm_logger.log_workflow('Could not add status. No delivery note found!', level=adm_logger.WARNING, add=data_holder.dataset_name)
            return
        checked_by = data_holder.delivery_note['data kontrollerad av']
        if not checked_by:
            adm_logger.log_transformation(f'Could not set "status" and "checked". Missing information in delivery_note: data kontrollerad av',
                                          level=adm_logger.WARNING)
            return
        data = dict()
        if data_holder.data_type.lower() in self.physical_chemical_keys:
            data = self._config['default_physical_chemical']
        else:
            if checked_by == r'Leverantör':
                data = self._config['deliverer']
            elif checked_by == r'Leverantör och Datavärd':
                data = self._config['deliverer_and_datahost']
        data_holder.data['check_status_sv'] = data['check_status_sv']
        data_holder.data['check_status_en'] = data['check_status_en']
        data_holder.data['data_checked_by_sv'] = data['data_checked_by_sv']
        data_holder.data['data_checked_by_en'] = data['data_checked_by_en']


