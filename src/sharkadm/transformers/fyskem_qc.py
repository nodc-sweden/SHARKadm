import pandas as pd
from sharkadm import adm_logger
from .base import Transformer, DataHolderProtocol

try:
    import fyskemqc
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(f'Could not import package "{module_name}" in module {__name__}. You need to install this dependency if you want to use this module.', level=adm_logger.WARNING)


class RunFyskKemQualityControl(Transformer):
    valid_data_types = ['PhysicalChemical']

    @staticmethod
    def get_transformer_description() -> str:
        return f'Applies quality control on physical chemical data'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        qc_object = fyskemqc.fyskemqc(data_holder.data)
