from sharkadm import adm_logger

from .base import Transformer, DataHolderProtocol

try:
    import nodc_dyntaxa
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(f'Could not import package "{module_name}" in module {__name__}. You need to install this dependency if you want to use this module.', level=adm_logger.WARNING)






