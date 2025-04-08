from importlib.util import find_spec

from sharkadm.sharkadm_logger import adm_logger

if find_spec("nodc_dyntaxa"):
    adm_logger.log_workflow(
        f"Could not import package 'nodc_dyntaxa' in module {__name__}. "
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )
