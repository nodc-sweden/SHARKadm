import subprocess

from sharkadm.sharkadm_exceptions import ConfigurationError
from sharkadm.utils import get_nodc_config_directory
from sharkadm.sharkadm_logger import adm_logger


def update_nodc_config_directory_from_svn() -> None:
    config_dir = get_nodc_config_directory()
    if not config_dir:
        adm_logger.log_workflow(
            "No nodc_config directory is configured.", level=adm_logger.WARNING
        )
        return
    status = subprocess.run(["svn", "update"], cwd=config_dir)
    if status.returncode:
        adm_logger.log_workflow("Could not update nodc_config.", level=adm_logger.WARNING)


update_nodc_config_directory_from_svn()
