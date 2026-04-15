import subprocess

from sharkadm.config import CONFIG_DIRECTORY
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.utils import get_nodc_config_directory, get_svn_info, svn


def update_nodc_config_directory_from_svn() -> None:
    config_dir = get_nodc_config_directory()
    if config_dir != CONFIG_DIRECTORY:
        print("Config not linked to SVN. Will not try to update.")
        return
    if not config_dir:
        adm_logger.log_workflow(
            "No nodc_config directory is configured.", level=adm_logger.WARNING
        )
        return
    status = subprocess.run(["svn", "update"], cwd=config_dir)
    if status.returncode:
        adm_logger.log_workflow("Could not update nodc_config.", level=adm_logger.WARNING)


def get_nodc_config_svn_revision() -> str:
    config_dir = get_nodc_config_directory()
    if not config_dir:
        return ""
    svn_info = get_svn_info(get_nodc_config_directory())
    if not svn_info:
        return ""
    return svn_info.revision


try:
    update_nodc_config_directory_from_svn()
except FileNotFoundError as e:
    print(f"Could not find file: {e}")
except Exception as e:
    print(e)
