import pathlib
import subprocess

from sharkadm.config import get_config_path
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.utils import get_svn_info
from sharkadm.utils.svn import commit_files, get_modified_svn_files, update_svn_directory


def _get_nodc_config_directory() -> pathlib.Path | None:
    config_dir = get_config_path()
    if not config_dir:
        adm_logger.log_workflow(
            "No nodc_config directory is configured.", level=adm_logger.WARNING
        )
        return
    return config_dir


def update_nodc_config_directory_from_svn() -> None:
    config_dir = _get_nodc_config_directory()
    if not config_dir:
        return
    if update_svn_directory(config_dir):
        adm_logger.log_workflow("Could not update nodc_config.", level=adm_logger.WARNING)


def get_nodc_config_svn_revision() -> str:
    config_dir = get_config_path()
    if not config_dir:
        return ""
    svn_info = get_svn_info(get_config_path())
    if not svn_info:
        return ""
    return svn_info.revision


def get_modified_config_files_in_svn() -> list[pathlib.Path]:
    config_dir = _get_nodc_config_directory()
    if not config_dir:
        return []
    return get_modified_svn_files(config_dir)


def commit_modified_config_files_in_svn(
    *paths: pathlib.Path, msg: str = "Auto commit by sharkadm"
) -> list[pathlib.Path]:
    config_dir = _get_nodc_config_directory()
    if not config_dir:
        return []
    updated_paths = get_modified_svn_files(config_dir, match_paths=paths)
    commit_files(*paths, msg=msg)
    return updated_paths


try:
    update_nodc_config_directory_from_svn()
except FileNotFoundError as e:
    print(f"Could not find file: {e}")
except Exception as e:
    print(e)
