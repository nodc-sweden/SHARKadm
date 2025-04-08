import subprocess

from sharkadm.sharkadm_exceptions import ConfigurationError
from sharkadm.utils import get_nodc_config_directory


def update_nodc_config_directory_from_svn() -> None:
    config_dir = get_nodc_config_directory()
    if not config_dir:
        print("No nodc_config directory is configured.")
        return
    status = subprocess.run(["svn", "update"], cwd=config_dir)
    if status.returncode:
        raise ConfigurationError("Could not update nodc_config.")


update_nodc_config_directory_from_svn()
