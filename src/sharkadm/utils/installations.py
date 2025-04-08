import inspect
from importlib.util import find_spec

from sharkadm.sharkadm_logger import adm_logger


def verify_installation(package_name: str) -> bool:
    if not find_spec(package_name):
        previous_frame = inspect.stack()[1]
        calling_module = inspect.getmodulename(previous_frame.filename)

        adm_logger.log_workflow(
            f"Could not import package '{package_name}' in module '{calling_module}'. "
            f"You need to install this dependency if you want to use this module.",
            level=adm_logger.WARNING,
        )
        return False
    return True
