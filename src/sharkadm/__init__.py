# Do not change order of imports!

import subprocess

from sharkadm.sharkadm_logger import adm_logger
from sharkadm.settings import adm_settings

from sharkadm.config import adm_config_paths
from sharkadm.controller import SHARKadmController
from sharkadm.data import get_data_holder
import pathlib
from sharkadm.transformers import get_transformers_description_text
from sharkadm.multi_transformers import get_multi_transformers_description_text
from sharkadm.utils import get_nodc_config_directory, TEMP_DIRECTORY
from sharkadm.validators import get_validators_description_text
from sharkadm.multi_validators import get_multi_validators_description_text
from sharkadm.exporters import get_exporters_description_text

from sharkadm.lims_data import get_row_data_from_lims_export
from sharkadm.dv_template_data import get_row_data_from_fyschem_dv_template
from sharkadm import workflow


def write_operations_description_to_file(path: str | pathlib.Path = '.') -> None:
    """Writes a summary of validators, transformers and exporters to file"""
    path = pathlib.Path(path)
    if path.is_dir():
        path = path / 'sharkadm_operations.txt'
    with open(path, 'w') as fid:
        fid.write('\n\n\n\n'.join([
            get_validators_description_text(),
            get_multi_validators_description_text(),
            get_transformers_description_text(),
            get_multi_transformers_description_text(),
            get_exporters_description_text()
        ]))


def get_controller_with_data(path: pathlib.Path | str) -> SHARKadmController:
    c = SHARKadmController()
    holder = get_data_holder(path)
    c.set_data_holder(holder)
    return c


def update_nodc_config_directory_from_svn() -> None:
    config_dir = get_nodc_config_directory()
    if not config_dir:
        # adm_logger.log_workflow('No config directory found', level=adm_logger.CRITICAL)
        return
    path = TEMP_DIRECTORY / 'update_nodc_config_with_svn.bat'
    lines = [f'cd {config_dir}', 'svn update']
    with open(path, 'w') as fid:
        fid.write('\n'.join(lines))
    subprocess.run(str(path))
    # adm_logger.log_workflow(f'NODC_CONFIG directory {config_dir} updated', level=adm_logger.DEBUG)


try:
    update_nodc_config_directory_from_svn()
except Exception as e:
    print(f'Could not update nodc_config: {e}')



