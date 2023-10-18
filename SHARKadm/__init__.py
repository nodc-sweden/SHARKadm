from SHARKadm.sharkadm_logger import adm_logger
import pathlib
from SHARKadm.transformers import get_transformers_description_text
from SHARKadm.validators import get_validators_description_text
from SHARKadm.exporters import get_exporters_description_text


def write_operations_description_to_file(path: str | pathlib.Path = '.') -> None:
    path = pathlib.Path(path)
    if path.is_dir():
        path = path / 'SHARKadm_operations.txt'
    with open(path, 'w') as fid:
        fid.write('\n\n\n\n'.join([
            get_validators_description_text(),
            get_transformers_description_text(),
            get_exporters_description_text()
        ]))
