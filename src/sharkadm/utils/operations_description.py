import pathlib

from sharkadm.exporters import get_exporters_description_text
from sharkadm.multi_transformers import get_multi_transformers_description_text
from sharkadm.multi_validators import get_multi_validators_description_text
from sharkadm.transformers import get_transformers_description_text
from sharkadm.validators import get_validators_description_text


def write_operations_description_to_file(path: str | pathlib.Path = ".") -> None:
    """Writes a summary of validators, transformers and exporters to file"""
    path = pathlib.Path(path)
    if path.is_dir():
        path = path / "sharkadm_operations.txt"
    with open(path, "w") as fid:
        fid.write(
            "\n\n\n\n".join(
                [
                    get_validators_description_text(),
                    get_multi_validators_description_text(),
                    get_transformers_description_text(),
                    get_multi_transformers_description_text(),
                    get_exporters_description_text(),
                ]
            )
        )
