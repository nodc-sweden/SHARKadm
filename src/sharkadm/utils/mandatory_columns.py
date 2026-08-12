import yaml

from sharkadm.config import sharkadm_config


def get_mandatory_columns(data_type: str) -> list[str]:
    with open(sharkadm_config("mandatory_columns")) as fid:
        data = yaml.safe_load(fid)
    mandatory = data["general"]
    mandatory.extend(data.get(data_type, []))
    return mandatory
