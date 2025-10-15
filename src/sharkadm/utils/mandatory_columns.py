import yaml
from sharkadm.config import adm_config_paths


def get_mandatory_columns(data_type: str) -> list[str]:
    with open(adm_config_paths("mandatory_columns")) as fid:
        data = yaml.safe_load(fid)
    mandatory = data["general"]
    mandatory.extend(data.get(data_type, []))
    return mandatory
