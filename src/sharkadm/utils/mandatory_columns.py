import yaml
from nodc_config import Config


def get_mandatory_columns(nodc_conf: Config, data_type: str) -> list[str]:
    path = nodc_conf("mandatory_columns")
    if not path:
        raise FileNotFoundError("File not found for mandatory columns")
    with open(path) as fid:
        data = yaml.safe_load(fid)
    mandatory = data["general"]
    mandatory.extend(data.get(data_type, []))
    return mandatory
