import pathlib

import yaml


def load_yaml(path: str | pathlib.Path, **kwargs):
    with open(path, **kwargs) as fid:
        data = yaml.safe_load(fid)
    return data
