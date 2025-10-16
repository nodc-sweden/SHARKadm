import yaml

from sharkadm import config


def get_ctd_parameter_mapper() -> dict:
    with open(config.adm_config_paths("ctd_parameter_mapping")) as fid:
        return yaml.safe_load(fid)
