from sharkadm import config
import yaml


def get_ctd_parameter_mapper() -> dict:
    with open(config.adm_config_paths("ctd_parameter_mapping")) as fid:
        return yaml.safe_load(fid)