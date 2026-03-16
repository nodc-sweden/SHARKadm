import json
import pathlib

from sharkadm import config


class ProfileMapper:
    def __init__(self, path: pathlib.Path | str):
        self._path = pathlib.Path(path)
        self._data = dict()
        self._par_mapping = dict()

        self._load_data()
        self._create_par_mapper()

    def _load_data(self) -> None:
        with open(self._path) as fid:
            self._data = json.load(fid)

    def _create_par_mapper(self) -> None:
        for short, pars in self.data["mapping_parameter"].items():
            for par in pars:
                self._par_mapping[par] = short

    @property
    def data(self) -> dict:
        return self._data

    def get(self, par: str, fallback: str | None = None) -> str:
        return self._par_mapping.get(par, fallback)

    def get_internal_name(self, par: str):
        return self.get(par, par)


def get_profile_mapper() -> ProfileMapper:
    return ProfileMapper(config.adm_config_paths("profile_mapping"))
