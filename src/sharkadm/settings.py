import pathlib
import sys
import yaml


if getattr(sys, "frozen", False):
    THIS_DIR = pathlib.Path(sys.executable).parent
else:
    THIS_DIR = pathlib.Path(__file__).parent

SETTINGS_PATH = THIS_DIR / "settings.yaml"


class Settings:
    def __init__(self):
        self._path = pathlib.Path(SETTINGS_PATH)
        self._data = dict()
        self._create_file()
        self._load_settings()

    def _create_file(self):
        if self._path.exists():
            return
        with open(self._path, "w") as fid:
            yaml.safe_dump(self._data, fid)

    def _load_settings(self):
        with open(self._path) as fid:
            self._data = yaml.safe_load(fid)

    def _save_settings(self):
        with open(self._path, "w") as fid:
            yaml.safe_dump(self._data, fid)

    def get(self, key):
        if not self._data:
            return
        return self._data.get(key)

    def set(self, key, value):
        self._data[key] = value
        self._save_settings()


adm_settings = Settings()
