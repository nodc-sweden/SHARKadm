import sys
import pathlib

from sharkadm import adm_settings


# if getattr(sys, 'frozen', False):
#     THIS_DIR = pathlib.Path(sys.executable).parent
# else:
#     THIS_DIR = pathlib.Path(__file__).parent

THIS_DIR = pathlib.Path(__file__).parent
LOCAL_CONFIG_ROOT_DIR = THIS_DIR / 'CONFIG_FILES'


class ConfigPaths:

    def __init__(self, root_path: str | pathlib.Path):
        self._root_path = pathlib.Path(root_path)
        self._paths = dict()
        self._save_paths()

    def __call__(self, item: str) -> pathlib.Path:
        return self._paths.get(item, None)

    def __getattr__(self, item: str) -> pathlib.Path:
        return self._paths.get(item, None)

    def __getitem__(self, item: str) -> pathlib.Path:
        return self._paths.get(item, None)

    def _save_paths(self):
        for path in self._root_path.iterdir():
            self._paths[path.stem] = path


def get_config_paths(root_path: str | pathlib.Path | None = None):
    if not root_path:
        print(f'{LOCAL_CONFIG_ROOT_DIR=}')
        if LOCAL_CONFIG_ROOT_DIR.exists():
            root_path = LOCAL_CONFIG_ROOT_DIR
        elif adm_settings.get('config_root_dir'):
            root_path = adm_settings.get('config_root_dir')
        if not root_path:
            raise NotADirectoryError('No config root directory found!')
    return ConfigPaths(root_path)


adm_config_paths = get_config_paths()
