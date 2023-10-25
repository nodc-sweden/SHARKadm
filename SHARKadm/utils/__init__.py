import os
import pathlib
import platform
import subprocess

TEMP_DIRECTORY = pathlib.Path.home() / '_temp_sharkadm'


def get_temp_directory(*subfolders: str) -> pathlib.Path:
    if not TEMP_DIRECTORY.parent.exists():
        raise NotADirectoryError(f'Cant create temp directory under {TEMP_DIRECTORY.parent}. Directory does not exist!')
    folder = pathlib.Path(TEMP_DIRECTORY, *subfolders)
    folder.mkdir(exist_ok=True, parents=True)
    return folder


def open_file_with_default_program(path: str | pathlib.Path) -> None:
    if platform.system() == 'Darwin':       # macOS
        subprocess.call(('open', str(path)))
    elif platform.system() == 'Windows':    # Windows
        os.startfile(str(path))
    else:                                   # linux variants
        subprocess.call(('xdg-open', str(path)))

