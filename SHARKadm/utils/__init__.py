import datetime
import os
import pathlib
import platform
import subprocess

SHARKADM_DIRECTORY = pathlib.Path.home() / 'sharkadm'


def get_root_directory(*subfolders: str) -> pathlib.Path:
    if not SHARKADM_DIRECTORY.parent.exists():
        raise NotADirectoryError(f'Cant create temp directory under {SHARKADM_DIRECTORY.parent}. Directory does not '
                                 f'exist!')
    folder = pathlib.Path(SHARKADM_DIRECTORY, *subfolders)
    folder.mkdir(exist_ok=True, parents=True)
    return folder


def get_temp_directory(*subfolders: str) -> pathlib.Path:
    temp_directory = get_root_directory() / '_temp'
    if not temp_directory.parent.exists():
        raise NotADirectoryError(f'Cant create temp directory under {temp_directory.parent}. Directory does not exist!')
    folder = pathlib.Path(temp_directory, *subfolders)
    folder.mkdir(exist_ok=True, parents=True)
    return folder


def get_config_directory(*subfolders: str) -> pathlib.Path:
    config_directory = get_root_directory() / 'config'
    if not config_directory.parent.exists():
        raise NotADirectoryError(f'Cant create config directory under {config_directory.parent}. Directory does not '
                                 f'exist!')
    folder = pathlib.Path(config_directory, *subfolders)
    folder.mkdir(exist_ok=True, parents=True)
    return folder


def get_export_directory(*subdirectories: str) -> pathlib.Path:
    export_directory = get_root_directory() / 'exports'
    if not export_directory.parent.exists():
        raise NotADirectoryError(f'Cant create export directory under {export_directory.parent}. Directory does not '
                                 f'exist!')
    # folder = pathlib.Path(export_directory, *subdirectories)
    folder = pathlib.Path(export_directory, datetime.datetime.now().strftime('%Y%m%d'), *subdirectories)
    folder.mkdir(exist_ok=True, parents=True)
    return folder


def open_file_with_default_program(path: str | pathlib.Path) -> None:
    if platform.system() == 'Darwin':       # macOS
        subprocess.call(('open', str(path)))
    elif platform.system() == 'Windows':    # Windows
        os.startfile(str(path))
    else:                                   # linux variants
        subprocess.call(('xdg-open', str(path)))


def open_files_in_winmerge(*args: str | pathlib.Path) -> None:
    try:
        string = '"C:\Program Files (x86)\WinMerge\WinMergeU.exe"'
        for arg in args:
            string = string + f' {arg}'
        subprocess.call(string)
        # subprocess.call((f'"C:\Program Files (x86)\WinMerge\WinMergeU.exe" {file1} {file2}'))
    except:
        pass


def open_directory(*args: str | pathlib.Path) -> None:
    for arg in args:
        print(f'{arg=}')
        os.startfile(str(arg))
    try:
        for arg in args:
            os.startfile(str(arg))
    except:
        pass


def open_export_directory(*subdirectories: str) -> None:
    open_directory(get_export_directory(*subdirectories))


def get_all_class_children_names(cls):
    if not cls.__subclasses__():
        return []
    names = []
    for c in cls.__subclasses__():
        names.append(c.__name__)
        names.extend(get_all_class_children_names(c))
    return names

