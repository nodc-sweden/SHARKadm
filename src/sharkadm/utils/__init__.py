import datetime
import os
import pathlib
import platform
import shutil
import subprocess
import zipfile

SHARKADM_DIRECTORY = pathlib.Path.home() / 'sharkadm'


def _remove_empty_directories(directory: pathlib.Path):
    for root, dirs, files in os.walk(directory, topdown=False):
        for name in dirs:
            directory = pathlib.Path(root, name)
            if not any(directory.iterdir()):
                try:
                    shutil.rmtree(directory)
                except PermissionError:
                    pass


def get_root_directory(*subfolders: str) -> pathlib.Path:
    if not SHARKADM_DIRECTORY.parent.exists():
        raise NotADirectoryError(f'Cant create root directory under {SHARKADM_DIRECTORY.parent}. Directory does not '
                                 f'exist!')
    folder = pathlib.Path(SHARKADM_DIRECTORY, *subfolders)
    folder.mkdir(exist_ok=True, parents=True)
    return folder


TEMP_DIRECTORY = get_root_directory() / '_temp'
CONFIG_DIRECTORY = get_root_directory() / 'config'
EXPORT_DIRECTORY = get_root_directory() / 'exports'


def get_temp_directory(*subfolders: str) -> pathlib.Path:
    if not TEMP_DIRECTORY.parent.exists():
        raise NotADirectoryError(f'Cant create temp directory under {TEMP_DIRECTORY.parent}. Directory does not exist!')
    folder = pathlib.Path(TEMP_DIRECTORY, *subfolders)
    folder.mkdir(exist_ok=True, parents=True)
    return folder


def clear_temp_directory(days_old: int = 7):
    """Clears temp directory from files and directories older than <days_old>"""
    remove_before_date = datetime.datetime.now() - datetime.timedelta(days=days_old)
    for root, dirs, files in os.walk(TEMP_DIRECTORY, topdown=False):
        for name in files:
            path = pathlib.Path(root, name)
            ts = os.path.getmtime(path)
            mod_time = datetime.datetime.fromtimestamp(ts, datetime.UTC)
            if mod_time.date() < remove_before_date.date():
                try:
                    os.remove(path)
                except PermissionError:
                    pass
        _remove_empty_directories(TEMP_DIRECTORY)


def clear_export_directory(days_old: int = 7):
    """Clears export directory from files and directories older than <days_old>"""
    remove_before_date = datetime.datetime.now() - datetime.timedelta(days=days_old)
    for root, dirs, files in os.walk(EXPORT_DIRECTORY, topdown=False):
        for name in files:
            path = pathlib.Path(root, name)
            ts = os.path.getmtime(path)
            mod_time = datetime.datetime.fromtimestamp(ts, datetime.UTC)
            if mod_time.date() < remove_before_date.date():
                try:
                    os.remove(path)
                except PermissionError:
                    pass
    _remove_empty_directories(EXPORT_DIRECTORY)


def get_config_directory(*subfolders: str) -> pathlib.Path:
    if not CONFIG_DIRECTORY.parent.exists():
        raise NotADirectoryError(f'Cant create config directory under {CONFIG_DIRECTORY.parent}. Directory does not '
                                 f'exist!')
    folder = pathlib.Path(CONFIG_DIRECTORY, *subfolders)
    folder.mkdir(exist_ok=True, parents=True)
    return folder


def get_export_directory(*subdirectories: str, date_directory=True) -> pathlib.Path:
    if not EXPORT_DIRECTORY.parent.exists():
        raise NotADirectoryError(f'Cant create export directory under {EXPORT_DIRECTORY.parent}. Directory does not '
                                 f'exist!')
    if date_directory:
        folder = pathlib.Path(EXPORT_DIRECTORY, datetime.datetime.now().strftime('%Y%m%d'), *subdirectories)
    else:
        folder = pathlib.Path(EXPORT_DIRECTORY, *subdirectories)
    folder.mkdir(exist_ok=True, parents=True)
    return folder


def open_file_with_default_program(path: str | pathlib.Path) -> None:
    if platform.system() == 'Darwin':       # macOS
        subprocess.call(('open', str(path)))
    elif platform.system() == 'Windows':    # Windows
        os.startfile(str(path))
    else:                                   # linux variants
        subprocess.call(('xdg-open', str(path)))


def open_file_with_excel(path: str | pathlib.Path) -> None:
    os.system(f'start EXCEL.EXE "{path}"')


def open_files_in_winmerge(*args: str | pathlib.Path) -> None:
    try:
        string = '"C:/Program Files/WinMerge/WinMergeU.exe"'
        for arg in args:
            string = string + f' {arg}'
        subprocess.call(string)
        # subprocess.call((f'"C:/Program Files (x86)/WinMerge/WinMergeU.exe" {file1} {file2}'))
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


def get_all_class_children_list(cls):
    if not cls.__subclasses__():
        return []
    children = []
    for c in cls.__subclasses__():
        children.append(c)
        children.extend(get_all_class_children_list(c))
    return children


def get_all_class_children_names(cls):
    return [c.__name__ for c in get_all_class_children_list(cls)]
    # if not cls.__subclasses__():
    #     return []
    # names = []
    # for c in cls.__subclasses__():
    #     names.append(c.__name__)
    #     names.extend(get_all_class_children_names(c))
    # return names


def get_all_class_children(cls):
    mapping = dict()
    for c in get_all_class_children_list(cls):
        mapping[c.__name__] = c
    return mapping


def unzip_file(path: pathlib.Path, export_directory: pathlib.Path, delete_old=False):
    sub_export_dir = export_directory / path.stem
    if sub_export_dir.exists() and not delete_old:
        raise IsADirectoryError(f'Already exist: {sub_export_dir}')
    with zipfile.ZipFile(path, 'r') as zip_ref:
        zip_ref.extractall(sub_export_dir)
    return sub_export_dir