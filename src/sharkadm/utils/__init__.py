import datetime
import os
import pathlib
import platform
import posixpath
import re
import shutil
import subprocess
import sys
import zipfile

SHARKADM_DIRECTORY = pathlib.Path.home() / "sharkadm"


def get_nodc_config_directory() -> pathlib.Path | None:
    CONFIG_ENV = "NODC_CONFIG"
    if os.getenv(CONFIG_ENV) and pathlib.Path(os.getenv(CONFIG_ENV)).exists():
        return pathlib.Path(os.getenv(CONFIG_ENV))


def has_admin_config() -> bool:
    """Returns True if user has local environment variable for NODC_CONFIG.
    Else return False"""
    if get_nodc_config_directory():
        return True
    return False


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
        raise NotADirectoryError(
            f"Cant create root directory under {SHARKADM_DIRECTORY.parent}."
            f"Directory does not exist!"
        )
    folder = pathlib.Path(SHARKADM_DIRECTORY, *subfolders)
    folder.mkdir(exist_ok=True, parents=True)
    return folder


TEMP_DIRECTORY = get_root_directory() / "_temp"
CONFIG_DIRECTORY = get_root_directory() / "config"
EXPORT_DIRECTORY = get_root_directory() / "exports"


def get_temp_directory(*subfolders: str) -> pathlib.Path:
    if not TEMP_DIRECTORY.parent.exists():
        raise NotADirectoryError(
            f"Cant create temp directory under {TEMP_DIRECTORY.parent}."
            f"Directory does not exist!"
        )
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


def clear_all_in_temp_directory():
    if TEMP_DIRECTORY.name != "_temp":
        print(f"I do not dare ro remove temp directory: {TEMP_DIRECTORY}")
        return
    for path in TEMP_DIRECTORY.iterdir():
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        else:
            os.remove(path)


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


def get_config_directory(*subfolders: str) -> pathlib.Path:
    if not CONFIG_DIRECTORY.parent.exists():
        raise NotADirectoryError(
            f"Cant create config directory under {CONFIG_DIRECTORY.parent}."
            f"Directory does not exist!"
        )
    folder = pathlib.Path(CONFIG_DIRECTORY, *subfolders)
    folder.mkdir(exist_ok=True, parents=True)
    return folder


def get_export_directory(*subdirectories: str, date_directory=True) -> pathlib.Path:
    if not EXPORT_DIRECTORY.parent.exists():
        raise NotADirectoryError(
            f"Cant create export directory under {EXPORT_DIRECTORY.parent}."
            f"Directory does not exist!"
        )
    if date_directory:
        folder = pathlib.Path(
            EXPORT_DIRECTORY,
            datetime.datetime.now().strftime("%Y%m%d"),
            *subdirectories,
        )
    else:
        folder = pathlib.Path(EXPORT_DIRECTORY, *subdirectories)
    folder.mkdir(exist_ok=True, parents=True)
    return folder


def open_file_with_default_program(path: str | pathlib.Path) -> None:
    if platform.system() == "Darwin":  # macOS
        subprocess.call(("open", str(path)))
    elif platform.system() == "Windows":  # Windows
        os.startfile(str(path))
    else:  # linux variants
        subprocess.call(("xdg-open", str(path)))


def open_file_with_excel(path: str | pathlib.Path) -> None:
    os.system(f'start EXCEL.EXE "{path}"')


def open_files_in_winmerge(*args: str | pathlib.Path) -> None:
    executable = '"C:/Program Files/WinMerge/WinMergeU.exe"'
    subprocess.call([executable, *args])


def open_file_or_directory(file_name):
    match sys.platform:
        case "win32":
            os.startfile(file_name)
        case "darwin":
            subprocess.call(["open", file_name])
        case _:
            subprocess.call(["xdg-open", file_name])


def open_export_directory(*subdirectories: str) -> None:
    open_file_or_directory(get_export_directory(*subdirectories))


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
        raise IsADirectoryError(f"Already exist: {sub_export_dir}")
    with zipfile.ZipFile(path) as zip_reference:
        members = _normalize_zip_content(zip_reference)
        zip_reference.extractall(sub_export_dir, members=members)
    return sub_export_dir


def _normalize_zip_content(zip_reference: zipfile.ZipFile) -> list[zipfile.ZipInfo]:
    """Normalize file paths in opened zip file

    Older Windows tools can create zip files that violates the zip standard. This function
    makes any absolute paths inside the archive relative and translates Windows path
    separators to POSIX."""
    members = []
    seen = set()
    for info in zip_reference.infolist():
        is_dir = info.is_dir() or info.filename.endswith("/")
        relative_path = re.sub(
            r"^[A-Za-z]:", "", info.filename.replace("\\", "/")
        ).lstrip("/")
        normalized_relative_path = posixpath.normpath(relative_path)
        if (
            normalized_relative_path in ("", ".")
            or normalized_relative_path.startswith("..")
            or normalized_relative_path in seen
        ):
            continue

        if is_dir and not normalized_relative_path.endswith("/"):
            normalized_relative_path += "/"

        seen.add(normalized_relative_path)
        info.filename = normalized_relative_path
        members.append(info)
    return members
