import pathlib


def get_next_incremented_file_path(path: pathlib.Path):
    i = 1
    new_path = _get_incremented_file_path(path, i)
    while new_path.exists():
        i += 1
        new_path = _get_incremented_file_path(path, i)
    return new_path


def _get_incremented_file_path(path, nr):
    return path.parent / f'{path.stem}({nr}){path.suffix}'