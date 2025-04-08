import pathlib


class ConfigPaths:
    def __init__(self, root_path: str | pathlib.Path) -> None:
        self._root_path = pathlib.Path(root_path)
        self._paths = dict()
        self._save_paths()

    def __call__(self, item: str) -> pathlib.Path | None:
        return self._paths.get(item, None)

    def __getattr__(self, item: str) -> pathlib.Path | None:
        return self._paths.get(item, None)

    def __getitem__(self, item: str) -> pathlib.Path | None:
        return self._paths.get(item, None)

    def _save_paths(self) -> None:
        for path in self._root_path.iterdir():
            self._paths[path.stem] = path
