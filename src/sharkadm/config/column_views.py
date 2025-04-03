import pathlib


class ColumnViews:
    def __init__(self, path: str | pathlib.Path):
        self._path = pathlib.Path(path)
        self._config = dict()
        self._load_config()

    def _load_config(self) -> None:
        header = list()
        with open((self)._path) as fid:
            for r, line in enumerate(fid):
                if not line.strip():
                    continue
                split_line = [item.strip().lower() for item in line.split("\t")]
                if r == 0:
                    header = split_line
                    self._config = {key: [] for key in header}
                    continue
                for key, col in zip(header, split_line):
                    if not col:
                        continue
                    self._config[key].append(col)

    def get_columns_for_view(self, view: str) -> list[str]:
        view = view.lower()
        return self._config.get(
            view,
            self._config.get(
                f"sharkdata_{view}", self._config.get(f"sharkweb_{view}", [])
            ),
        )
