import hashlib
import shutil
from dataclasses import dataclass
from multiprocessing.dummy import Pool as ThreadPool
from pathlib import Path
from typing import Protocol

from sharkadm import event
from sharkadm.utils import svn
from enum import StrEnum, auto


class ConfigStates(StrEnum):
    PROD = auto()
    TEST = auto()


class ConfigSync:
    def __init__(
        self,
        prod_dir: Path | str,
        test_dir: Path | str | None = None,
    ):

        self._prod_dir: Path = Path(prod_dir)
        if not self._prod_dir.exists() or self._prod_dir.is_file():
            raise NotADirectoryError(f"Invalid config directory: {self._prod_dir}")

        self._config_name_prod: str = self._prod_dir.name
        self._config_name_test: str = f"{self._config_name_prod}_test"

        self._test_dir: Path | str | None = test_dir

        self._check_and_fix_test_dir()

        self._prod_files: dict[str, Path] = dict()
        self._test_files: dict[str, Path] = dict()

        self._only_in_prod: set[str] = self._prod_files.keys() - self._test_files.keys()
        self._only_in_test: set[str] = self._test_files.keys() - self._prod_files.keys()

        self._changed_files: set[str] = set()
        self._check_hash_files: list[tuple[Path, Path, str]] = []

    def _check_and_fix_test_dir(self) -> None:
        if self._test_dir is None:
            self._test_dir = self._prod_dir.parent / self._config_name_test

        if self._test_dir.name == self._config_name_test:
            self._test_dir.mkdir(parents=True, exist_ok=True)
            return
        if self._test_dir.exists():
            self._test_dir = self._test_dir / self._config_name_test
            self._test_dir.mkdir(parents=True, exist_ok=True)
            return

    def sync(self):
        self.check()
        self.copy_new_or_updated_files_to_test()
        self._update_inventory()

    def check(self) -> None:
        self._changed_files = set()
        self._check_hash_files = []

        self._update_inventory()
        self._check_files()
        self._add_hash_diff()

    def _update_inventory(self):
        self._prod_files = self._get_file_map(self._prod_dir)
        self._test_files = self._get_file_map(self._test_dir)

        self._only_in_prod = self._prod_files.keys() - self._test_files.keys()
        self._only_in_test = self._test_files.keys() - self._prod_files.keys()

    @property
    def prod_dir(self) -> Path:
        return self._prod_dir

    @property
    def test_dir(self) -> Path:
        return self._test_dir

    @property
    def prod_files(self) -> dict[str, Path]:
        return self._prod_files

    @property
    def test_files(self) -> dict[str, Path]:
        return self._test_files

    @property
    def files_not_in_test(self) -> set[str]:
        return self._only_in_prod

    @property
    def changed_files(self) -> set[str]:
        return self._changed_files

    @property
    def new_or_updated_files_in_prod(self) -> set[str]:
        return self.files_not_in_test | self.changed_files

    def get_prod_path(self, name: str) -> Path | None:
        return self.prod_dir / self._prod_files.get(name)

    def get_test_path(self, name: str) -> Path | None:
        return self.test_dir / self._test_files.get(name)

    def copy_new_or_updated_files_to_test(self) -> None:
        tot_nr = len(self.new_or_updated_files_in_prod)
        for n, name in enumerate(self.new_or_updated_files_in_prod):
            rel = self.prod_files[name]
            source_path = self._prod_dir / rel
            target_path = self._test_dir / rel
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, target_path)
            event.post_event(
                event.Events.LOG_PROGRESS,
                dict(total=tot_nr, current=n, title=f"Copying config file {rel}..."),
            )

    def _check_files(self):
        for name in self._prod_files.keys() & self._test_files.keys():
            f1 = self.get_prod_path(name)
            f2 = self.get_test_path(name)

            if f1.stat().st_size != f2.stat().st_size:
                self._changed_files.add(name)
                continue
            self._check_hash_files.append((f1, f2, name))

    def _add_hash_diff(self):
        pool = ThreadPool(4)
        diff_files = pool.map(self._check_hash_diff, self._check_hash_files)
        self._changed_files.update([file for file in diff_files if file])

    @staticmethod
    def _get_file_map(root):
        root = Path(root)
        files = dict()
        for path in root.rglob("*"):
            if path.is_dir():
                continue
            path_str = str(path)
            if ".svn" in path_str:
                continue
            if ".idea" in path_str:
                continue
            files[path.stem] = path.relative_to(root)
        return files

    @staticmethod
    def _get_hash_of_file(path: Path) -> str:
        with open(str(path), "rb") as f:
            return hashlib.file_digest(f, hashlib.sha256).hexdigest()

    def _check_hash_diff(self, val):
        f1, f2, rel = val
        if self._get_hash_of_file(f1) != self._get_hash_of_file(f2):
            return rel


class ConfigState(Protocol):
    def update(self) -> None: ...

    def commit(self) -> None: ...

    def get_path(self, name: str) -> Path | None: ...

    def get_root_dir(self) -> Path: ...

    def set_to_prod(self) -> None: ...

    def set_to_test(self) -> None: ...


#
#
# class ConfigContext(Protocol):
#     config_sync: ConfigSync
#
#     def set_state(self, state: ConfigState) -> None: ...
#
#     def update(self) -> None: ...
#
#     def commit(self) -> None: ...
#
#     def get_path(self, name: str) -> Path | None: ...
#
#     def set_to_prod(self) -> None: ...
#
#     def set_to_test(self) -> None: ...

# def get_prod_path(self, name: str) -> Path | None:
#     ...
#
# def get_test_path(self, name: str) -> Path | None:
#     ...


@dataclass
class ProdState:
    config: "Config"
    state: str = ConfigStates.PROD

    def update(self) -> None:
        info = svn.get_svn_info(self.get_root_dir())
        if not info:
            return
        svn.update_svn_directory(self.get_root_dir())

    def commit(self) -> None:
        print("Check and commit svn")

    def get_path(self, name: str) -> Path | None:
        return self.config.config_sync.get_prod_path(name)

    def get_root_dir(self) -> Path:
        return self.config.config_sync.prod_dir

    def set_to_prod(self) -> None:
        print("Already in prod!!")

    def set_to_test(self) -> None:
        self.config.set_state(TestState(self.config))


@dataclass
class TestState:
    config: "Config"
    state: str = ConfigStates.TEST

    def update(self) -> None:
        self.config.config_sync.sync()

    def commit(self) -> None:
        print("Cant commit test config!")

    def get_path(self, name: str) -> Path | None:
        return self.config.config_sync.get_test_path(name)

    def get_root_dir(self) -> Path:
        return self.config.config_sync.test_dir

    def set_to_prod(self) -> None:
        self.config.set_state(ProdState(self.config))

    def set_to_test(self) -> None:
        print("Already in test!!")


class Config:
    def __init__(self, config_root: Path | str):
        self.state: ConfigState = ProdState(self)
        self.config_sync = ConfigSync(config_root)

    def __str__(self) -> str:
        return str(self.state)

    def set_state(self, state: ConfigState):
        self.state = state

    def update(self) -> None:
        self.state.update()

    def commit(self) -> None:
        self.state.commit()

    @property
    def unsynced_files(self) -> list[str]:
        self.config_sync.new_or_updated_files_in_prod

    @property
    def test_is_synced_with_prod(self) -> bool:
        self.config_sync.check()
        return not bool(self.config_sync.new_or_updated_files_in_prod)

    def sync_test_with_prod(self) -> None:
        self.config_sync.sync()

    def get_path(self, name: str) -> Path | None:
        return self.state.get_path(name)

    def set_to_prod(self, update: bool = False) -> None:
        self.state.set_to_prod()
        if update:
            self.update()

    def set_to_test(self, update: bool = False) -> None:
        self.state.set_to_test()
        if update:
            self.update()

    # @property
    # def states(self) -> list[str]:
    #     return ["PROD", "TEST"]

    @property
    def root_dir(self) -> Path:
        return self.state.get_root_dir()
