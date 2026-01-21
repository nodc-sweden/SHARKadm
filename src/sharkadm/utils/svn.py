import pathlib
import shutil
import subprocess
from dataclasses import dataclass


@dataclass
class SvnInfo:
    path: str
    working_copy_root_path: str
    url: str
    relative_url: str
    repository_root: str
    repository_uuid: str
    revision: str
    node_kind: str
    schedule: str
    last_changed_author: str
    last_changed_rev: str
    last_changed_date: str

    def __repr__(self):
        return f"SVN repo: {self.working_copy_root_path} (revision {self.revision})"

    @classmethod
    def from_subprocess(cls, path: str | pathlib.Path):
        if not pathlib.Path(path).exists():
            return
        try:
            svn_info = subprocess.check_output("svn info " + str(path))
            info = {}
            for line in svn_info.decode().split("\r\n"):
                if not line.strip():
                    continue
                split_line = line.split(":", 1)
                info[split_line[0].strip().lower().replace(" ", "_")] = split_line[
                    1
                ].strip()
            return SvnInfo(**info)
        except subprocess.CalledProcessError:
            return


def get_svn_info(path: str | pathlib.Path):
    return SvnInfo.from_subprocess(path)


def update_svn_directory(directory: pathlib.Path) -> bool:
    status = subprocess.run(["svn", "update"], cwd=directory)
    return bool(status.returncode)


def commit_files(*paths: pathlib.Path, msg: str = "Auto commit by sharkadm") -> bool:
    svn_exec = shutil.which("svn")
    if svn_exec is None:
        return False
    for path in paths:
        args = [svn_exec, "commit", str(path), "-m", f'"{msg}"']
        subprocess.run(args)
    return True


def _split_svn_status_message(msg: bytes) -> dict[str, pathlib.Path]:
    files = dict()
    for row in msg.decode().split("\r\n"):
        if row.startswith("?"):
            _, path_str = row.split()
            files.setdefault("new", [])
            files["new"].append(pathlib.Path(path_str))
        elif row.startswith("M"):
            _, path_str = row.split()
            files.setdefault("modified", [])
            files["modified"].append(pathlib.Path(path_str))
    return files


def get_modified_svn_files(
    repo: pathlib.Path | str,
    match_paths: list[pathlib.Path] | tuple[pathlib.Path, ...] | None = None,
) -> list[pathlib.Path]:
    args = ["svn", "status", repo]
    ans = subprocess.check_output(args)
    result = _split_svn_status_message(ans)
    if not match_paths:
        return result.get("modified", [])
    paths = []
    for path in result.get("modified", []):
        if path in match_paths:
            paths.append(path)
    return paths
