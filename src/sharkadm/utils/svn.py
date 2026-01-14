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


def commit_files(*paths: pathlib.Path, msg: str = "Auto commit by sharkadm"):
    svn_exec = shutil.which('svn')
    if svn_exec is None:
        return
    for path in paths:
        args = [
            svn_exec,
            "commit",
            str(path),
            "-m",
            f'"{msg}"'
        ]
        subprocess.run(args)

