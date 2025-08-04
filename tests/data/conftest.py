from pathlib import Path

import pytest


@pytest.fixture
def lims_folder(tmp_path) -> Path:
    data_file = tmp_path / "Raw_data" / "data.txt"
    data_file.parent.mkdir(parents=True)
    data_file.touch()
    return data_file
