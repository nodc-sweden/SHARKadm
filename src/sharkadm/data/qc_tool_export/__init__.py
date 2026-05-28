import pathlib
from typing import Union

from sharkadm import config

from .qc_tool_working_file_holder import PolarsQcToolDataHolder


def get_polars_qc_file_data_holder(
    path: str | pathlib.Path, **kwargs
) -> PolarsQcToolDataHolder:
    path = pathlib.Path(path)
    mapper = None
    if not kwargs.get("keep_header"):
        mapper = config.get_import_matrix_mapper(
            data_type="physicalchemical", import_column="LIMS"
        )
    return PolarsQcToolDataHolder(qc_file_path=path, header_mapper=mapper)
