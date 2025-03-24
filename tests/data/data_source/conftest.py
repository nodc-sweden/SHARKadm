import csv
from functools import reduce
from pathlib import Path
from typing import Iterable

import polars as pl


def csv_file_from_dict(data: Iterable[dict], file_path: Path, delimiter: str = "\t"):
    field_names = reduce(lambda a, b: a | b, [set(row.keys()) for row in data])
    with file_path.open("w") as csv_file:
        writer = csv.DictWriter(csv_file, field_names, delimiter=delimiter)
        writer.writeheader()
        writer.writerows(data)


def xlsx_file_from_dict(data: Iterable[dict], file_path: Path):
    dataframe = pl.DataFrame(data)
    dataframe.write_excel(file_path)
