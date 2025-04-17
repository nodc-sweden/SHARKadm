import math
from datetime import date, datetime, timedelta

import polars as pl
import pytest


@pytest.fixture(scope="session")
def given_data_in_row_format(rows: int = 10):
    start_date = datetime(2025, 1, 1)
    data_list = []
    for n in range(math.ceil(rows / 3)):
        ntri = 0.5 + 0.01 * n
        ntra = 5 + 0.01 * n
        ntrz = ntri + ntra

        data_list.append(
            {
                "parameter": "NTRI",
                "value": str(ntri),
                "datetime": start_date + timedelta(days=n, seconds=10),
            }
        )
        data_list.append(
            {
                "parameter": "NTRA",
                "value": str(ntra),
                "datetime": start_date + timedelta(days=n, seconds=20),
            }
        )
        data_list.append(
            {
                "parameter": "NTRZ",
                "value": str(ntrz),
                "datetime": start_date + timedelta(days=n, seconds=30),
            }
        )

    return pl.DataFrame(data_list[:rows])


@pytest.fixture(scope="session")
def given_analyse_info():
    return {
        "NTRI": [
            {
                "VALIDFR": date(2000, 1, 1),
                "VALIDTO": date(2050, 1, 1),
                "analyse_info_field_1": "1",
                "analyse_info_field_2": "1",
                "analyse_info_field_3": "1",
            }
        ],
        "NTRA": [
            {
                "VALIDFR": date(2000, 1, 1),
                "VALIDTO": date(2050, 1, 1),
                "analyse_info_field_1": "2",
                "analyse_info_field_2": "2",
                "analyse_info_field_3": "2",
            }
        ],
        "NTRZ": [
            {
                "VALIDFR": date(2000, 1, 1),
                "VALIDTO": date(2050, 1, 1),
                "analyse_info_field_1": "3",
                "analyse_info_field_2": "3",
                "analyse_info_field_3": "3",
            }
        ],
    }
