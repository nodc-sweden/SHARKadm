# This is a 'temporary' module to read data from translate_code_NEW.txt
# that is not implemented in sharkadm yet. But we need this fil to find the list
# of valid size_class_ref_list_code

import functools

import polars as pl

from sharkadm.sharkadm_logger import adm_logger
from sharkadm.utils import get_nodc_config_directory

FILE_PATH = None
if get_nodc_config_directory():
    FILE_PATH = get_nodc_config_directory() / "translate_codes_NEW.txt"


def _file_exists() -> bool:
    if not FILE_PATH or not FILE_PATH.exists():
        adm_logger.log_workflow(
            f"Could not find file: {FILE_PATH}", level=adm_logger.ERROR
        )
        return False
    return True


@functools.cache
def get_translate_codes_new_data() -> pl.DataFrame | None:
    if not _file_exists():
        return
    return pl.read_csv(FILE_PATH, separator="\t", encoding="cp1252")


def get_valid_size_class_ref_list_codes() -> list[str]:
    if not _file_exists():
        return []
    df = get_translate_codes_new_data()
    return df.filter(pl.col("field") == "size_class_ref_list_code")["code"].to_list()


def get_valid_species_flag_codes() -> list[str]:
    if not _file_exists():
        return []
    df = get_translate_codes_new_data()
    return df.filter(pl.col("field") == "species_flag_code")["code"].to_list()


if __name__ == "__main__":
    lst = get_valid_size_class_ref_list_codes()
