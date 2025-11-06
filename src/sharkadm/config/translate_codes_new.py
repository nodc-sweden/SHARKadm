# This is a 'temporary' module to read data from translate_code_NEW.txt
# that is not implemented in sharkadm yet. But we need this fil to find the list
# of valid size_class_ref_list_code

import functools

import polars as pl

from sharkadm.sharkadm_logger import adm_logger
from sharkadm.utils import get_nodc_config_directory


@functools.cache
def get_valid_size_class_ref_list_codes() -> list[str]:
    path = get_nodc_config_directory() / "translate_codes_NEW.txt"
    if not path.exists():
        adm_logger.log_workflow(f"Could not find file: {path}", level=adm_logger.ERROR)
        return []
    df = pl.read_csv(path, separator="\t", encoding="cp1252")
    return df.filter(pl.col("field") == "size_class_ref_list_code")["code"].to_list()


if __name__ == "__main__":
    lst = get_valid_size_class_ref_list_codes()
