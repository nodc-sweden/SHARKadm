import pandas as pd
import polars as pl

from sharkadm.data.df.df_data_holder import PolarsDataFrameDataHolder
from sharkadm.sharkadm_logger import adm_logger


def get_data_frame_data_holder(
    df: pl.DataFrame | pd.DataFrame,
    data_type: str | None = None,
    import_matrix_key: str = "",
):
    if not data_type:
        for col in ("delivery_datatype", "datatype", "data_type"):
            if col in df.columns:
                data_types = set(df[col])
                if len(data_types) != 1:
                    adm_logger.log_workflow(
                        f"Several data types found in data column "
                        f"{col}. Consider to specify data_type in "
                        f"function call.",
                        level=adm_logger.ERROR,
                    )
                    return
                data_type = data_types.pop()
                break
        else:
            data_type = "unknown"
    return PolarsDataFrameDataHolder(
        df, data_type=data_type, import_matrix_key=import_matrix_key
    )
