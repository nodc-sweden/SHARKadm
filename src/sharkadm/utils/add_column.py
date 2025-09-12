import polars as pl


def add_float_column(
    data: pl.DataFrame, column: str, column_name: str = "", suffix: str = "float"
) -> pl.DataFrame:
    new_column_name = column_name
    if not new_column_name:
        suffix = suffix.strip("_")
        new_column_name = f"{column}_{suffix}"
    if column not in data.columns:
        return data.with_columns(
            pl.lit(None)
            .cast(float)
            .alias(new_column_name)
        )
    return data.with_columns(
        pl.when(pl.col(column).str.len_chars() == 0)
        .then(None)
        .otherwise(pl.col(column))
        .cast(float)
        .alias(new_column_name)
    )
