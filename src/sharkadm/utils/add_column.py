import polars as pl


def add_float_column(data: pl.DataFrame,
                     column: str,
                     column_name: str = "",
                     suffix: str = "_float") -> pl.DataFrame:
    print(f"{set(data[column])=}")
    if column_name:
        return data.with_columns(
            pl.when(pl.col(column).str.len_chars() == 0)
            .then(None)
            .otherwise(pl.col(column))
            .cast(float)
            .alias(column_name)
        )
    return data.with_columns(
        pl.when(pl.col(column).str.len_chars() == 0)
        .then(None)
        .otherwise(pl.col(column))
        .cast(float)
        .name.suffix(suffix)
    )
