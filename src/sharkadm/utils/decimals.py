import polars as pl


def set_nr_decimals(
    data: pl.DataFrame, col: str, nr_decimals: int, pad_end: bool = False
) -> pl.DataFrame:
    # Round value
    data = data.with_columns(
        pl.when(pl.col(col) != "")
        .then(
            pl.col(col)
            .cast(pl.Float64, strict=False)
            .round(
                nr_decimals,
                mode="half_away_from_zero",
            )
            .cast(pl.String)
        )
        .otherwise(pl.col(col))
        .alias(col)
    )
    # Add trailing padding
    if pad_end:
        data = data.with_columns(
            pl.when(pl.col(col) != "")
            .then(
                pl.concat_str(
                    [
                        pl.col(col).str.split(".").list.get(0, null_on_oob=True),
                        pl.col(col)
                        .str.split(".")
                        .list.get(1, null_on_oob=True)
                        .str.slice(0, nr_decimals)
                        .str.pad_end(nr_decimals, fill_char="0"),
                    ],
                    separator=".",
                )
            )
            .otherwise(pl.col(col))
            .alias(col)
        )
    return data
