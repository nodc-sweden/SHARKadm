from typing import Sequence

import polars as pl

from sharkadm.utils import decimals

from ..data import PolarsDataHolder
from .base import PolarsTransformer


class PolarsStripAndSetNrDecimals(PolarsTransformer):
    def __init__(
        self,
        apply_on_columns: Sequence[str],
        nr_decimals: int,
        pad_end_char: str | None = None,
    ) -> None:
        super().__init__()
        self.apply_on_columns = apply_on_columns
        self.nr_decimals = nr_decimals
        self.pad_end_char = pad_end_char

    @staticmethod
    def get_transformer_description() -> str:
        return "Sets nr decimals to given columns. Pads with zeroes if needed."

    #
    # def _transform(self, data_holder: PolarsDataHolder) -> None:
    #     for col in self.apply_on_columns:
    #         data_holder.data = data_holder.data.with_columns(
    #         pl.when(pl.col(col) != "")
    #         .then(
    #             pl.col(col).str.replace(
    #                 rf"^([^.]+\.\d{{{self.nr_decimals}}})\d*$", r"$1"
    #             ).str.pad_end(3 + self.nr_decimals, fill_char="0")
    #         )
    #         .otherwise(pl.col(col))
    #         .alias(col)
    #     )
    def _transform(self, data_holder: PolarsDataHolder) -> None:
        for col in self.apply_on_columns:
            # Add trailing . if missing
            data_holder.data = data_holder.data.with_columns(
                pl.when((pl.col(col) != "") & ~(pl.col(col).str.contains(".")))
                .then(pl.concat_str([pl.col(col), pl.lit(".")], separator=""))
                .otherwise(pl.col(col))
                .alias(col)
            )
            # Strip value
            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(col) != "")
                .then(
                    pl.concat_str(
                        [
                            pl.col(col).str.split(".").list[0],
                            pl.col(col)
                            .str.split(".")
                            .list[1]
                            .str.slice(0, self.nr_decimals),
                        ],
                        separator=".",
                    )
                )
                .otherwise(pl.col(col))
                .alias(col)
            )
            # Add trailing padding
            if self.pad_end_char:
                data_holder.data = data_holder.data.with_columns(
                    pl.when(pl.col(col) != "")
                    .then(
                        pl.concat_str(
                            [
                                pl.col(col).str.split(".").list[0],
                                pl.col(col)
                                .str.split(".")
                                .list[1]
                                .str.pad_end(
                                    self.nr_decimals, fill_char=self.pad_end_char
                                ),
                            ],
                            separator=".",
                        )
                    )
                    .otherwise(pl.col(col))
                    .alias(col)
                )


class PolarsRoundAndSetNrDecimals(PolarsTransformer):
    def __init__(
        self, apply_on_columns: Sequence[str], nr_decimals: int, pad_end: bool = False
    ) -> None:
        super().__init__()
        self._apply_on_columns = apply_on_columns
        self._nr_decimals = nr_decimals
        self._pad_end = pad_end

    @staticmethod
    def get_transformer_description() -> str:
        return "Sets nr decimals to given columns. Pads with zeroes if needed."

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        for col in self._apply_on_columns:
            data_holder.data = decimals.set_nr_decimals(
                data_holder.data,
                col=col,
                nr_decimals=self._nr_decimals,
                pad_end=self._pad_end,
            )
