import polars as pl

from sharkadm import adm_logger
from sharkadm.transformers.base import (
    DataHolderProtocol,
    PolarsDataHolderProtocol,
    PolarsTransformer,
    Transformer,
)
from sharkadm.utils import matching_strings


class ReplaceCommaWithDot(Transformer):
    apply_on_columns = (
        ".*latitude.*",
        ".*longitude.*",
        "water_depth_m",
        ".*DIVIDE.*",
        ".*MULTIPLY.*",
        ".*COPY_VARIABLE.*",
        "sampled_volume.*",
        "sampler_area.*",
        ".*wind.*",
        ".*pressure.*",
        ".*temperature.*",
    )

    def __init__(self, apply_on_columns: tuple[str] | None = None) -> None:
        super().__init__()
        if apply_on_columns:
            self.apply_on_columns = apply_on_columns

        self._handled_cols = dict()

    @staticmethod
    def get_transformer_description() -> str:
        return "Replacing comma with dot in given columns"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for col in self._get_matching_cols(data_holder):
            for item, df in data_holder.data.groupby(col):
                item = str(item)
                if "," not in item:
                    continue
                new_item = item.replace(",", ".")
                self._log(
                    f"Replacing comma with dot for value {item} in column {col}",
                    level=adm_logger.INFO,
                )
                data_holder.data.loc[df.index, col] = new_item

    def _get_matching_cols(self, data_holder: DataHolderProtocol) -> list[str]:
        return matching_strings.get_matching_strings(
            strings=data_holder.data.columns, match_strings=self.apply_on_columns
        )


class PolarsReplaceCommaWithDot(PolarsTransformer):
    apply_on_columns = (
        "latitude",
        "longitude",
        "depth",
        "DIVIDE",
        "MULTIPLY",
        "COPY_VARIABLE",
        "sampled_volume",
        "sampler_area",
        "wind",
        "pressure",
        "temperature",
    )

    def __init__(self, apply_on_columns: tuple[str] | None = None) -> None:
        super().__init__()
        if apply_on_columns:
            self.apply_on_columns = apply_on_columns

        self._handled_cols = dict()

    @staticmethod
    def get_transformer_description() -> str:
        return "Replacing comma with dot in given columns"

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
        columns = self._get_matching_cols(data_holder)
        transformed_data = data_holder.data.with_columns(
            [pl.col(column).str.replace(",", ".").alias(column) for column in columns]
        )

        if columns:
            unchanged_rows = data_holder.data.join(
                transformed_data, on=columns, how="semi"
            )

            changed_rows = list(
                data_holder.data.filter(
                    ~pl.col("row_number").is_in(unchanged_rows["row_number"])
                )["row_number"]
            )

            if changed_rows:
                self._log(
                    f"Replaced comma with dot in {len(changed_rows)} rows.",
                    row_numbers=changed_rows,
                )

        data_holder.data = transformed_data

    def _get_matching_cols(self, data_holder: PolarsDataHolderProtocol) -> list[str]:
        return matching_strings.get_matching_strings(
            strings=data_holder.data.columns, match_strings=self.apply_on_columns
        )


class ReplaceCommaWithDotPolars(Transformer):
    apply_on_columns = (
        ".*latitude.*",
        ".*longitude.*",
        "water_depth_m",
        ".*DIVIDE.*",
        ".*MULTIPLY.*",
        ".*COPY_VARIABLE.*",
        "sampled_volume.*",
        "sampler_area.*",
        ".*wind.*",
        ".*pressure.*",
        ".*temperature.*",
    )

    def __init__(self, apply_on_columns: tuple[str] | None = None) -> None:
        super().__init__()
        if apply_on_columns:
            self.apply_on_columns = apply_on_columns

        self._handled_cols = dict()

    @staticmethod
    def get_transformer_description() -> str:
        return "Replacing comma with dot in given columns"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for col in self._get_matching_cols(data_holder):
            data_holder.data = data_holder.data.with_columns(
                _temp=pl.col(col).str.replace(",", ".")
            )
            for (old, new), df in data_holder.data.group_by([col, "_temp"]):
                if old == new:
                    continue
                self._log(
                    f"Replacing comma with dot for value {old} in column {col} "
                    f"({len(df)} places)",
                    level=adm_logger.INFO,
                )

    def _get_matching_cols(self, data_holder: DataHolderProtocol) -> list[str]:
        return matching_strings.get_matching_strings(
            strings=data_holder.data.columns, match_strings=self.apply_on_columns
        )
