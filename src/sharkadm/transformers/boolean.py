from types import MappingProxyType

import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.transformers.base import DataHolderProtocol, PolarsTransformer, Transformer
from sharkadm.utils import matching_strings


class FixYesNo(Transformer):
    apply_on_columns = (".*accreditated",)
    _mapping = MappingProxyType({"y": "Y", "yes": "Y", "n": "N", "no": "N"})

    def __init__(self, apply_on_columns: list[str] | None = None, **kwargs):
        super().__init__(**kwargs)
        if apply_on_columns:
            self.apply_on_columns = apply_on_columns

    @staticmethod
    def get_transformer_description() -> str:
        return "Fix boolean values to YES or No (Y or N?)"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        use_columns = matching_strings.get_matching_strings(
            strings=data_holder.data.columns, match_strings=self.apply_on_columns
        )
        for col in use_columns:
            data_holder.data[col] = data_holder.data[col].apply(self._map_value)

    def _map_value(self, x: str):
        return self._mapping.get(x.lower(), x)


class PolarsFixYesNo(PolarsTransformer):
    apply_on_columns = (".*accreditated",)
    _mapping = MappingProxyType({"y": "Y", "yes": "Y", "n": "N", "no": "N"})

    def __init__(self, apply_on_columns: list[str] | None = None, **kwargs):
        super().__init__(**kwargs)
        if apply_on_columns:
            self.apply_on_columns = apply_on_columns

    @staticmethod
    def get_transformer_description() -> str:
        return "Fix boolean values to YES or No (Y or N?)"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        use_columns = matching_strings.get_matching_strings(
            strings=data_holder.data.columns, match_strings=self.apply_on_columns
        )

        for col in use_columns:
            data_holder.data = data_holder.data.with_columns(
                pl.col(col)
                .str.to_lowercase()
                .replace_strict(self._mapping, default=pl.col(col))
            )
