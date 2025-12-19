import polars as pl

from sharkadm.config import get_header_mapper_from_data_holder
from sharkadm.data import PolarsDataHolder
from sharkadm.transformers import PolarsTransformer


class PolarsAddParameterShortColumn(PolarsTransformer):
    valid_data_structures = ("row",)
    valid_data_types = ("physicalchemical",)

    col_to_set = "parameter_short"
    source_col = "parameter"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds new parameter "
            f"column {PolarsAddParameterShortColumn.col_to_set} "
            f"translated to short name."
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.col(self.source_col).alias(self.col_to_set)
        )
        mapper = get_header_mapper_from_data_holder(
            data_holder, import_column="PhysicalChemical"
        )
        for par in set(data_holder.data[self.source_col]):
            mapped_par = mapper.get_external_name(par)
            if mapped_par == par:
                continue
            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(self.source_col) == par)
                .then(pl.lit(mapped_par))
                .otherwise(pl.col(self.col_to_set))
                .alias(self.col_to_set)
            )
