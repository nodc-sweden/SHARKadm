import polars as pl

from ..data import PolarsDataHolder
from . import PolarsTransformer


class PolarsAddDatatype(PolarsTransformer):
    col_to_set = "delivery_datatype"

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds {PolarsAddDatatype.col_to_set} column"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.lit(data_holder.data_type_in_data).alias(self.col_to_set)
        )


class PolarsAddDatatypePlanktonBarcoding(PolarsTransformer):
    valid_data_types = ("plankton_barcoding",)

    datatype_column_name = "delivery_datatype"
    value_to_set = "Plankton Barcoding"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Sets delivery_datatype column to "
            f"{PolarsAddDatatypePlanktonBarcoding.value_to_set}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.lit(self.value_to_set).alias(self.datatype_column_name)
        )
