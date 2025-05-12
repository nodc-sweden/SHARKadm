from .base import Transformer, DataHolderProtocol
import polars as pl


class AddDatatype(Transformer):
    datatype_column_name = 'delivery_datatype'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds delivery_datatype column'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.datatype_column_name] = data_holder.data_type


class AddDatatypePlanktonBarcoding(Transformer):
    valid_data_types = ['plankton_barcoding']

    datatype_column_name = 'delivery_datatype'
    value_to_set = 'Plankton Barcoding'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Sets delivery_datatype column to {AddDatatypePlanktonBarcoding.value_to_set}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.datatype_column_name] = self.value_to_set


class PolarsAddDatatype(Transformer):
    col_to_set = 'delivery_datatype'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds {PolarsAddDatatype.col_to_set} column'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data = data_holder.data.with_columns(pl.lit(data_holder.data_type).alias(self.col_to_set))


class PolarsAddDatatypePlanktonBarcoding(Transformer):
    valid_data_types = ['plankton_barcoding']

    col_to_set = 'delivery_datatype'
    value_to_set = 'Plankton Barcoding'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Sets {PolarsAddDatatypePlanktonBarcoding.col_to_set} column to {PolarsAddDatatypePlanktonBarcoding.value_to_set}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data = data_holder.data.with_columns(pl.lit(data_holder.data_type).alias(self.col_to_set))


