from .base import Transformer, DataHolderProtocol


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

