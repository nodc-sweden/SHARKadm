from .base import Transformer, DataHolderProtocol


class AddDatatype(Transformer):
    datatype_column_name = 'delivery_datatype'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds delivery_datatype column'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.datatype_column_name] = data_holder.data_type

