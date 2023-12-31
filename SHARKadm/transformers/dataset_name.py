from .base import Transformer, DataHolderProtocol


class AddDatasetName(Transformer):
    datatype_column_name = 'dataset_name'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds dataset_name column'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.datatype_column_name] = data_holder.dataset_name

