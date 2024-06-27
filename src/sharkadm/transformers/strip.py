from .base import Transformer, DataHolderProtocol


class StripAllValues(Transformer):

    @staticmethod
    def get_transformer_description() -> str:
        return f'Strips all values in data'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data = data_holder.data.map(lambda x: x.strip() if type(x) == str else x)
