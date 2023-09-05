from .base import Transformer, DataHolderProtocol


class ReplaceCommaWithDot(Transformer):
    apply_on_columns = [
        'sample_reported_latitude',
        'sample_reported_longitude',
    ]

    def transform(self, data_holder: DataHolderProtocol) -> None:
        for col in self.apply_on_columns:
            data_holder.data[col] = data_holder.data[col].apply(self.convert)

    @staticmethod
    def convert(x):
        return x.replace(',', '.')
