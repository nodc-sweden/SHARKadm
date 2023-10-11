from .base import Transformer, DataHolderProtocol
from SHARKadm import adm_logger


class ReplaceCommaWithDot(Transformer):
    default_columns = [
        'sample_reported_latitude',
        'sample_reported_longitude',
    ]

    def __init__(self, apply_on_columns: list[str] = None) -> None:
        self.apply_on_columns = self.default_columns
        if apply_on_columns:
            self.apply_on_columns = apply_on_columns

    @property
    def transformer_description(self) -> str:
        cols = '\n'.join(self.default_columns)
        return f'Replaces comma with dot for the given columns. Columns that should be modified can be given in the ' \
               f'constructor. This object will modify the following columns:\n{cols}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for col in self.apply_on_columns:
            data_holder.data[col] = data_holder.data[col].apply(self.convert)

    @staticmethod
    def convert(x) -> str:
        return x.replace(',', '.').replace(' ', '').split('.')[0]
