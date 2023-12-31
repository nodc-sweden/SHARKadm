from .base import Transformer, DataHolderProtocol
from SHARKadm import adm_logger


class ReplaceCommaWithDot(Transformer):
    apply_on_columns = [
        'sample_reported_latitude',
        'sample_reported_longitude',
    ]

    def __init__(self, apply_on_columns: list[str] = None) -> None:
        super().__init__()
        if apply_on_columns:
            self.apply_on_columns = apply_on_columns

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds position to all levels if not present'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for col in self.apply_on_columns:
            if col not in data_holder.data:
                adm_logger.log_transformation(f'Missing column: {col}')
                continue
            data_holder.data[col] = data_holder.data[col].apply(self.convert)

    @staticmethod
    def convert(x) -> str:
        return x.replace(',', '.').replace(' ', '')
