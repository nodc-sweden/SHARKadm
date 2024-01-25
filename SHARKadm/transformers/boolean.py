from .base import Transformer, DataHolderProtocol
from ..utils import matching_strings


class FixYesNo(Transformer):
    apply_on_columns = ['.*accreditated']
    _mapping = {
        'y': 'Y',
        'yes': 'Y',
        'n': 'N',
        'no': 'N'
    }

    def __init__(self, apply_on_columns: list[str] | None = None, **kwargs):
        super().__init__(**kwargs)
        if apply_on_columns:
            self.apply_on_columns = apply_on_columns

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds cruise id column'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        use_columns = matching_strings.get_matching_strings(strings=data_holder.data.columns,
                                                     match_strings=self.apply_on_columns)
        for col in use_columns:
            data_holder.data[col] = data_holder.data[col].apply(self._map_value)

    def _map_value(self, x: str):
        return self._mapping.get(x.lower(), x)