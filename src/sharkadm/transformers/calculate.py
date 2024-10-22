from .base import Transformer, DataHolderProtocol
import pandas as pd


class Occurrence:

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    @property
    def counted(self):
        return int(list(self._df[self._df['parameter'] == '# counted']['value'])[0])

    @property
    def coefficient(self):
        return int(list(self._df['coefficient'])[0])


class Calculate(Transformer):
    occurrence_id_col = 'custom_occurrence_id'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Calculate abundance and other stuf'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for _id, df in data_holder.data.groupby(self.occurrence_id_col):
            occ = Occurrence(df)