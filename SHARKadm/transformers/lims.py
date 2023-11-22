import pandas as pd
from .base import Transformer, DataHolderProtocol
from SHARKadm import adm_logger


class MoveLessThanFlag(Transformer):
    valid_data_holders = ['LimsDataHolder']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._q_prefix = 'Q_'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Moves flag < in value column to Q_-column'

    def _get_q_col(self, col: str) -> str:
        return f'{self._q_prefix}{col}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for col in data_holder.data.columns[:]:
            q_col = self._get_q_col(col)
            if q_col not in data_holder.data.columns:
                continue
            data_holder.data[q_col] = data_holder.data.apply(lambda row, c=col: self._add_to_q_column(c, row), axis=1)

        for col in data_holder.data.columns[:]:
            q_col = self._get_q_col(col)
            if q_col not in data_holder.data.columns:
                continue
            data_holder.data[col] = data_holder.data[col].apply(lambda x: x.lstrip('<'))

    def _add_to_q_column(self, col: str, row: pd.Series) -> str:
        q_col = self._get_q_col(col)
        if not row[col].startswith('<'):
            return row[q_col]
        if row[q_col]:
            adm_logger.log_transformation(f'Will not overwrite flag {row[q_col]} with flag <')
            return row[q_col]
        adm_logger.log_transformation(f'Moving {col} flag < to flag column')
        return '<'

