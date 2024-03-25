import pandas as pd
import numpy as np
from .base import Transformer, DataHolderProtocol
from sharkadm import adm_logger


class MoveLessThanFlagRowFormat(Transformer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f'Moves flag < in value column to quality_flag column'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        boolean = data_holder.data['value'].str.startswith('<')

        qf_boolean = data_holder.data['value'] != ''

        move_boolean = boolean & qf_boolean
        data_holder.data.loc[move_boolean, 'quality_flag'] = '<'

        data_holder.data['value'] = data_holder.data['value'].str.lstrip('<')

        nr_flags = boolean.value_counts().get(True)
        if nr_flags:
            adm_logger.log_transformation(f'Moving {nr_flags} "<"-flags to quality_flag column')

class MoveLessThanFlagColumnFormat(Transformer):
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


class RemoveNonDataLines(Transformer):
    valid_data_holders = ['LimsDataHolder']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f'Removes SLA and ZOO lines in data'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        sla_bool = data_holder.data['sample_id'].str.contains('-SLA_')
        zoo_bool = data_holder.data['sample_id'].str.contains('-ZOO_')
        remove_bool = sla_bool | zoo_bool
        data_holder.data = data_holder.data[~remove_bool]


