import datetime

import pandas as pd
import re

from .base import Transformer, DataHolderProtocol
from sharkadm import adm_logger
from sharkadm.config import get_column_views_config


class AddColumnViewsColumns(Transformer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._column_views = get_column_views_config()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds empty columns from column_views not already present in dataframe. ' \
               f'This transformer should be used with caution since no data is added.'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        columns_to_add = self._column_views.get_columns_for_view(data_holder.data_type)
        for col in columns_to_add:
            if col in data_holder.data.columns:
                continue
            data_holder.data[col] = ''


class AddDEPHqcColumn(Transformer):
    valid_data_holders = ['LimsDataHolder']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._column_views = get_column_views_config()

    @staticmethod
    def get_transformer_description() -> str:
        return 'Adds QC column for DEPH if missing'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if 'Q_DEPH' not in data_holder.data.columns:
            data_holder.data['Q_DEPH'] = ''


class RemoveColumns(Transformer):
    def __init__(self, *args, **kwargs):
        self._args = args
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return 'Removes columns matching given strings in args'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        exclude_columns = []
        for col in data_holder.data.columns:
            for arg in self._args:
                if re.match(arg, col):
                    exclude_columns.append(col)
                    break
        keep_columns = [col for col in data_holder.data.columns if col not in exclude_columns]
        data_holder.data = data_holder.data[keep_columns]




