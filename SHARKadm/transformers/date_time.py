import datetime

import pandas as pd

from .base import Transformer, DataHolderProtocol
from SHARKadm import adm_logger

DATETIME_FORMATS = [
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%d'
]


class AddDateAndTimeToAllLevels(Transformer):
    dates_to_sync = [
        'sample_date',
        'visit_date'
    ]  # In order of prioritization

    @staticmethod
    def get_transformer_description() -> str:
        return 'Adds date and time column to all levels'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for par in self.dates_to_sync:
            data_holder.data[par] = data_holder.data.apply(lambda row, p=par: self.check_and_add(p, row), axis=1)

    def check_and_add(self, par: str, row: pd.Series) -> str:
        if row.get(par):
            return row[par]
        for date_par in self.dates_to_sync:
            if row.get(date_par):
                adm_logger.log_transformation(f'Added {par} from {date_par}')
                return row[date_par]
        return ''


class AddDatetime(Transformer):
    datetime_source_column = 'sample_date'

    @staticmethod
    def get_transformer_description() -> str:
        return 'Adds column datetime. Time is taken from sample_date'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data['datetime'] = data_holder.data[self.datetime_source_column].apply(self.to_datetime)

    @staticmethod
    def to_datetime(x: str) -> datetime.datetime:
        for form in DATETIME_FORMATS:
            try:
                return datetime.datetime.strptime(x, form)
            except ValueError:
                continue


class AddMonth(Transformer):
    month_columns = [
        'sample_month',
        'visit_month'
    ]

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds month column to date. Month is taken from the datetime column and will overwrite old value'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if 'datetime' not in data_holder.data.columns:
            adm_logger.log_transformation(f'Missing key: datetime')
            return
        for col in self.month_columns:
            data_holder.data[col] = data_holder.data['datetime'].apply(lambda x, dh=data_holder: self.get_month(x, dh))

    @staticmethod
    def get_month(x: datetime.datetime, data_holder: DataHolderProtocol) -> str:
        if not x:
            adm_logger.log_transformation(f'Missing datetime in {data_holder}')
            return ''
        return str(x.month).zfill(2)

