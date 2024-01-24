import datetime

import pandas as pd

from .base import Transformer, DataHolderProtocol
from SHARKadm import adm_logger

DATETIME_FORMATS = [
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%d %H:%M',
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
            data_holder.data[par] = data_holder.data.apply(lambda row, p=par: self._check_and_add(p, row), axis=1)
        self._split_date_and_time(data_holder=data_holder)

    def _check_and_add(self, par: str, row: pd.Series) -> str:
        if row.get(par):
            return row[par]
        for date_par in self.dates_to_sync:
            if row.get(date_par):
                adm_logger.log_transformation(f'Added {par} from {date_par}', level=adm_logger.INFO)
                return row[date_par]
        return ''

    def _split_date_and_time(self, data_holder: DataHolderProtocol):
        for date_par in self.dates_to_sync:
            time_par = date_par.replace('date', 'time')
            data_holder.data[time_par] = data_holder.data[date_par].apply(self._get_time_from_date)
            data_holder.data[date_par] = data_holder.data[date_par].apply(self._get_date_from_date)

    def _get_time_from_date(self, x: str) -> str:
        parts = x.strip().split()
        if len(parts) == 2:
            adm_logger.log_transformation('Setting time string from date string', level=adm_logger.DEBUG)
            return parts[1]
        return ''

    def _get_date_from_date(self, x: str) -> str:
        if ' ' not in x:
            return x
        return x.strip().split()[0]




class AddDatetime(Transformer):
    date_source_column = 'sample_date'
    time_source_column = 'sample_time'

    @staticmethod
    def get_transformer_description() -> str:
        return 'Adds column datetime. Time is taken from sample_date'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data['date_and_time'] = data_holder.data[self.date_source_column]
        if self.time_source_column in data_holder.data.columns:
            data_holder.data['date_and_time'] = data_holder.data['date_and_time'] + ' ' + data_holder.data[
            self.time_source_column]
        data_holder.data['datetime'] = data_holder.data['date_and_time'].apply(self.to_datetime)
        # data_holder.data.drop('date_and_time', axis=1, inplace=True)

    @staticmethod
    def to_datetime(x: str) -> datetime.datetime:
        for form in DATETIME_FORMATS:
            try:
                return datetime.datetime.strptime(x.strip(), form)
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


