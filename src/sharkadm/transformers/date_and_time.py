import datetime

import pandas as pd

from .base import Transformer, DataHolderProtocol
from sharkadm import adm_logger

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
    end_date_col = 'sample_enddate'

    @staticmethod
    def get_transformer_description() -> str:
        return 'Adds date and time column to all levels'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for par in self.dates_to_sync:
            data_holder.data[par] = data_holder.data.apply(lambda row, p=par: self._check_and_add(p, row), axis=1)
        self._split_date_and_time(data_holder=data_holder)
        self._fix_end_date(data_holder=data_holder)

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
            time_par = self._get_time_par(date_par)
            # data_holder.data[time_par] = data_holder.data[date_par].apply(self._get_time_from_date)
            data_holder.data[time_par] = data_holder.data.apply(lambda row, p=date_par: self._get_time_from_date(p, row), axis=1)
            data_holder.data[date_par] = data_holder.data[date_par].apply(self._get_date_from_date)

    # def _get_time_from_date(self, x: str) -> str:
    def _get_time_from_date(self, date_par: str, row: pd.Series) -> str:
        time_par = self._get_time_par(date_par)
        if row.get(time_par):
            return row[time_par]
        parts = row[date_par].strip().split()
        if len(parts) == 2:
            adm_logger.log_transformation('Setting time string from date string', level=adm_logger.DEBUG)
            return parts[1]
        return ''

    def _get_date_from_date(self, x: str) -> str:
        if ' ' not in x:
            return x
        return x.strip().split()[0]

    def _get_time_par(self, date_par: str) -> str:
        return date_par.replace('date', 'time')

    def _fix_end_date(self, data_holder: DataHolderProtocol):
        data_holder.data[self.end_date_col] = data_holder.data[self.end_date_col].apply(lambda x: x.split()[0])



class ChangeDateFormat(Transformer):
    dates_to_check = [
        'sample_date',
        'visit_date'
    ]
    from_format = '%Y%m%d'
    to_format = '%Y-%m-%d'

    mapping = {}

    @staticmethod
    def get_transformer_description() -> str:
        return f'Changes date format from {ChangeDateFormat.from_format} to {ChangeDateFormat.to_format}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for par in self.dates_to_check:
            data_holder.data[par] = data_holder.data[par].apply(self._set_new_format)

    def _set_new_format(self, x: str):
        try:
            return self.mapping.setdefault(x, datetime.datetime.strptime(x, self.from_format).strftime(self.to_format))
        except ValueError:
            return x


class AddDatetime(Transformer):

    def __init__(self,
                 date_source_column: str = 'sample_date',
                 time_source_column: str = 'sample_time',
                 **kwargs):
        super().__init__(**kwargs)
        self.date_source_column = date_source_column
        self.time_source_column = time_source_column

    @staticmethod
    def get_transformer_description() -> str:
        return 'Adds column datetime. Time is taken from sample_date'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data['date_and_time'] = data_holder.data[self.date_source_column]
        if self.time_source_column in data_holder.data.columns:
            data_holder.data['date_and_time'] = data_holder.data['date_and_time'].str[:10] + ' ' + data_holder.data[
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


