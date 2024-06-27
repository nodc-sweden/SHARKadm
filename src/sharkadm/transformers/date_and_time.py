import datetime

import pandas as pd

from .base import Transformer, DataHolderProtocol
from sharkadm import adm_logger

DATETIME_FORMATS = [
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%d %H:%M',
    '%Y-%m-%d'
]


class FixTimeFormat(Transformer):
    time_cols = [
        'sample_time',
        'visit_time',
        'sample_endtime'
    ]

    @staticmethod
    def get_transformer_description() -> str:
        cols_str = ', '.join(FixTimeFormat.time_cols)
        return f'Reformat time values in columns: {cols_str}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        self._buffer = {}
        for col in self.time_cols:
            if col not in data_holder.data:
                continue
            self._current_col = col
            data_holder.data[col] = data_holder.data[col].apply(self._fix)

    def _fix(self, x: str):
        xx = x.strip()
        if not xx:
            return ''
        fixed_x = self._buffer.get(xx)
        if fixed_x:
            return fixed_x
        xx = xx.replace('.', ':')
        if ':' in xx:
            if len(xx) > 5:
                adm_logger.log_transformation(f'Cant handle time format in column {self._current_col}', add=x,
                                              level=adm_logger.ERROR)
                return ''
            xx = xx.zfill(5)
        else:
            if len(xx) > 4:
                adm_logger.log_transformation(f'Cant handle time format in column {self._current_col}', add=x,
                                              level=adm_logger.ERROR)
                return ''
            xx = xx.zfill(4)
            xx = f'{xx[:2]}:{xx[2:]}'.zfill(5)
        self._buffer[x.strip()] = xx
        return xx


class FixDateFormat(Transformer):
    dates_to_check = [
        'sample_date',
        'visit_date'
    ]

    from_format = '%Y%m%d'
    to_format = '%Y-%m-%d'

    mapping = {}

    @staticmethod
    def get_transformer_description() -> str:
        return f'Changes date format from {FixDateFormat.from_format} to {FixDateFormat.to_format}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for par in self.dates_to_check:
            if par not in data_holder.data:
                continue
            data_holder.data[par] = data_holder.data[par].apply(self._set_new_format)

    def _set_new_format(self, x: str):
        try:
            return self.mapping.setdefault(x, datetime.datetime.strptime(x, self.from_format).strftime(self.to_format))
        except ValueError:
            return x


class AddSampleTime(Transformer):

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adding time format sample_time'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if 'sample_time' in data_holder.data:
            data_holder.data['reported_sample_time'] = data_holder.data['sample_time']
            if 'visit_time' in data_holder.data:
                has_no_sample_time = data_holder.data['sample_time'].str.strip() == ''
                data_holder.data.loc[has_no_sample_time, 'sample_time'] = data_holder.data.loc[has_no_sample_time, 'visit_time']
        else:
            data_holder.data['sample_time'] = data_holder.data['visit_time']


class AddSampleDate(Transformer):

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adding time format sample_date'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if 'sample_date' in data_holder.data:
            data_holder.data['reported_sample_date'] = data_holder.data['sample_date']
            if 'visit_date' in data_holder.data:
                has_no_sample_date = data_holder.data['sample_date'].str.strip() == ''
                data_holder.data.loc[has_no_sample_date, 'sample_date'] = data_holder.data.loc[has_no_sample_date, 'visit_date']
        else:
            data_holder.data['sample_date'] = data_holder.data['visit_date']


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
    def to_datetime(x: str) -> datetime.datetime | str:
        for form in DATETIME_FORMATS:
            try:
                return datetime.datetime.strptime(x.strip(), form)
            except ValueError:
                continue
        return ''


class AddMonth(Transformer):
    month_columns = [
        'sample_month',
        'visit_month'
    ]

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds month column to data. Month is taken from the datetime column and will overwrite old value'

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


