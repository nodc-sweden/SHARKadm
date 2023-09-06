import datetime

import pandas as pd

from .base import Transformer, DataHolderProtocol
from SHARKadm import adm_logger


class AddPositionToAllLevels(Transformer):
    lat_to_sync = [
        'sample_reported_latitude',
    ]  # In order of prioritization

    lon_to_sync = [
        'sample_reported_longitude',
    ]  # In order of prioritization

    def transform(self, data_holder: DataHolderProtocol) -> None:
        for par in self.dates_to_sync:
            data_holder.data[par] = data_holder.data.apply(lambda row, p=par: self.check_and_add(p, row), axis=1)

    def check_and_add(self, par: str, row: pd.Series) -> str:
        if row.get(par):
            return row[par]
        for date_par in self.dates_to_sync:
            if row.get(date_par):
                adm_logger.log_transformation(f'Added {par} from {date_par}', 'test')
                return row[date_par]
        return ''


class AddDatetime(Transformer):
    datetime_source_column = 'sample_date'

    def transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data['datetime'] = data_holder.data[self.datetime_source_column].apply(self.to_datetime)

    @staticmethod
    def to_datetime(x: str) -> datetime.datetime:
        return datetime.datetime.strptime(x, DATETIME_FORMAT)
