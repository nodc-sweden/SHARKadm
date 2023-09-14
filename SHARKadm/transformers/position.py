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
        for par in self.lat_to_sync:
            data_holder.data[par] = data_holder.data.apply(lambda row, p=par: self.check_lat(p, row), axis=1)
        for par in self.lon_to_sync:
            data_holder.data[par] = data_holder.data.apply(lambda row, p=par: self.check_lon(p, row), axis=1)

    def check_lat(self, par: str, row: pd.Series) -> str:
        if row.get(par):
            return row[par]
        for lat_par in self.lat_to_sync:
            if row.get(lat_par):
                adm_logger.log_transformation(f'Added {par} from {lat_par}', 'test')
                return row[lat_par]
        return ''

    def check_lon(self, par: str, row: pd.Series) -> str:
        if row.get(par):
            return row[par]
        for lon_par in self.lon_to_sync:
            if row.get(lon_par):
                adm_logger.log_transformation(f'Added {par} from {lon_par}')
                return row[lon_par]
        return ''


class AddDatetime(Transformer):
    datetime_source_column = 'sample_date'

    def transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data['datetime'] = data_holder.data[self.datetime_source_column].apply(self.to_datetime)

    @staticmethod
    def to_datetime(x: str) -> datetime.datetime:
        return datetime.datetime.strptime(x, DATETIME_FORMAT)
