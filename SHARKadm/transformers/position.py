import datetime

import pandas as pd

from .base import Transformer, DataHolderProtocol
from SHARKadm import adm_logger
from SHARKadm.utils import position


class AddPositionToAllLevels(Transformer):
    lat_to_sync = [
        'sample_reported_latitude',
        'visit_reported_latitude'
    ]  # In order of prioritization

    lon_to_sync = [
        'sample_reported_longitude',
        'visit_reported_longitude'
    ]  # In order of prioritization

    def __init__(self, lat_to_sync: list[str] | None = None, lon_to_sync: list[str] | None = None, **kwargs):
        super().__init__(**kwargs)
        if lat_to_sync:
            self.lat_to_sync = lat_to_sync
        if lon_to_sync:
            self.lon_to_sync = lon_to_sync

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds position to all levels if not present'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for par in self.lat_to_sync:
            data_holder.data[par] = data_holder.data.apply(lambda row, p=par: self.check_lat(p, row), axis=1)
        for par in self.lon_to_sync:
            data_holder.data[par] = data_holder.data.apply(lambda row, p=par: self.check_lon(p, row), axis=1)

    def check_lat(self, par: str, row: pd.Series) -> str:
        if row.get(par):
            return row[par]
        for lat_par in self.lat_to_sync:
            if row.get(lat_par):
                adm_logger.log_transformation(f'Added {par} from {lat_par}', level='info')
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


class AddPositionDM(Transformer):
    lat_source_col = 'sample_reported_latitude'
    lon_source_col = 'sample_reported_longitude'
    lat_column_to_set = 'sample_latitude_dm'
    lon_column_to_set = 'sample_longitude_dm'

    def __init__(self,
                 lat_source_col: str | None = None,
                 lon_source_col: str | None = None,
                 lat_column_to_set: str | None = None,
                 lon_column_to_set: str | None = None,
                 **kwargs):
        super().__init__(**kwargs)
        if lat_source_col:
            self.lat_source_col = lat_source_col
        if lon_source_col:
            self.lon_source_col = lon_source_col
        if lat_column_to_set:
            self.lat_column_to_set = lat_column_to_set
        if lon_column_to_set:
            self.lon_column_to_set = lon_column_to_set

        self._cached_lat = dict()
        self._cached_lon = dict()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds sample position in decimal minute'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.lat_column_to_set] = data_holder.data.apply(lambda row: self._get_lat(row), axis=1)
        data_holder.data[self.lon_column_to_set] = data_holder.data.apply(lambda row: self._get_lon(row), axis=1)

    def _get_lat(self, row):
        lat = row[self.lat_source_col]
        return self._cached_lat.setdefault(lat, position.decdeg_to_decmin(lat))

    def _get_lon(self, row):
        lon = row[self.lon_source_col]
        return self._cached_lon.setdefault(lon, position.decdeg_to_decmin(lon))


class AddPositionDD(Transformer):
    lat_source_col = 'sample_reported_latitude'
    lon_source_col = 'sample_reported_longitude'
    lat_column_to_set = 'sample_latitude_dd'
    lon_column_to_set = 'sample_longitude_dd'

    def __init__(self,
                 lat_source_col: str | None = None,
                 lon_source_col: str | None = None,
                 lat_column_to_set: str | None = None,
                 lon_column_to_set: str | None = None,
                 **kwargs):
        super().__init__(**kwargs)
        if lat_source_col:
            self.lat_source_col = lat_source_col
        if lon_source_col:
            self.lon_source_col = lon_source_col
        if lat_column_to_set:
            self.lat_column_to_set = lat_column_to_set
        if lon_column_to_set:
            self.lon_column_to_set = lon_column_to_set

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds sample position in decimal degree'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.lat_column_to_set] = data_holder.data[self.lat_source_col].apply(self._split_pos)
        data_holder.data[self.lon_column_to_set] = data_holder.data[self.lon_source_col].apply(self._split_pos)

    @staticmethod
    def _split_pos(x):
        x = x.replace(' ', '')
        return f'{x[:2]} {x[2:]}'
