import datetime

import pandas as pd

from .base import Transformer, DataHolderProtocol
from SHARKadm import adm_logger
from SHARKadm.utils import geography


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
        import time
        t0 = time.time()
        for par in self.lat_to_sync:
            data_holder.data[par] = data_holder.data.apply(lambda row, p=par: self.check_lat(p, row), axis=1)
        for par in self.lon_to_sync:
            data_holder.data[par] = data_holder.data.apply(lambda row, p=par: self.check_lon(p, row), axis=1)
        print(f'{time.time()-t0=}')

    def check_lat(self, par: str, row: pd.Series) -> str:
        if row.get(par):
            return row[par]
        for lat_par in self.lat_to_sync:
            if row.get(lat_par):
                adm_logger.log_transformation(f'Added {par} from {lat_par}', level=adm_logger.INFO)
                return row[lat_par]
        return ''

    def check_lon(self, par: str, row: pd.Series) -> str:
        if row.get(par):
            return row[par]
        for lon_par in self.lon_to_sync:
            if row.get(lon_par):
                adm_logger.log_transformation(f'Added {par} from {lon_par}', level=adm_logger.INFO)
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

        self._cached_pos = dict()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds sample position in decimal minute'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.lat_column_to_set] = data_holder.data[self.lat_source_col].apply(self._get_pos)
        data_holder.data[self.lon_column_to_set] = data_holder.data[self.lon_source_col].apply(self._get_pos)

    def _get_pos(self, pos: str) -> str:
        pos = pos.replace(' ', '')
        parts = pos.split('.')
        if len(parts[0]) == 2:
            pos = self._cached_pos.setdefault(pos, geography.decdeg_to_decmin(pos, nr_decimals=None))
        pos = f'{pos[:2]} {pos[2:]}'
        return pos[:8]


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

        self._cached_pos = dict()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds sample position in decimal degree'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.lat_column_to_set] = data_holder.data[self.lat_source_col].apply(self._get_pos)
        data_holder.data[self.lon_column_to_set] = data_holder.data[self.lon_source_col].apply(self._get_pos)

    def _get_pos(self, pos: str) -> str:
        pos = pos.replace(' ', '').lstrip('0')
        parts = pos.split('.')
        if len(parts[0]) == 4:
            pos = self._cached_pos.setdefault(pos, geography.decmin_to_decdeg(pos, nr_decimals=None))
        return pos[:8]


class AddPositionSweref99tm(Transformer):
    lat_source_col = 'sample_latitude_dd'
    lon_source_col = 'sample_longitude_dd'
    y_column_to_set = 'sample_sweref99tm_y'
    x_column_to_set = 'sample_sweref99tm_x'

    _cached_pos = {}

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds crs position in sweref99tm'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.x_column_to_set] = ''
        data_holder.data[self.y_column_to_set] = ''
        # data_holder.data[self.x_column_to_set], data_holder.data[self.y_column_to_set] = \
        for (lat, lon), df in data_holder.data.groupby([self.lat_source_col, self.lon_source_col]):
            x, y = geography.decdeg_to_sweref99tm(lat=lat, lon=lon)
            boolean = (data_holder.data[self.lat_source_col] == lat) & (data_holder.data[self.lon_source_col] == lon)
            data_holder.data.loc[boolean, self.x_column_to_set] = x
            data_holder.data.loc[boolean, self.y_column_to_set] = y


    #     data_holder.data = data_holder.data.apply(self._get_pos, axis=1)
    #
    # def _get_pos(self, row: dict) -> dict:
    #     lat, lon = row[self.lat_source_col], row[self.lon_source_col]
    #     if not (lat and lon):
    #         adm_logger.log_transformation(
    #             f'Missing {self.lat_source_col} and/or {self.lon_source_col} in {self.__class__.__name__}',
    #             add=row.get('row_number', None),
    #             level=adm_logger.WARNING)
    #         return row
    #     row[self.x_column_to_set], row[self.y_column_to_set] = \
    #         self._cached_pos.setdefault((lat, lon), geography.decdeg_to_sweref99tm(lat=lat, lon=lon))
    #     return row

    # def _transform(self, data_holder: DataHolderProtocol) -> None:
    #     # data_holder.data[self.x_column_to_set], data_holder.data[self.y_column_to_set] = \
    #     data_holder.data = data_holder.data.apply(self._get_pos, axis=1)
    #
    # def _get_pos(self, row: dict) -> dict:
    #     lat, lon = row[self.lat_source_col], row[self.lon_source_col]
    #     if not (lat and lon):
    #         adm_logger.log_transformation(f'Missing {self.lat_source_col} and/or {self.lon_source_col} in {self.__class__.__name__}',
    #                                       add=row.get('row_number', None),
    #                                       level=adm_logger.WARNING)
    #         return row
    #     row[self.x_column_to_set], row[self.y_column_to_set] = \
    #         self._cached_pos.setdefault((lat, lon), geography.decdeg_to_sweref99tm(lat=lat, lon=lon))
    #     return row

