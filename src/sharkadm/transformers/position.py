import pandas as pd

from sharkadm import adm_logger
from sharkadm.utils import geography
from .base import Transformer, DataHolderProtocol


class AddSamplePosition(Transformer):
    lat_info = dict(
        nr_sweref_digits=7,
        columns = [
        'sample_reported_latitude',
        'visit_reported_latitude'
    ]  # In order of prioritization
    )

    lon_info = dict(
        nr_sweref_digits=6,
        columns=[
            'sample_reported_longitude',
            'visit_reported_longitude'
        ]  # In order of prioritization
    )

    lat_col_to_set = 'sample_latitude_dd'
    lon_col_to_set = 'sample_longitude_dd'

    _cached_pos = {}

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds sample position based on reported position'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[[self.lat_col_to_set, self.lon_col_to_set]] = data_holder.data.apply(lambda row: self._get(row), axis=1, result_type='expand')

    def _get(self, row: pd.Series) -> (str, str):
        lat_col = ''
        lon_col = ''
        for lat_col in self.lat_info['columns']:
            if lat_col in row and row[lat_col].strip():
                break
        for lon_col in self.lon_info['columns']:
            if lon_col in row and row[lon_col].strip():
                break

        if not all([lat_col, lon_col]):
            return '', ''

        lat_value = row[lat_col].strip().replace(' ', '')  # .replace(',', '.')
        lon_value = row[lon_col].strip().replace(' ', '')  # .replace(',', '.')

        if not all([lat_value, lon_value]):
            return '', ''

        if self._is_sweref99tm(value=lat_value, info=self.lat_info) and self._is_sweref99tm(value=lon_value, info=self.lon_info):
            return geography.sweref99tm_to_decdeg(lon_value, lat_value)
        elif self._is_dm_lat(lat_value) and self._is_dm_lon(lon_value):
            return geography.decmin_to_decdeg(lat_value), geography.decmin_to_decdeg(lon_value)
        elif self._is_dd(lat_value) and self._is_dd(lon_value):
            return lat_value, lon_value

    def _is_sweref99tm(self, value: str, info: dict) -> bool:
        if len(value.split('.')[0]) == info['nr_sweref_digits']:
            return True
        return False

    def _is_dm_lat(self, value: str) -> bool:
        parts = value.split('.')
        if len(parts[0].zfill(4)) == 4:
            return True
        return False

    def _is_dm_lon(self, value: str) -> bool:
        parts = value.split('.')
        if len(parts[0].zfill(5)) == 5:
            return True
        return False

    def _is_dd(self, value: str) -> bool:
        parts = value.split('.')
        if len(parts[0].zfill(2)) == 2:
            return True
        return False


class AddSamplePositionSweref99tm(Transformer):
    lat_source_col = 'sample_latitude_dd'
    lon_source_col = 'sample_longitude_dd'
    y_column_to_set = 'sample_sweref99tm_y'
    x_column_to_set = 'sample_sweref99tm_x'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds sample position in sweref99tm'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.x_column_to_set] = ''
        data_holder.data[self.y_column_to_set] = ''
        # data_holder.data[self.x_column_to_set], data_holder.data[self.y_column_to_set] = \
        for (lat, lon), df in data_holder.data.groupby([self.lat_source_col, self.lon_source_col]):
            if not all([lat, lon]):
                adm_logger.log_transformation(f'Missing position when trying to set {self.x_column_to_set} and {self.y_column_to_set}')
                continue
            x, y = geography.decdeg_to_sweref99tm(lat=lat, lon=lon)
            data_holder.data.loc[df.index, self.x_column_to_set] = x
            data_holder.data.loc[df.index, self.y_column_to_set] = y
            # boolean = (data_holder.data[self.lat_source_col] == lat) & (data_holder.data[self.lon_source_col] == lon)
            # data_holder.data.loc[boolean, self.x_column_to_set] = x
            # data_holder.data.loc[boolean, self.y_column_to_set] = y


class AddSamplePositionDM(Transformer):
    lat_source_col = 'sample_latitude_dd'
    lon_source_col = 'sample_longitude_dd'
    lat_column_to_set = 'sample_latitude_dm'
    lon_column_to_set = 'sample_longitude_dm'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds sample position in decimal minute'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.lat_column_to_set] = ''
        data_holder.data[self.lon_column_to_set] = ''
        # data_holder.data[self.x_column_to_set], data_holder.data[self.y_column_to_set] = \
        for (lat, lon), df in data_holder.data.groupby([self.lat_source_col, self.lon_source_col]):
            if not all([lat, lon]):
                adm_logger.log_transformation(
                    f'Missing position when trying to set {self.lat_column_to_set} and {self.lon_column_to_set}')
                continue
            lat = geography.decdeg_to_decmin(lat)
            lon = geography.decdeg_to_decmin(lon)
            data_holder.data.loc[df.index, self.lat_column_to_set] = lat
            data_holder.data.loc[df.index, self.lon_column_to_set] = lon


