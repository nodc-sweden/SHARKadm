import datetime

import pandas as pd

from .base import Transformer, DataHolderProtocol
from SHARKadm import adm_logger


class AddPositionToAllLevels(Transformer):
    lat_to_sync = [
        'sample_reported_latitude',
        'visit_reported_latitude'
    ]  # In order of prioritization

    lon_to_sync = [
        'sample_reported_longitude',
        'visit_reported_longitude'
    ]  # In order of prioritization

    @property
    def transformer_description(self) -> str:
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

