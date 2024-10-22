from sharkadm import adm_logger
from .base import Transformer, DataHolderProtocol

try:
    from nodc_station.station_file import get_station_object
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(f'Could not import package "{module_name}" in module {__name__}. You need to install this dependency if you want to use this module.', level=adm_logger.WARNING)


class AddStationInfo(Transformer):
    source_lat_column = 'sample_latitude_dd'
    source_lon_column = 'sample_longitude_dd'
    reported_station_col = 'reported_station_name'
    columns_to_set = ['station_name', 'station_id', 'sample_location_id', 'station_viss_eu_id']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._stations = get_station_object()
        self._station_synonyms = {}
        self._loaded_stations_info = {}

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds station information to all places'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        self._create_columns_if_missing(data_holder)

        for (lat_str, lon_str, reported_station), df in data_holder.data.groupby([self.source_lat_column, self.source_lon_column, self.reported_station_col]):
            if not (lat_str and lon_str):
                rows = ', '.join(list(df['row_number']))
                adm_logger.log_transformation(adm_logger.feedback.missing_position(rows=sorted(set(df['row_number']))),
                                              level=adm_logger.ERROR, purpose=adm_logger.FEEDBACK)  # Ska kanske vara i validator istället
                adm_logger.log_transformation(f'Missing {self.source_lat_column} and/or {self.source_lon_column} in {self.__class__.__name__}', row_number=rows,
                                              level=adm_logger.ERROR)
                continue

            boolean = (data_holder.data[self.source_lat_column] == lat_str) & (data_holder.data[self.source_lon_column] == lon_str) & (data_holder.data[self.reported_station_col] == reported_station)

            lat = float(lat_str)
            lon = float(lon_str)

            info = self._loaded_stations_info.setdefault(reported_station,
                                                         self._stations.get_station_info_from_synonym_and_position(reported_station, lat, lon))
            if not info:
                closest_info = self._stations.get_closest_station_info(lat, lon)
                if len(closest_info) == 1:
                    closest_info = closest_info[0]
                    if closest_info['accepted']:
                        adm_logger.log_transformation(f'Station "{reported_station}" is not found as a synonym in station list'
                                                      f'Closest station is {closest_info["STATION_NAME"]} and is accepted', level=adm_logger.WARNING)
                    else:
                        adm_logger.log_transformation(
                            f'Station "{reported_station}" is not found as a synonym in station list'
                            f'Closest station is {closest_info["STATION_NAME"]} but is not accepted', level=adm_logger.WARNING)
                    continue
                else:
                    station_names_str = ', '.join([info["STATION_NAME"] for info in closest_info])
                    adm_logger.log_transformation(
                            f'Station "{reported_station}" is not found as a synonym in station list. '
                            f'Closest station(s) is/are {station_names_str}', level=adm_logger.WARNING)
                    continue
            if not info['accepted']:
                adm_logger.log_transformation(f'Reported station name found in station list but it is outside the accepted radius. '
                                              f'Distance={info["calc_dist"]}, Accepted radius={info["OUT_OF_BOUNDS_RADIUS"]}', level=adm_logger.WARNING)
                continue

            if reported_station != info['STATION_NAME']:
                name = info['STATION_NAME']
                adm_logger.log_transformation(f'Station name translated: {reported_station} -> {name}', level='warning')
            data_holder.data.loc[boolean, 'station_name'] = info['STATION_NAME']
            data_holder.data.loc[boolean, 'station_id'] = info['REG_ID_GROUP']
            data_holder.data.loc[boolean, 'sample_location_id'] = info['REG_ID']
            data_holder.data.loc[boolean, 'station_viss_eu_id'] = info['EU_CD']

    def _create_columns_if_missing(self, data_holder: DataHolderProtocol) -> None:
        for col in self.columns_to_set:
            if col not in data_holder.data.columns:
                adm_logger.log_transformation(f'Adding column {col}', level=adm_logger.DEBUG)
                data_holder.data[col] = ''



