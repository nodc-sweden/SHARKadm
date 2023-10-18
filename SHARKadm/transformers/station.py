from .base import Transformer, DataHolderProtocol
from micro.stations import get_default_station_object
from SHARKadm import adm_logger


class AddStationInfo(Transformer):
    columns_to_set = ['station_name', 'station_id', 'sample_location_id']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._stations = get_default_station_object()
        self._loaded_stations_info = {}

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds station information to all places'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        self._create_columns_if_missing(data_holder)

        for i in data_holder.data.index:
            reported_station = data_holder.data.at[i, 'reported_station_name']
            info = self._loaded_stations_info.setdefault(reported_station, self._stations.get_station_info(
                reported_station))
            data_holder.data.at[i, 'station_name'] = info['station_name']
            data_holder.data.at[i, 'station_id'] = info['reg_id']
            data_holder.data.at[i, 'sample_location_id'] = info['reg_id_group']

        # data_holder.data['station_name'], data_holder.data['station_name'], data_holder.data['station_name'] = \
        #     data_holder.data.map(self._translate)

    def _create_columns_if_missing(self, data_holder: DataHolderProtocol) -> None:
        for col in self.columns_to_set:
            if col not in data_holder.data.columns:
                adm_logger.log_transformation(f'Adding column {col}', level='info')
                data_holder.data[col] = ''

        # data_holder.data['station_name'] = data_holder.data.apply(lambda x: self._stations.get_station_name(
        #     data_holder.data['reported_station_name']))
        #
        # data_holder.data['sample_location_id'] = data_holder.data.apply(lambda x: self._stations.get_translation(
        #     data_holder.data['reported_station_name'], 'REG_ID'))
        #
        # data_holder.data['station_id'] = data_holder.data.apply(lambda x: self._stations.get_translation(
        #     data_holder.data['reported_station_name'], 'REG_ID_GROUP'))


