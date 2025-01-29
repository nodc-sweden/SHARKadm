from .base import Transformer, DataHolderProtocol
from sharkadm import adm_logger


class SortData(Transformer):
    sort_by_columns = [
        'sample_date',
        'sample_time',
        'station_name',
        
        'sample_min_depth_m',
        'sample_max_depth_m',
        'scientific_name',
        'parameter',
    ]

    def __init__(self, sort_by_columns: list[str] = None, remove_sorting_column: bool = True) -> None:
        super().__init__()
        self._remove_sorting_column = remove_sorting_column
        if sort_by_columns:
            self.sort_by_columns = sort_by_columns

    @staticmethod
    def get_transformer_description() -> str:
        return f'Sorts data by: sample_date -> sample_time -> sample_min_depth_m -> sample_max_depth_m'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        sort_column_name = 'sorting_column'
        data_holder.data[sort_column_name] = ''
        sort_by_columns = [col for col in self.sort_by_columns if col in data_holder.data.columns]
        column_string = ', '.join(sort_by_columns)
        adm_logger.log_transformation(f'Sorting data based on columns: {column_string}')
        for col in sort_by_columns:
            data_holder.data[sort_column_name] = data_holder.data[sort_column_name] + '_' + data_holder.data[
                col].str.zfill(4)
        data_holder.data.sort_values(sort_column_name, inplace=True)
        if self._remove_sorting_column:
            data_holder.data.drop(sort_column_name, axis='columns', inplace=True)
    # def _transform(self, data_holder: DataHolderProtocol) -> None:
    #     sort_by_columns = [col for col in self.sort_by_columns if col in data_holder.data.columns]
    #     column_string = ', '.join(sort_by_columns)
    #     adm_logger.log_transformation(f'Sorting data based on columns: {column_string}')
    #     data_holder.data.sort_values(sort_by_columns, inplace=True)


class SortDataIFCB(SortData):
    sort_by_columns = [
        'image_verified_by',
        'sample_date',
        'sample_time',
        'station_name',

        'sample_min_depth_m',
        'sample_max_depth_m',
        'scientific_name',
        'parameter',
    ]