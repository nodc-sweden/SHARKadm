from .base import Transformer, DataHolderProtocol


class SortData(Transformer):
    sort_by_columns = [
        'sample_date',
        'sample_time',
        'station_name',
        'sample_min_depth_m',
        'sample_max_depth_m',
        'scientific_name',
    ]

    def __init__(self, sort_by_columns: list[str] = None) -> None:
        super().__init__()
        if sort_by_columns:
            self.sort_by_columns = sort_by_columns

    @staticmethod
    def get_transformer_description() -> str:
        return f'Sorts data by: sample_date -> sample_time -> sample_min_depth_m -> sample_max_depth_m'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data.sort_values(self.sort_by_columns, inplace=True)
