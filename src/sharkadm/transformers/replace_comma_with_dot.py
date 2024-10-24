from .base import Transformer, DataHolderProtocol
from sharkadm import adm_logger
from sharkadm.utils import matching_strings
import re


class ReplaceCommaWithDot(Transformer):
    apply_on_columns = [
        '.*latitude.*',
        '.*longitude.*',
        'water_depth_m',
        '.*DIVIDE.*',
        '.*MULTIPLY.*',
        '.*COPY_VARIABLE.*',
        'sampled_volume.*',
        'sampler_area.*',
        '.*wind.*',
        '.*pressure.*',
        '.*temperature.*',
    ]

    def __init__(self, apply_on_columns: list[str] = None) -> None:
        super().__init__()
        if apply_on_columns:
            self.apply_on_columns = apply_on_columns

        self._handled_cols = dict()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds position to all levels if not present'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for col in self._get_matching_cols(data_holder):
            for item, df in data_holder.data.groupby(col):
                item = str(item)
                if ',' not in item:
                    continue
                new_item = item.replace(',', '.')
                adm_logger.log_transformation(f'Replacing comma with dot for value {item} in column {col}', level=adm_logger.INFO)
                data_holder.data.loc[df.index, col] = new_item

    def _get_matching_cols(self, data_holder: DataHolderProtocol) -> list[str]:
        return matching_strings.get_matching_strings(strings=data_holder.data.columns, match_strings=self.apply_on_columns)

