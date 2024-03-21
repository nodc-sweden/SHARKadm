from .base import Transformer, DataHolderProtocol
from sharkadm import adm_logger
from sharkadm.utils import matching_strings
import re


class ReplaceCommaWithDot(Transformer):
    apply_on_columns = [
        'sample_reported_latitude',
        'sample_reported_longitude',
        'water_depth_m',
        '.*DIVIDE.*',
        '.*MULTIPLY.*',
        '.*COPY_VARIABLE.*',
        'sampled_volume.*',
        'sampler_area.*'
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
            adm_logger.log_transformation(f'Replacing comma with dot in column', add=col)
            data_holder.data[col] = data_holder.data[col].apply(self.convert)

    def _get_matching_cols(self, data_holder: DataHolderProtocol) -> list[str]:
        return matching_strings.get_matching_strings(strings=data_holder.data.columns, match_strings=self.apply_on_columns)
        # cols = dict()
        # for item in self.apply_on_columns:
        #     if item in data_holder.data.columns:
        #         cols[item] = True
        #         continue
        #     for col in data_holder.data.columns:
        #         if re.match(item, col):
        #             cols[col] = True
        # return list(cols)

    @staticmethod
    def convert(x) -> str:
        return x.replace(',', '.').replace(' ', '')
