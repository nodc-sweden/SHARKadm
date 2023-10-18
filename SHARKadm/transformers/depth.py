from .base import Transformer, DataHolderProtocol
from SHARKadm import adm_logger
import pandas as pd


class AddSampleMinAndMaxDepth(Transformer):
    depth_par = 'sample_depth_m'
    min_depth_par = 'sample_min_depth_m'
    max_depth_par = 'sample_max_depth_m'

    @staticmethod
    def get_transformer_description() -> str:
        return 'Adds min and max depth if missing. Depth is set from sample_depth_m'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for par in [self.min_depth_par, self.max_depth_par]:
            data_holder.data[par] = data_holder.data.apply(lambda row, p=par: self.add_if_missing(p, row), axis=1)

    def add_if_missing(self, par: str, row: pd.Series) -> str:
        if row.get(par):
            return row[par]
        if row.get(self.depth_par):
            adm_logger.log_transformation(f'Added {par} from {self.depth_par}')
            return row[self.depth_par]


class AddSectionStartAndEndDepth(Transformer):
    depth_par = 'sample_depth_m'
    min_depth_par = 'sample_min_depth_m'
    max_depth_par = 'sample_max_depth_m'
    section_start_depth_par = 'section_start_depth_m'
    section_end_depth_par = 'section_end_depth_m'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds section start and end depth'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.min_depth_par] = data_holder.data.apply(lambda row: self.add_min_depth(row), axis=1)
        data_holder.data[self.max_depth_par] = data_holder.data.apply(lambda row: self.add_max_depth(row), axis=1)

    def add_min_depth(self, row: pd.Series) -> str:
        if row.get(self.min_depth_par):
            return row[self.min_depth_par]
        if row.get(self.depth_par):
            adm_logger.log_transformation(f'Added {self.min_depth_par} from {self.depth_par}')
            return row[self.depth_par]
        if row.get(self.section_end_depth_par):
            adm_logger.log_transformation(f'Added {self.min_depth_par} from {self.section_end_depth_par}')
            return row[self.section_end_depth_par]

    def add_max_depth(self, row: pd.Series) -> str:
        if row.get(self.max_depth_par):
            return row[self.max_depth_par]
        if row.get(self.depth_par):
            adm_logger.log_transformation(f'Added {self.max_depth_par} from {self.depth_par}')
            return row[self.depth_par]
        if row.get(self.section_start_depth_par):
            adm_logger.log_transformation(f'Added {self.max_depth_par} from {self.section_start_depth_par}')
            return row[self.section_start_depth_par]


class ReorderSampleMinAndMaxDepth(Transformer):
    min_depth_par = 'sample_min_depth_m'
    max_depth_par = 'sample_max_depth_m'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Reorders sample min and max depth if they are in wrong order.'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[[self.min_depth_par, self.max_depth_par]] = \
            data_holder.data.apply(lambda row: self.reorder(row), axis=1, result_type='expand')
        # data_holder.data[par] = data_holder.data.apply(lambda row, p=par: self.reorder(p, row), axis=1)

    def reorder(self, row: pd.Series) -> list[str, str]:
        if row[self.min_depth_par] > row[self.max_depth_par]:
            adm_logger.log_transformation(f'Changed value order of {self.min_depth_par} and {self.max_depth_par}')
            return [row[self.max_depth_par], row[self.min_depth_par]]
        return [row[self.min_depth_par], row[self.max_depth_par]]

