import re

from .base import Transformer, DataHolderProtocol
from sharkadm import adm_logger


class RemoveValuesInColumns(Transformer):

    def __init__(self, *columns: str, replace_value: int | float | str = '') -> None:
        super().__init__()
        self.apply_on_columns = columns
        if isinstance(columns[0], list):
            self.apply_on_columns = columns[0]

        self._replace_value = str(replace_value)

    @staticmethod
    def get_transformer_description() -> str:
        return f'Removes all values in given columns. Option to set replace_value.'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        len_data = len(data_holder.data)
        # for col in self.apply_on_columns:
        for col in self._get_apply_on_columns(data_holder):
            if col not in data_holder.data:
                continue
            data_holder.data[col] = self._replace_value
            if self._replace_value:
                adm_logger.log_transformation(
                    f'All values in column {col} are set to {self._replace_value} (all {len_data} places)',
                    level=adm_logger.WARNING)
            else:
                adm_logger.log_transformation(
                    f'All values in column {col} are removed (all {len_data} places)',
                    level=adm_logger.WARNING)

    def _get_apply_on_columns(self, data_holder: DataHolderProtocol):
        columns = []
        for col in data_holder.data.columns:
            for arg in self.apply_on_columns:
                if re.match(arg, col):
                    columns.append(col)
                    break
        return columns


class RemoveRowsForParameters(Transformer):
    valid_data_structures = ['row']

    parameter_column = 'parameter'

    def __init__(self, *parameters: str) -> None:
        super().__init__()
        self.apply_on_parameters = parameters
        if isinstance(parameters[0], list):
            self.apply_on_parameters = parameters[0]

    @staticmethod
    def get_transformer_description() -> str:
        return f'Removes entire row for given parameters'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if 'parameter' not in data_holder.data:
            adm_logger.log_transformation(f'Can not remove rows in data. Missing column "{self.parameter_column}"', level=adm_logger.WARNING)
            return

        for par in self.apply_on_parameters:
            boolean = data_holder.data[self.parameter_column].str.strip().str.match(par.strip(), case=False)
            index = data_holder.data.loc[boolean].index
            if not len(index):
                continue
            data_holder.data.drop(index=index, inplace=True)
            adm_logger.log_transformation(f'Removing parameter "{par}" ({len(index)} rows)',
                                          level=adm_logger.WARNING)


class RemoveDeepestDepthAtEachVisit(Transformer):
    valid_data_holders = ['ZipArchiveDataHolder']
    valid_data_types = []

    visit_id_columns = ['sample_date', 'sample_time', 'sample_latitude_dd', 'sample_longitude_dd', 'platform_code', 'visit_id']

    def __init__(self,
                 valid_data_types: list[str],
                 depth_column: str,
                 also_remove_from_columns: list[str] = None,
                 replace_value: int | float | str = '',
                 keep_single_depth_at_surface: bool = False,
                 ) -> None:
        super().__init__()
        self.valid_data_types = valid_data_types
        self._depth_column = depth_column
        self._also_remove_from_columns = also_remove_from_columns or []
        self._replace_value = str(replace_value)
        self._keep_single_depth_at_surface = keep_single_depth_at_surface

    @staticmethod
    def get_transformer_description() -> str:
        return f'Removes deepest depth at each visit in given datatype'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self._depth_column not in data_holder.data:
            adm_logger.log_transformation(f'Depth column {self._depth_column} not found in data',
                                          level=adm_logger.DEBUG)
            return
        all_index_to_set = []
        nr_visits = 0
        id_cols = [col for col in self.visit_id_columns if col in data_holder.data]
        for _id, df in data_holder.data.groupby(id_cols):
            df = df.loc[df[self._depth_column] != '']
            depths = set(df[self._depth_column])
            if not len(depths):
                adm_logger.log_transformation(f'No depths in column {self._depth_column} at {_id}',
                                              level=adm_logger.WARNING)
                continue
            if len(depths) == 1 and float(list(depths)[0]) <= 2 and self._keep_single_depth_at_surface:
                continue
            max_depth = max(depths, key=lambda x: float(x))
            max_depth_index = df.loc[df[self._depth_column] == max_depth].index
            all_index_to_set.extend(list(max_depth_index))
            nr_visits += 1

        additional_cols = [col for col in self._also_remove_from_columns if col in data_holder.data]
        data_holder.data.loc[all_index_to_set, additional_cols] = ''
        data_holder.data.loc[all_index_to_set, self._depth_column] = self._replace_value

        for col in additional_cols:
            adm_logger.log_transformation(f'Removing deepest {col} info at {nr_visits} visits',
                                          level=adm_logger.WARNING)

        if self._replace_value:
            adm_logger.log_transformation(f'Setting deepest depth to {self._replace_value} at {nr_visits} visits',
                                          level=adm_logger.WARNING)
        else:
            adm_logger.log_transformation(f'Removing deepest depth info at {nr_visits} visits',
                                          level=adm_logger.WARNING)


class RemoveInterval(Transformer):
    valid_data_holders = ['ZipArchiveDataHolder']
    valid_data_types = []

    min_col = 'sample_min_depth_m'
    max_col = 'sample_max_depth_m'

    def __init__(self,
                 valid_data_types: list[str],
                 keep_intervals: list[str] = None,
                 keep_if_min_depths_are: list[str] = None,
                 replace_value: int | float | str = '',
                 also_replace_in_columns: list[str] = None,
                 also_remove_from_columns: list[str] = None,
                 ) -> None:
        super().__init__()
        self.valid_data_types = valid_data_types
        self._keep_intervals = keep_intervals or []
        self._keep_if_min_depths_are = keep_if_min_depths_are or []
        self._replace_value = str(replace_value)
        self._also_replace_in_columns = also_replace_in_columns or []
        self._also_remove_from_columns = also_remove_from_columns or []

    @staticmethod
    def get_transformer_description() -> str:
        return f'Removes all intervals [{RemoveInterval.min_col}-{RemoveInterval.max_col}]. Option to set replace_value and keep_intervals'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for (mi, ma), df in data_holder.data.groupby([self.min_col, self.max_col]):
            inter = f'{mi}-{ma}'
            if inter in self._keep_intervals:
                continue
            msg = f'Removing interval {inter} from columns {self.min_col} and {self.max_col} ({len(df)} places)'
            if self._replace_value:
                msg = f'Replacing interval {inter} with {self._replace_value}-{self._replace_value} in columns {self.min_col} and {self.max_col} ({len(df)} places)'
            adm_logger.log_transformation(msg, level=adm_logger.WARNING)
            if self._keep_if_min_depths_are and mi in self._keep_if_min_depths_are:
                pass
            else:
                data_holder.data.loc[df.index, self.min_col] = self._replace_value
            data_holder.data.loc[df.index, self.max_col] = self._replace_value
            for col in self._also_replace_in_columns:
                if col not in data_holder.data:
                    continue
                msg = f'Removing values in column {col} at interval {inter} ({len(df)} places)'
                if self._replace_value:
                    msg = f'Replacing values in column {col} with {self._replace_value} at interval {inter} ({len(df)} places)'
                adm_logger.log_transformation(msg, level=adm_logger.WARNING)
                data_holder.data.loc[df.index, col] = self._replace_value
            for col in self._also_remove_from_columns:
                if col not in data_holder.data:
                    continue
                msg = f'Removing values in column {col} at interval {inter} ({len(df)} places)'
                adm_logger.log_transformation(msg, level=adm_logger.WARNING)
                data_holder.data.loc[df.index, col] = ''


# class RemoveDeepestDepthAtEachVisitPhysicalChemical(Transformer):
#     valid_data_holders = ['ZipArchiveDataHolder']
#     valid_data_types = ['PhysicalChemical']
#
#     visit_id_columns = ['sample_date', 'sample_time', 'sample_latitude_dd', 'sample_longitude_dd', 'platform_code', 'visit_id']
#     depth_column = 'sample_depth_m'
#
#     additional_cols_to_set = ['sample_id', 'shark_sample_id']
#
#     def __init__(self, replace_value: int | float | str = '') -> None:
#         super().__init__()
#         self._replace_value = str(replace_value)
#
#     @staticmethod
#     def get_transformer_description() -> str:
#         return f'Removes deepest depth at each visit in datatype PhysicalChemical'
#
#     def _transform(self, data_holder: DataHolderProtocol) -> None:
#         all_index_to_set = []
#         nr_visits = 0
#         id_cols = [col for col in self.visit_id_columns if col in data_holder.data]
#         for _id, df in data_holder.data.groupby(id_cols):
#             max_depth = max(set(df[self.depth_column]), key=lambda x: float(x))
#             max_depth_index = df.loc[df[self.depth_column] == max_depth].index
#             all_index_to_set.extend(list(max_depth_index))
#             nr_visits += 1
#
#         additional_cols = [col for col in self.additional_cols_to_set if col in data_holder.data]
#         data_holder.data.loc[all_index_to_set, additional_cols] = ''
#         data_holder.data.loc[all_index_to_set, self.depth_column] = self._replace_value
#
#         for col in additional_cols:
#             adm_logger.log_transformation(f'Removing deepest {col} info at {nr_visits} visits',
#                                           level=adm_logger.WARNING)
#
#         if self._replace_value:
#             adm_logger.log_transformation(f'Setting deepest depth to {self._replace_value} at {nr_visits} visits',
#                                           level=adm_logger.WARNING)
#         else:
#             adm_logger.log_transformation(f'Removing deepest depth info at {nr_visits} visits',
#                                           level=adm_logger.WARNING)
#
#
# class RemoveDeepestDepthAtEachVisitBacterioplankton(Transformer):
#     valid_data_holders = ['ZipArchiveDataHolder']
#     valid_data_types = ['Bacterioplankton']
#
#     visit_id_columns = ['sample_date', 'sample_time', 'sample_latitude_dd', 'sample_longitude_dd', 'platform_code', 'visit_id']
#     depth_columns = ['sample_min_depth_m', 'sample_max_depth_m']
#
#     additional_cols_to_set = ['sample_id', 'shark_sample_id']
#
#     def __init__(self, replace_value: int | float | str = '') -> None:
#         super().__init__()
#         self._replace_value = str(replace_value)
#
#     @staticmethod
#     def get_transformer_description() -> str:
#         return f'Removes deepest depth at each visit in datatype Bacterioplankton'
#
#     def _transform(self, data_holder: DataHolderProtocol) -> None:
#         all_index_to_set = {}
#         nr_visits = 0
#         id_cols = [col for col in self.visit_id_columns if col in data_holder.data]
#         for _id, df in data_holder.data.groupby(id_cols):
#             for column in self.depth_columns:
#                 all_index_to_set[column] = []
#                 max_depth = max(set(df[column]), key=lambda x: float(x))
#                 max_depth_index = df.loc[df[column] == max_depth].index
#                 all_index_to_set[column].extend(list(max_depth_index))
#                 nr_visits += 1
#
#         for column in self.depth_columns:
#             additional_cols = [col for col in self.additional_cols_to_set if col in data_holder.data]
#             data_holder.data.loc[all_index_to_set[column], additional_cols] = ''
#             data_holder.data.loc[all_index_to_set[column], column] = self._replace_value
#
#             for col in additional_cols:
#                 adm_logger.log_transformation(f'Removing deepest {col} info at {nr_visits} visits',
#                                               level=adm_logger.WARNING)
#
#             if self._replace_value:
#                 adm_logger.log_transformation(f'Setting deepest depth to {self._replace_value} at {nr_visits} visits',
#                                               level=adm_logger.WARNING)
#             else:
#                 adm_logger.log_transformation(f'Removing deepest depth info at {nr_visits} visits',
#                                               level=adm_logger.WARNING)

class SetMaxLengthOfValuesInColumns(Transformer):

    def __init__(self, *columns: str, length: int) -> None:
        super().__init__()
        self.apply_on_columns = columns
        if isinstance(columns[0], list):
            self.apply_on_columns = columns[0]

        self._length = length

    @staticmethod
    def get_transformer_description() -> str:
        return f'Set max length och all values in given columns.'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        len_data = len(data_holder.data)
        for col in self.apply_on_columns:
            if col not in data_holder.data:
                continue
            data_holder.data[col] = data_holder.data[col].str[:self._length]
            adm_logger.log_transformation(
                f'Setting all values in column {col} to length {self._length} (all {len_data} places)',
                level=adm_logger.INFO)
