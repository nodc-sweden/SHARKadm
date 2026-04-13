import re

import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.data.zip_archive import PolarsZipArchiveDataHolder
from sharkadm.data_filter.base import PolarsDataFilter
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.utils import modify

from .base import PolarsTransformer

# class RemoveReportedValueIfNotCalculated(Transformer):
#     col_to_set = "reported_value"
#     check_col = "calc_by_dc"
#     check_val = "Y"
#
#     @staticmethod
#     def get_transformer_description() -> str:
#         return (
#             f"Remove values in {RemoveReportedValueIfNotCalculated.col_to_set} column "
#             f"if {RemoveReportedValueIfNotCalculated.check_col} "
#             f"is not {RemoveReportedValueIfNotCalculated.check_val}"
#         )
#
#     def _transform(self, data_holder: DataHolderProtocol) -> None:
#         boolean = data_holder.data[self.col_to_set] != self.check_val
#         data_holder.data.loc[boolean, self.col_to_set] = ""


class PolarsKeepMask(PolarsTransformer):
    def __init__(
        self,
        data_filter: PolarsDataFilter,
        valid_data_types: tuple[str, ...] = (),
        **kwargs,
    ) -> None:
        self.valid_data_types = valid_data_types or self.valid_data_types
        super().__init__(data_filter=data_filter, **kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Keeps all rows that are valid in filter"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        mask = self._get_filter_mask(data_holder)
        if mask.is_empty():
            adm_logger.log_transformation(
                f"Could not run transformer {PolarsKeepMask.__class__.__name__}. "
                f"Missing data_filter",
                level=adm_logger.ERROR,
            )
            return
        data_holder.data = data_holder.data.remove(~mask)


class PolarsRemoveMask(PolarsTransformer):
    def __init__(
        self,
        data_filter: PolarsDataFilter,
        valid_data_types: tuple[str, ...] = (),
        **kwargs,
    ) -> None:
        self.valid_data_types = valid_data_types or self.valid_data_types
        super().__init__(data_filter=data_filter, **kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Removes all rows that are valid in filter"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        mask = self._get_filter_mask(data_holder)
        if mask.is_empty():
            adm_logger.log_transformation(
                f"Could not run transformer {PolarsRemoveMask.__class__.__name__}. "
                f"Missing data_filter",
                level=adm_logger.ERROR,
            )
        data_holder.data = data_holder.data.remove(mask)


class PolarsReplaceColumnWithMask(PolarsTransformer):
    def __init__(
        self,
        data_filter: PolarsDataFilter,
        column: str,
        replace_value: str = "",
        valid_data_types: tuple[str, ...] = (),
        **kwargs,
    ) -> None:
        self.valid_data_types = valid_data_types or self.valid_data_types
        self._column = column
        self._replace_value = replace_value
        super().__init__(data_filter=data_filter, **kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Removes all rows that are valid in filter"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self._column not in data_holder.data.columns:
            adm_logger.log_transformation(
                f"No column named {self._column}",
                level=adm_logger.DEBUG,
            )
            return
        mask = self._get_filter_mask(data_holder)
        if mask.is_empty():
            adm_logger.log_transformation(
                f"Could not run transformer {PolarsRemoveMask.__class__.__name__}. "
                f"Missing data_filter",
                level=adm_logger.ERROR,
            )
            return
        data_holder.data = data_holder.data.with_columns(
            pl.when(mask)
            .then(pl.lit(self._replace_value))
            .otherwise(pl.col(self._column))
            .alias(self._column)
        )


class PolarsRemoveProfiles(PolarsTransformer):
    valid_data_types = ("profile",)
    valid_data_holders = ("PolarsZipArchiveDataHolder",)

    def __init__(
        self,
        data_filter: PolarsDataFilter,
        **kwargs,
    ) -> None:
        super().__init__(data_filter=data_filter, **kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Removes profiles specified in the given filter"

    def _transform(self, data_holder: PolarsZipArchiveDataHolder) -> None:
        mask = self._get_filter_mask(data_holder)
        if mask.is_empty():
            adm_logger.log_transformation(
                f"Could not run transformer {PolarsRemoveProfiles.__class__.__name__}. "
                f"Missing data_filter",
                level=adm_logger.ERROR,
            )
            return
        remove_df = data_holder.data.filter(mask)
        file_names_to_remove = list(set(remove_df["profile_file_name_db"]))
        file_names_to_remove.append("metadata.txt")
        data_holder.remove_files_in_processed_directory(file_names_to_remove)
        data_holder.remove_received_data_directory()


class PolarsRemoveBottomDepthInfoProfiles(PolarsTransformer):
    valid_data_types = ("profile",)
    valid_data_holders = ("PolarsZipArchiveDataHolder",)

    def __init__(
        self,
        data_filter: PolarsDataFilter,
        **kwargs,
    ) -> None:
        super().__init__(data_filter=data_filter, **kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Removes profiles specified in the given filter"

    def _transform(self, data_holder: PolarsZipArchiveDataHolder) -> None:
        mask = self._get_filter_mask(data_holder)
        if mask.is_empty():
            raise
            adm_logger.log_transformation(
                f"Could not run transformer {PolarsRemoveProfiles.__class__.__name__}. "
                f"Missing data_filter",
                level=adm_logger.ERROR,
            )
            return
        remove_df = data_holder.data.filter(mask)
        file_names_to_remove = list(set(remove_df["profile_file_name_db"]))

        data_holder.modify_files_in_processed_directory(
            ["metadata.txt"], func=modify.remove_wadep_in_metadata_file, overwrite=True
        )

        data_holder.modify_files_in_processed_directory(
            file_names_to_remove,
            func=modify.remove_depth_info_in_standard_format,
            overwrite=True,
        )
        data_holder.remove_received_data_directory()


class PolarsRemoveValueInRowsForParameters(PolarsTransformer):
    valid_data_structures = ("row",)

    parameter_column = "parameter"
    value_column = "value"

    def __init__(self, *parameters: str, replace_value: str = "", **kwargs) -> None:
        super().__init__(**kwargs)
        self._replace_value = replace_value
        self.apply_on_parameters = parameters
        if isinstance(parameters[0], list):
            self.apply_on_parameters = parameters[0]

    @staticmethod
    def get_transformer_description() -> str:
        return (
            "Removes or replaces value column in rows for given parameters. "
            "Transformer also takes data filter. "
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self.parameter_column not in data_holder.data:
            adm_logger.log_transformation(
                f"Can not remove values in rows. "
                f'Missing column "{self.parameter_column}"',
                level=adm_logger.WARNING,
            )
            return
        filter_mask = self._get_filter_mask(data_holder)
        for par in self.apply_on_parameters:
            mask = data_holder.data[self.parameter_column] == par
            if not filter_mask.is_empty():
                mask = filter_mask & mask

            data_holder.data = data_holder.data.with_columns(
                pl.when(mask)
                .then(pl.lit(self._replace_value))
                .otherwise(pl.col(self.value_column))
                .alias(self.value_column)
            )

            nr_rows = len(data_holder.data.filter(mask))
            if not nr_rows:
                continue
            adm_logger.log_transformation(
                f'Replacing value (-> {self._replace_value}) for parameter "{par}" '
                f"({nr_rows} rows)",
                level=adm_logger.WARNING,
            )


class PolarsRemoveValueInColumns(PolarsTransformer):
    def __init__(
        self,
        *columns: str,
        regex: bool = False,
        replace_value: int | float | str = "",
        only_when_value: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.apply_on_columns = columns
        if isinstance(columns[0], list):
            self.apply_on_columns = columns[0]

        self._regex = regex
        self._replace_value = str(replace_value)
        self._only_when_value = only_when_value

    @staticmethod
    def get_transformer_description() -> str:
        return (
            "Removes all values in given columns. "
            "Column names can be perfect match or regular expresiones. "
            "Option to set replace_value. Transformer also takes data filter. "
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        for col in self._get_apply_on_columns(data_holder):
            if col not in data_holder.data.columns:
                continue
            mask = self._get_filter_mask(data_holder)
            if self._only_when_value:
                if not mask.is_empty():
                    mask = mask & (data_holder.data[col] != "")
            if not mask.is_empty():
                data_holder.data = data_holder.data.with_columns(
                    pl.when(mask)
                    .then(pl.lit(self._replace_value))
                    .otherwise(pl.col(col))
                    .alias(col)
                )
                nr_rows = len(data_holder.data.filter(mask))
            else:
                if self._only_when_value:
                    mask = data_holder.data[col] != ""
                    data_holder.data = data_holder.data.with_columns(
                        pl.when(mask)
                        .then(pl.lit(self._replace_value))
                        .otherwise(pl.col(col))
                        .alias(col)
                    )
                else:
                    data_holder.data = data_holder.data.with_columns(
                        pl.lit(self._replace_value).alias(col)
                    )
                nr_rows = len(data_holder.data)
            if self._replace_value:
                adm_logger.log_transformation(
                    f"All values in column {col} are set to {self._replace_value} "
                    f"(all {nr_rows} places)",
                    level=adm_logger.WARNING,
                )
            else:
                adm_logger.log_transformation(
                    f"All values in column {col} are removed (all {nr_rows} places)",
                    level=adm_logger.WARNING,
                )

    def _get_apply_on_columns(self, data_holder: PolarsDataHolder):
        if not self._regex:
            return [
                col for col in data_holder.data.columns if col in self.apply_on_columns
            ]

        columns = []
        for col in data_holder.data.columns:
            for arg in self.apply_on_columns:
                if re.search(arg, col):
                    columns.append(col)
                    break
        return columns
