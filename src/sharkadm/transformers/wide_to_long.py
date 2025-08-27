import pandas as pd
import polars as pl

from sharkadm import event
from sharkadm.data import PandasDataHolder
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.utils import matching_strings

from ..data.data_holder import PolarsDataHolder
from .base import PolarsTransformer, Transformer


class WideToLong(Transformer):
    # valid_data_structures = ['column']
    invalid_data_holders = ("ZoobenthosBedaArchiveDataHolder",)

    def __init__(
        self,
        ignore_containing: str | list[str] | None = None,
        column_name_parameter: str = "parameter",
        column_name_value: str = "value",
        column_name_qf: str = "quality_flag",
        column_name_unit: str = "unit",
        keep_empty_rows: bool = False,
        **kwargs,
    ):
        # ignore_containing can be regex
        super().__init__(**kwargs)

        self._ignore_containing = ignore_containing or []
        if isinstance(self._ignore_containing, str):
            self._ignore_containing = [self._ignore_containing]

        self._column_name_parameter = column_name_parameter
        self._column_name_value = column_name_value
        self._column_name_qf = column_name_qf
        self._column_name_unit = column_name_unit
        self._keep_empty_rows = keep_empty_rows

        self._metadata_columns = []
        self._data_columns = []
        self._qf_col_mapping = {}

    @staticmethod
    def get_transformer_description() -> str:
        return "Transposes data from column data to row data"

    def _transform(self, data_holder: PandasDataHolder) -> None:
        self._qf_prefix = data_holder.qf_column_prefixes
        self._metadata_columns = []
        self._data_columns = []
        self._qf_col_mapping = {}
        if self._column_name_parameter in data_holder.columns:
            self._log(
                f"Could not transform to row format. "
                f"Column {self._column_name_parameter} already in data",
                level=adm_logger.WARNING,
            )
            return
        self._save_metadata_columns(data_holder.data)
        self._save_data_columns(data_holder.data)
        data_holder.data = self._get_transposed_data(data_holder.data)
        # self._cleanup(data_holder)
        self._add_reported_columns(data_holder)
        data_holder.data_structure = "row"

    def _save_metadata_columns(self, df: pd.DataFrame) -> None:
        for col in df.columns:
            if col == "quality_flag":
                continue
            # if 'COPY_VARIABLE' in col:
            #     continue
            if self._ignore(col):
                if not self._associated_qf_col(col, df):
                    self._metadata_columns.append(col)
                    continue
            if self._is_qf_col(col):
                continue
            if self._associated_qf_col(col, df):
                continue
            self._metadata_columns.append(col)

    def _save_data_columns(self, df: pd.DataFrame) -> None:
        for original_col in df.columns:
            col = original_col
            if "COPY_VARIABLE" in original_col:
                col = col.split(".")[1]
            qcol = self._associated_qf_col(col, df)
            if not qcol and "COPY_VARIABLE" not in original_col:
                continue
            if self._ignore(col):
                continue
            # print('')
            # print('******')
            # print(f'{col=}')
            # print(f'{qcol=}')
            # print('......')
            self._data_columns.append(original_col)
            self._qf_col_mapping[original_col] = qcol
            self._qf_col_mapping[col] = qcol

    def old_save_data_columns(self, df: pd.DataFrame) -> None:
        for original_col in df.columns:
            col = original_col
            if "COPY_VARIABLE" in original_col:
                col = col.split(".")[1]
            qcol = self._associated_qf_col(col, df)
            print("")
            print("******")
            print(f"{col=}")
            print(f"{qcol=}")
            print("......")
            if not qcol and "COPY_VARIABLE" not in original_col:
                continue
            if self._ignore(col):
                continue
            self._data_columns.append(original_col)
            self._qf_col_mapping[original_col] = qcol
            self._qf_col_mapping[col] = qcol

    def _ignore(self, col: str) -> bool:
        if matching_strings.get_matching_strings([col], self._ignore_containing):
            return True
        return False

    def _is_qf_col(self, col: str) -> bool:
        for prefix in self._qf_prefix:
            if col.startswith(prefix):
                return True
        return False

    def _associated_qf_col(self, col: str, df: pd.DataFrame) -> str:
        for prefix in self._qf_prefix:
            qcol = f"{prefix}{col}"
            if qcol in df.columns:
                return qcol
        return ""

    def _get_transposed_data(self, df: pd.DataFrame) -> pd.DataFrame:
        data = []
        len_df = len(df)
        for index, (i, row) in enumerate(df.iterrows()):
            meta = list(row[self._metadata_columns].values)
            if not index % 1000:
                event.post_event(
                    "progress",
                    dict(total=len_df, current=index, title="Transposing data"),
                )
            for col in self._data_columns:
                q_col = self._qf_col_mapping.get(col)
                value = row[col]
                if not value and not self._keep_empty_rows:
                    continue
                par = self._get_parameter_name_from_parameter(col)
                qf = row.get(q_col)
                if qf is None:
                    # self._log(
                    #     f'No quality_flag parameter ({q_col}) found for {par}',
                    #     level=adm_logger.WARNING
                    # )
                    qf = ""
                unit = self._get_unit_from_parameter(col)
                new_row = [*meta, par, value, qf, unit]
                data.append(new_row)

        # self.meta = meta
        # self.par = par
        # self.value = value
        # self.qf = qf
        # self.unit = unit
        # self.new_row = new_row
        # self.row = row
        # self.data = data
        self.columns = [
            *self._metadata_columns,
            self._column_name_parameter,
            self._column_name_value,
            self._column_name_qf,
            self._column_name_unit,
        ]
        new_df = pd.DataFrame(
            data=data,
            columns=[
                *self._metadata_columns,
                self._column_name_parameter,
                self._column_name_value,
                self._column_name_qf,
                self._column_name_unit,
            ],
        )
        return new_df

    def _cleanup(self, data_holder: PandasDataHolder):
        keep_columns = [
            col for col in data_holder.data.columns if not col.startswith("COPY_VARIABLE")
        ]
        data_holder.data = data_holder.data[keep_columns]

    def _add_reported_columns(self, data_holder: PandasDataHolder) -> None:
        cols_to_save = [
            "parameter",
            "value",
            "quality_flag",
            "unit",
        ]
        for col in cols_to_save:
            if col not in data_holder.data:
                continue
            data_holder.data[f"reported_{col}"] = data_holder.data[col]

    @staticmethod
    def _get_unit_from_parameter(par: str) -> str:
        if "." in par:
            return par.split(".")[-1]
        if "[" in par:
            return par.split("[")[-1].split("]")[0].strip()
        return ""

    @staticmethod
    def _get_parameter_name_from_parameter(par: str) -> str:
        if "." in par:
            return par.split(".")[1]
        if "[" in par:
            return par.split("[")[0].strip()
        return par

    # def _old_remove_columns(self, data_holder: archive.ArchiveDataHolder) -> None:
    #     for col in ['parameter', 'value', 'unit']:
    #         if col in data_holder.data.columns:
    #             data_holder.data.drop(col, axis=1, inplace=True)
    #
    # def _wide_to_long(self, data_holder: archive.ArchiveDataHolder) -> None:
    #     data_holder.data = data_holder.data.melt(
    #         id_vars=[col for col in data_holder.data.columns
    #         if not col.startswith('COPY_VARIABLE')],
    #         var_name='parameter'
    #     )
    #
    # def _cleanup(self, data_holder: archive.ArchiveDataHolder) -> None:
    #     data_holder.data['unit'] = data_holder.data['parameter'].apply(self._fix_unit)
    #     data_holder.data['parameter'] = data_holder.data['parameter'].apply(
    #         self._fix_parameter
    #     )
    #
    # def _fix_unit(self, x) -> str:
    #     return x.split('.')[-1]
    #
    # def _fix_parameter(self, x) -> str:
    #     return x.split('.')[1]


class PolarsWideToLong(PolarsTransformer):
    valid_data_structures = ("column",)
    # invalid_data_holders = ("ZoobenthosBedaArchiveDataHolder",)

    def __init__(
        self,
        ignore_containing: str | list[str] | None = None,
        column_name_parameter: str = "parameter",
        column_name_value: str = "value",
        column_name_qf: str = "quality_flag",
        column_name_unit: str = "unit",
        keep_empty_rows: bool = False,
        **kwargs,
    ):
        # ignore_containing can be regex
        super().__init__(**kwargs)

        self._ignore_containing = ignore_containing or []
        if isinstance(self._ignore_containing, str):
            self._ignore_containing = [self._ignore_containing]

        self._column_name_parameter = column_name_parameter
        self._column_name_value = column_name_value
        self._column_name_qf = column_name_qf
        self._column_name_unit = column_name_unit
        self._keep_empty_rows = keep_empty_rows

        self._metadata_columns = []
        self._data_columns = []
        self._qf_col_mapping = {}

    @staticmethod
    def get_transformer_description() -> str:
        return "Transposes data from column data to row data"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        self._qf_prefix = data_holder.qf_column_prefixes
        self._metadata_columns = []
        self._data_columns = []
        self._qf_col_mapping = {}
        if self._column_name_parameter in data_holder.columns:
            self._log(
                "Could not transform to row format. "
                f"Column {self._column_name_parameter} already in data",
                level=adm_logger.WARNING,
            )
            return
        self._save_metadata_columns(data_holder.data)
        self._save_data_columns(data_holder.data)
        data_holder.data = self._get_transposed_data(data_holder.data)
        # self._cleanup(data_holder)
        # self._add_reported_columns(data_holder)
        data_holder.data_structure = "row"

    def _save_metadata_columns(self, df: pd.DataFrame) -> None:
        for col in df.columns:
            if col == "quality_flag":
                continue
            # if 'COPY_VARIABLE' in col:
            #     continue
            if self._ignore(col):
                if not self._associated_qf_col(col, df):
                    self._metadata_columns.append(col)
                    continue
            if self._is_qf_col(col):
                continue
            if self._associated_qf_col(col, df):
                continue
            self._metadata_columns.append(col)

    def _save_data_columns(self, df: pd.DataFrame) -> None:
        for original_col in df.columns:
            col = original_col
            if "COPY_VARIABLE" in original_col:
                col = col.split(".")[1]
            qcol = self._associated_qf_col(col, df)
            if not qcol and "COPY_VARIABLE" not in original_col:
                continue
            if self._ignore(col):
                continue
            self._data_columns.append(original_col)
            self._qf_col_mapping[original_col] = qcol
            self._qf_col_mapping[col] = qcol

    def _ignore(self, col: str) -> bool:
        if matching_strings.get_matching_strings([col], self._ignore_containing):
            return True
        return False

    def _is_qf_col(self, col: str) -> bool:
        for prefix in self._qf_prefix:
            if col.startswith(prefix):
                return True
        return False

    def _associated_qf_col(self, col: str, df: pd.DataFrame) -> str:
        for prefix in self._qf_prefix:
            qcol = f"{prefix}{col}"
            if qcol in df.columns:
                return qcol
        return ""

    def _get_transposed_data(self, df: pl.DataFrame) -> pl.DataFrame:
        data = []
        len_df = len(df)
        for index, row in enumerate(df.iter_rows(named=True)):
            meta = [row[column] for column in self._metadata_columns]
            if not index % 1000:
                event.post_event(
                    "progress",
                    dict(total=len_df, current=index, title="Transposing data"),
                )
            for col in self._data_columns:
                q_col = self._qf_col_mapping.get(col)
                value = row[col]
                if not value and not self._keep_empty_rows:
                    continue
                par = self._get_parameter_name_from_parameter(col)
                qf = row.get(q_col)
                if qf is None:
                    # self._log(
                    #     f'No quality_flag parameter ({q_col}) found for {par}',
                    #     level=adm_logger.WARNING
                    # )
                    qf = ""
                unit = self._get_unit_from_parameter(col)
                new_row = [*meta, par, value, qf, unit]
                data.append(new_row)

        self.columns = [
            *self._metadata_columns,
            self._column_name_parameter,
            self._column_name_value,
            self._column_name_qf,
            self._column_name_unit,
        ]
        new_df = pl.DataFrame(
            data=data,
            schema=[
                *self._metadata_columns,
                self._column_name_parameter,
                self._column_name_value,
                self._column_name_qf,
                self._column_name_unit,
            ],
            orient="row",
        )
        return new_df

    def _cleanup(self, data_holder: PolarsDataHolder):
        keep_columns = [
            col for col in data_holder.data.columns if not col.startswith("COPY_VARIABLE")
        ]
        data_holder.data = data_holder.data[keep_columns]

    def _add_reported_columns(self, data_holder: PolarsDataHolder) -> None:
        cols_to_save = [
            "parameter",
            "value",
            "quality_flag",
            "unit",
        ]
        for col in cols_to_save:
            if col not in data_holder.data:
                continue
            data_holder.data = data_holder.data.with_columns(
                data_holder.data[col].alias(f"reported_{col}")
            )

    @staticmethod
    def _get_unit_from_parameter(par: str) -> str:
        if "." in par:
            return par.split(".")[-1]
        if "[" in par:
            return par.split("[")[-1].split("]")[0].strip()
        return ""

    @staticmethod
    def _get_parameter_name_from_parameter(par: str) -> str:
        if "." in par:
            return par.split(".")[1]
        if "[" in par:
            return par.split("[")[0].strip()
        return par
