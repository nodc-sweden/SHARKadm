import datetime

import pandas as pd

from sharkadm.utils.paths import get_next_incremented_file_path

from .base import SharkadmLoggerExporter

COLUMN_WIDTH = dict(
    cls=33,
    column=5,
    columns=5,
    dataset_name=40,
    item=5,
    level=15,
    log_nr=10,
    log_type=15,
    msg=120,
    purpose=5,
    row_numbers=10,
    validation_success=10,
    validator=40,
)


def get_column_width(col: str) -> int:
    return COLUMN_WIDTH.get(col, 30)


class XlsxExporter(SharkadmLoggerExporter):
    def _get_default_file_name(self):
        date_str = datetime.datetime.now().strftime("%Y%m%d")
        data_string = "-".join(self.adm_logger.filtered_on_levels)
        file_name = f"sharkadm_log_{self.adm_logger.name}_{date_str}_{data_string}"
        return file_name

    def _export(self) -> None:
        self._set_save_path(suffix=".xlsx")
        df = self._extract_info()
        try:
            self._save(df)
        except PermissionError:
            self.file_path = get_next_incremented_file_path(self.file_path)
            self._save(df)

    def _save(self, df: pd.DataFrame) -> None:
        if self.kwargs.get("as_table"):
            self._save_as_xlsx_with_table(df)
        elif self.kwargs.get("with_filter"):
            self._save_as_xlsx_with_filter(df)
        else:
            self._save_as_xlsx(df)

    def _extract_info(self) -> pd.DataFrame:
        header = self.adm_logger.keys
        df = pd.DataFrame(data=self.adm_logger.data, columns=header, dtype=str)
        df.fillna("", inplace=True)
        if self.kwargs.get("columns"):
            columns = [col for col in self.kwargs.get("columns") if col in df.columns]
            df = df[columns]
        if self.kwargs.get("sort_by"):
            sort_by_columns = [
                col
                for col in df.columns
                if self._compress_item(col)
                in self._compress_list_items(self.kwargs.get("sort_by"))
            ]
            df.sort_values(sort_by_columns, inplace=True)
        include_columns = header
        if self.kwargs.get("include_columns"):
            include_columns = [
                col
                for col in header
                if self._compress_item(col)
                in self._compress_list_items(self.kwargs.get("include_columns"))
            ]
        if self.kwargs.get("exclude_columns"):
            include_columns = [
                col
                for col in include_columns
                if self._compress_item(col)
                not in self._compress_list_items(self.kwargs.get("exclude_columns"))
            ]
        df = df[include_columns]
        return df

    def _compress_item(self, item: str) -> str:
        return item.lower().replace(" ", "")

    def _compress_list_items(self, lst: list[str] | str) -> list[str]:
        if isinstance(lst, str):
            lst = [lst]
        return [self._compress_item(item) for item in lst]

    @property
    def writer(self) -> pd.ExcelWriter:
        return pd.ExcelWriter(str(self.file_path), engine="xlsxwriter")

    @property
    def sheet_name(self) -> str:
        return self.file_path.stem.split("SHARK_")[-1][:30]

    @staticmethod
    def _set_column_width(df: pd.DataFrame, worksheet):
        for c, col in enumerate(df.columns):
            width = get_column_width(col)
            worksheet.set_column(c, c, width)

    def _save_as_xlsx(self, df: pd.DataFrame):
        with self.writer as writer:
            df.to_excel(
                writer, sheet_name=self.sheet_name, startrow=0, header=True, index=False
            )
            worksheet = writer.sheets[self.sheet_name]
            self._set_column_width(df, worksheet)

    def _save_as_xlsx_with_filter(self, df: pd.DataFrame):
        with self.writer as writer:
            df.to_excel(
                writer, sheet_name=self.sheet_name, startrow=0, header=True, index=False
            )
            worksheet = writer.sheets[self.sheet_name]

            (max_row, max_col) = df.shape
            worksheet.autofilter(0, 0, max_row, max_col - 1)
            self._set_column_width(df, worksheet)

    def _save_as_xlsx_with_table(self, df: pd.DataFrame):
        """
        https://stackoverflow.com/questions/58326392/how-to-create-excel-table-with-pandas-to-excel
        """
        with self.writer as writer:
            df.to_excel(
                writer, sheet_name=self.sheet_name, startrow=1, header=False, index=False
            )

            worksheet = writer.sheets[self.sheet_name]
            (max_row, max_col) = df.shape
            column_settings = []
            for header in df.columns:
                column_settings.append({"header": header})
            worksheet.add_table(0, 0, max_row, max_col - 1, {"columns": column_settings})
            self._set_column_width(df, worksheet)
