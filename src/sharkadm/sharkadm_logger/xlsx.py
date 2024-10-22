import datetime
import logging
from typing import TYPE_CHECKING

import pandas as pd

from sharkadm.utils.paths import get_next_incremented_file_path
from .base import SharkadmLoggerExporter

if TYPE_CHECKING:
    pass


logger = logging.getLogger(__name__)


class XlsxExporter(SharkadmLoggerExporter):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _get_default_file_name(self):
        date_str = datetime.datetime.now().strftime('%Y%m%d')
        data_string = '-'.join(list(self.adm_logger.data.keys()))
        file_name = f'sharkadm_log_{self.adm_logger.name}_{date_str}_{data_string}'
        return file_name

    def _export(self) -> None:
        self._set_save_path(suffix='.xlsx')
        df = self._extract_info(self.adm_logger.data)
        try:
            self._save_as_xlsx_with_table(df)
            logger.info(f'Saving sharkadm xlsx log to {self.file_path}')
        except PermissionError:
            self.file_path = get_next_incremented_file_path(self.file_path)
            self._save_as_xlsx_with_table(df)
            logger.info(f'Saving sharkadm xlsx log to {self.file_path}')

    def _extract_info(self, data: dict) -> pd.DataFrame:
        info = []
        header = ['Dataset name', 'Purpose', 'Level', 'Log type', 'Class', 'Message', 'Nr of logs', 'Log number']
        if self.kwargs.get('include_items'):
            header = header + ['Item']

        for level, level_data in data.items():
            for purpose, purpose_data in level_data.items():
                for log_type, log_type_data in purpose_data.items():
                    for msg, msg_data in log_type_data.items():
                        # print(f'{level=}')
                        # print(f'{purpose=}')
                        # print(f'{log_type=}')
                        # print(f'{msg=}')
                        # print(f'{msg_data=}')
                        count = msg_data.get('count', '')
                        log_nr = msg_data.get('log_nr', '')
                        cls = msg_data.get('cls', '')
                        dataset_name = msg_data.get('dataset_name', '')
                        general_line = [dataset_name, purpose, level, log_type, cls, msg, count, log_nr]
                        if not self.kwargs.get('include_items'):
                            line = general_line
                            # line = [dataset_name, purpose, level, log_type, cls, msg, count, log_nr]
                            info.append(line)
                            continue
                        if not msg_data['items']:
                            line = general_line + ['']
                            # line = [dataset_name, purpose, level, log_type, cls, msg, count, log_nr, '']
                            info.append(line)
                            continue
                        for item in msg_data['items']:
                            line = general_line + [item]
                            # line[3] = 1
                            # line = [dataset_name, purpose, level, log_type, cls, msg, count, log_nr, item]
                            info.append(line)
        df = pd.DataFrame(data=info, columns=header)
        df.fillna('', inplace=True)
        if self.kwargs.get('sort_by'):
            sort_by_columns = [col for col in df.columns if self._compress_item(col) in self._compress_list_items(self.kwargs.get('sort_by'))]
            df.sort_values(sort_by_columns, inplace=True)
        include_columns = header
        if self.kwargs.get('include_columns'):
            include_columns = [col for col in header if
                               self._compress_item(col) in self._compress_list_items(self.kwargs.get('include_columns'))]
        if self.kwargs.get('exclude_columns'):
            include_columns = [col for col in include_columns if
                               self._compress_item(col) not in self._compress_list_items(self.kwargs.get('exclude_columns'))]
        df = df[include_columns]
        return df

    def _compress_item(self, item: str) -> str:
        return item.lower().replace(' ', '')

    def _compress_list_items(self, lst: list[str] | str) -> list[str]:
        if type(lst) == str:
            lst = [lst]
        return [self._compress_item(item) for item in lst]

    def _save_as_xlsx_with_table(self, df: pd.DataFrame):
        """
        https://stackoverflow.com/questions/58326392/how-to-create-excel-table-with-pandas-to-excel
        """

        # Create a Pandas Excel writer using XlsxWriter as the engine.
        writer = pd.ExcelWriter(str(self.file_path), engine='xlsxwriter')

        # Convert the dataframe to an XlsxWriter Excel object. Turn off the default
        # header and index and skip one row to allow us to insert a user defined
        # header.
        sheet_name = self.file_path.stem.split('SHARK_')[-1][:30]
        df.to_excel(writer, sheet_name=sheet_name, startrow=1, header=False, index=False)

        # Get the xlsxwriter workbook and worksheet objects.
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]

        # Get the dimensions of the dataframe.
        (max_row, max_col) = df.shape

        # Create a list of column headers, to use in add_table().
        column_settings = []
        for header in df.columns:
            column_settings.append({'header': header})

        # Add the table.
        worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})

        # Make the columns wider for clarity.
        worksheet.set_column(0, 0, 40)
        worksheet.set_column(1, 1, 10)
        worksheet.set_column(2, 2, 15)
        worksheet.set_column(3, 3, 20)
        worksheet.set_column(4, 4, 30)
        worksheet.set_column(5, 5, 110)
        worksheet.set_column(6, 6, 10)
        worksheet.set_column(7, 7, 10)
        worksheet.set_column(8, 8, 100)

        # worksheet.set_column(0, 0, 40)
        # worksheet.set_column(1, 1, 10)
        # worksheet.set_column(2, 2, 20)
        # worksheet.set_column(3, 3, 40)
        # worksheet.set_column(4, 4, 90)
        # worksheet.set_column(5, 5, 8)
        # worksheet.set_column(6, 6, 70)

        # Close the Pandas Excel writer and output the Excel file.
        writer.close()