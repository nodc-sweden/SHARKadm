import os

from sharkadm import controller
from sharkadm import transformers
from sharkadm.data import archive
from sharkadm import validators
from sharkadm import exporters
from sharkadm.data import lims
from sharkadm import adm_logger
from sharkadm.data import get_data_holder
import pathlib
import pandas as pd
from sharkadm import adm_logger
from sharkadm.data.archive.delivery_note import DeliveryNote
from sharkadm.data.archive.shark_metadata import SharkMetadata

from sharkadm.data.archive import ZoobenthosArchiveDataHolder
from sharkadm import sharkadm_exceptions
import re
from sharkadm import utils
from sharkadm.utils import matching_strings
from sharkadm import event
import numpy as np


class ArchiveController:
    def __init__(self, archive_root_directory: str | pathlib.Path):
        self._archive_root_directory = pathlib.Path(archive_root_directory)
        self._data_type: str | None = None
        self._archive_list: list[pathlib.Path] = []
        self._df = pd.DataFrame()
        self._save_path: pathlib.Path | None = None

    @property
    def data(self) -> pd.DataFrame:
        return self._df

    @property
    def data_type(self) -> str:
        return self._data_type

    @property
    def archive_list(self) -> list[pathlib.Path]:
        if not self._archive_list:
            self._archive_list = [path for path in self.archive_directory.iterdir() if path.name.startswith('SHARK')]
        return self._archive_list

    @property
    def archive_directory(self) -> pathlib.Path:
        if not self.data_type:
            raise Exception('data_type is not set!')
        directory = pathlib.Path(self._archive_root_directory, self.data_type)
        if not directory.exists():
            raise NotADirectoryError(directory)
        return directory

    def reset(self):
        self._data_type = None
        self._archive_list = []
        self._df = pd.DataFrame()
        return self

    def set_data_type(self, data_type: str):
        for path in self._archive_root_directory.iterdir():
            if data_type.lower() == path.name.lower():
                self._data_type = path.name
                return self
        raise Exception(f'Invalid data_type: {data_type}')

    def filter_archive_name(self, string: str):
        if not self.data.empty:
            self._filter_archive_name_in_data(string)
        else:
            self._filter_archive_name_in_list(string)
        return self

    def filter_columns(self, include: list[str] | None = None, exclude: list[str] | None = None):
        if self.data.empty:
            raise Exception('No data loaded')
        include_columns = list(self.data.columns)
        exclude_columns = []
        if include:
            include_columns = matching_strings.get_matching_strings(self.data.columns, include)
        if exclude_columns:
            exclude_columns = matching_strings.get_matching_strings(self.data.columns, exclude)
        columns = []
        for col in self.data.columns:
            if col in exclude_columns:
                continue
            if col not in include_columns:
                continue
            columns.append(col)
        self._df = self._df[columns]
        return self

    def filter_column_content(self, exclude: dict[str, str] | None = None, **kwargs):
        """Filter column 'key' that include 'value' in kwargs"""
        exclude = exclude or dict()
        boolean = np.array([np.ones(len(self.data))], dtype='bool')
        for key, value in kwargs.items():
            boolean = boolean & self.data[key].str.contains(value)
        for key, value in exclude.items():
            boolean = boolean & ~self.data[key].str.contains(value)
        self._df = self.data[boolean]
        return self

    def _filter_archive_name_in_data(self, string: str):
        self._df = self._df[self._df['archive'].str.contains(string)]

    def _filter_archive_name_in_list(self, string: str):
        new_archive_list = []
        for path in self.archive_list:
            if not re.match(string, path.name):
                continue
            new_archive_list.append(path)
        self._archive_list = new_archive_list

    def create_delivery_note_summary(self):
        if not self.data_type:
            raise Exception('data_type is not set!')
        info = []
        info_set = set()
        for path in self.archive_list:
            adm_logger.log_workflow(f'Looking at: {path}')
            line_data = dict()
            delivery_note_path = path / 'processed_data' / 'delivery_note.txt'
            line_data['archive_directory_name'] = path.name
            if not delivery_note_path.exists():
                line_data['delivery_note_missing'] = 'x'
            else:
                try:
                    delivery_note = DeliveryNote.from_txt_file(delivery_note_path)
                    for key, value in delivery_note.data.items():
                        if 'kommentar' == key:
                            print(f'{path.name=}: {key=}: {value=}')
                    line_data.update(delivery_note.data)
                    # print(f'{delivery_note.data.keys()=}')
                    info_set.update(list(line_data))
                except sharkadm_exceptions.NoDataFormatFoundError:
                    print('====¤¤¤¤¤====')
                    line_data['data_format_missing'] = 'x'
            # temp_list = [item for item in list(info_set) if item.startswith('i')]
            # print(f'{temp_list=}')
            info.append(line_data)

        # temp_list = [item for item in info_set if item.lower().startswith('kom')]
        # print(f'{temp_list=}')
        column_mapping = {}
        first_cols = ['archive_directory_name', 'delivery_note_missing', 'data_format_missing']
        collection = {}
        cols_to_use = dict((item, item) for item in first_cols)
        for col in info_set:
            col_lower = col.lower().strip()
            collection.setdefault(col_lower, [])
            col_name = f'{col} {'.' * len(collection[col_lower])}'.strip()
            collection[col_lower].append(col)
            cols_to_use[col] = col_name

        cols_to_use = dict((key, cols_to_use[key]) for key in sorted(cols_to_use, key=lambda x: x.lower()))

        # other_columns = sorted([col for col in info_set if col.lower() not in first_cols])
        # cols_to_use = ['archive_directory_name', 'delivery_note_missing', 'data_format_missing'] + other_columns
        self.info = info
        self.cols_to_use = cols_to_use
        # print(f'{cols_to_use=}')
        self._create_dataframe(info, cols_to_use)
        self._save_path = utils.get_export_directory() / f'delivery_note_summary_{self._data_type}.txt'
        return self

    def create_shark_metadata_summary(self):
        if not self.data_type:
            raise Exception('data_type is not set!')
        info = []
        info_set = set()
        for path in self.archive_list:
            adm_logger.log_workflow(f'Looking at: {path}')
            line_data = dict()
            shark_metadata_path = path / 'shark_metadata.txt'
            line_data['archive_directory_name'] = path.name
            if not shark_metadata_path.exists():
                line_data['shark_metadata_missing'] = 'x'
            else:
                delivery_note = SharkMetadata.from_txt_file(shark_metadata_path)
                line_data.update(delivery_note.data)
                info_set.update(list(line_data))
            info.append(line_data)

        first_cols = ['archive_directory_name', 'shark_metadata_missing']
        collection = {}
        cols_to_use = dict((item, item) for item in first_cols)
        for col in info_set:
            col_lower = col.lower().strip()
            collection.setdefault(col_lower, [])
            col_name = f'{col} {'.'*len(collection[col_lower])}'.strip()
            collection[col_lower].append(col)
            cols_to_use[col] = col_name

        cols_to_use = dict((key, cols_to_use[key]) for key in sorted(cols_to_use, key=lambda x: x.lower()))
        self.info = info
        self.cols_to_use = cols_to_use
        self._create_dataframe(info, cols_to_use)
        self._save_path = utils.get_export_directory() / f'shark_metadata_summary_{self._data_type}.txt'
        return self

    def create_column_summary(self, *columns: str, all_columns=False):
        if not self.data_type:
            raise Exception('data_type is not set!')
        info = []
        all_columns_set = set()
        for path in self.archive_list:
            adm_logger.log_workflow(f'Looking at: {path}')
            line = dict(archive=path.name)
            mapped_columns = dict()
            try:
                data_holder = get_data_holder(path)
                line['format'] = data_holder.delivery_note.import_matrix_key
                mapped_columns = data_holder.mapped_columns
                if all_columns:
                    all_columns_set.update(list(mapped_columns))
                    columns = list(mapped_columns)
                # print(f'{data_holder.data.columns=}')
                # print('='*100)
                # print(f'{path.name=}')
                # for key in sorted(mapped_columns):
                #     value = mapped_columns[key]
                #     print(f'    {key} -> {value}')
                #
                # for c in sorted(data_holder.data.columns):
                #     print(f'    {c=}')

                for col in columns:
                    internal_col = mapped_columns.get(col)
                    # if internal_col and internal_col in data_holder.data.columns:
                    if internal_col:
                        if any(data_holder.data[internal_col]):
                            line[col] = 'has values'
                        else:
                            line[col] = 'x'
                    else:
                        line[col] = ''
                    print(f'{path.name}:   {col} -> {internal_col} = {line[col]}')
                info.append(line)

            except (sharkadm_exceptions.ArchiveDataError, sharkadm_exceptions.DeliveryNoteError):
                adm_logger.log_workflow(f'Could not create data_holder for {path}')
                continue
                # # Check for zoobenthos beda format
                # processed_folder = pathlib.Path(path, 'processed_data')
                # for file in processed_folder.iterdir():
                #     if not file.suffix == '.txt':
                #         continue
                #     if not file.name.startswith('data'):
                #         continue
                #     with open(file) as fid:
                #         cols = [item.strip() for item in fid.readline().split('\t')]
                #         for col in cols:
                #             mapped_columns[col] = True
                # try:
                #     delivery_note = DeliveryNote.from_txt_file(processed_folder / 'delivery_note.txt')
                #     line['format'] = delivery_note.import_matrix_key
                # except sharkadm_exceptions.DeliveryNoteError:
                #     line['format'] = 'missing'

        cols = sorted(all_columns_set) or list(columns)
        cols_to_use = ['archive', 'format'] + cols
        self._create_dataframe(info, cols_to_use)
        nr = 4
        string = '-'.join([col for col in self.data.columns if col not in ['archive', 'format']][:4])
        if len(self.data.columns) > nr:
            string = string + '___'
        self._save_path = utils.get_export_directory() / f'column_summary_{self._data_type}_{string}.txt'
        return self

    def create_files_list(self):
        """
        Lists all files in the
        """
        columns = ['archive', 'path', 'parent_directory_name', 'name', 'suffix']

        def get_data_from_path(archive: str, p: pathlib.Path) -> dict[str, str]:
            return dict(
                archive=archive,
                path=str(p),
                parent_directory_name=p.parent.name,
                name=p.name,
                suffix=p.suffix
            )

        info = []
        for directory in self.archive_list:
            for root, dirs, files in os.walk(directory):
                for name in files:
                    path = pathlib.Path(root, name)
                    info.append(get_data_from_path(directory.name, path))
                for name in dirs:
                    path = pathlib.Path(root, name)
                    info.append(get_data_from_path(directory.name, path))
        self._create_dataframe(info, columns=columns)
        self._save_path = utils.get_export_directory() / f'file_list_{self._data_type}.txt'
        return self

    def _create_dataframe(self, info: list[dict], columns: list | dict):
        temp_list_columns = [item for item in columns if item.lower().startswith('kom')]
        print(f'{temp_list_columns=}')
        data = []
        if type(columns) is list:
            columns = dict((col, col) for col in columns)
        for line in info:
            new_line = []
            for col in columns:
                new_line.append(line.get(col, ''))
            data.append(new_line)
        self._df = pd.DataFrame(data=data, columns=list(columns.values()))
        temp_list_columns_df = [item for item in self._df.columns if item.lower().startswith('kom')]
        print(f'{temp_list_columns_df=}')

    def save_data_as_txt(self, **kwargs):
        self._save_path = kwargs.get('path', self._save_path)
        self._save_path = self._save_path.with_suffix('.txt')
        self.data.to_csv(self._save_path, sep=kwargs.get('sep', '\t'), index=False, encoding=kwargs.get('encoding',
                                                                                                        'cp1252'))
        return self

    def save_data_as_xlsx(self, **kwargs):
        """
        https://stackoverflow.com/questions/58326392/how-to-create-excel-table-with-pandas-to-excel
        """
        self._save_path = kwargs.get('path', self._save_path)
        self._save_path = self._save_path.with_suffix('.xlsx')
        # Create a Pandas Excel writer using XlsxWriter as the engine.
        writer = pd.ExcelWriter(str(self._save_path), engine='xlsxwriter')

        # Convert the dataframe to an XlsxWriter Excel object. Turn off the default
        # header and index and skip one row to allow us to insert a user defined
        # header.
        sheet_name = self._save_path.stem.split('SHARK_')[-1][:30]
        self.data.to_excel(writer, sheet_name=sheet_name, startrow=1, header=False, index=False)

        # Get the xlsxwriter workbook and worksheet objects.
        # workbook = writer.book
        worksheet = writer.sheets[sheet_name]

        # Get the dimensions of the dataframe.
        (max_row, max_col) = self.data.shape

        # Create a list of column headers, to use in add_table().
        column_settings = []
        for header in self.data.columns:
            column_settings.append({'header': header})

        # Add the table.
        worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})

        # Make the columns wider for clarity.
        worksheet.set_column(0, max_col - 1, 30)

        # Close the Pandas Excel writer and output the Excel file.
        writer.close()

        return self

    def open_file(self):
        if self._save_path:
            utils.open_file_with_default_program(self._save_path)
        return self

    def open_file_with_excel(self):
        if self._save_path:
            utils.open_file_with_excel(self._save_path)
        return self

    def open_directory(self):
        if self._save_path:
            utils.open_directory(self._save_path.parent)
        return self


def print_workflow(msg):
    print(msg)
