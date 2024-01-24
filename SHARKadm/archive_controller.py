import os

from SHARKadm import controller
from SHARKadm import transformers
from SHARKadm.data import archive
from SHARKadm import validators
from SHARKadm import exporters
from SHARKadm.data import lims
from SHARKadm import adm_logger
from SHARKadm.data import get_data_holder
import pathlib
import pandas as pd
from SHARKadm import adm_logger
from SHARKadm.data.archive.delivery_note import DeliveryNote

from SHARKadm.data.archive import ZoobenthosArchiveDataHolder
from SHARKadm import sharkadm_exceptions
import re
from SHARKadm import utils
from SHARKadm import event
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

    def filter_columns(self, *cols, exclude: list[str] | None = None):
        if self.data.empty:
            raise Exception('No data loaded')
        exclude = exclude or []
        columns = []
        for col in cols:
            if type(col) == str:
                col = [col]
            else:
                col = list(col)
            for c in col:
                if col in exclude:
                    continue
                if c not in self.data.columns:
                    continue
                columns.append(c)
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
                    line_data['data_format_missing'] = 'x'
            # temp_list = [item for item in list(info_set) if item.startswith('i')]
            # print(f'{temp_list=}')
            info.append(line_data)

        temp_list = [item for item in info_set if item.lower().startswith('kom')]
        print(f'{temp_list=}')
        cols_to_use = ['archive_directory_name', 'delivery_note_missing', 'data_format_missing'] + sorted(info_set,
                                                                                                          key=lambda
                                                                                                              x:
                                                                                                          x.lower())
        # print(f'{cols_to_use=}')
        self._create_dataframe(info, cols_to_use)
        self._save_path = utils.get_export_directory() / f'delivery_note_summary_{self._data_type}.txt'
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

    def _create_dataframe(self, info, columns):
        temp_list_columns = [item for item in columns if item.lower().startswith('kom')]
        print(f'{temp_list_columns=}')
        data = []
        for line in info:
            new_line = []
            for col in columns:
                new_line.append(line.get(col, ''))
            data.append(new_line)
        self._df = pd.DataFrame(data=data, columns=columns)
        temp_list_columns_df = [item for item in self._df.columns if item.lower().startswith('kom')]
        print(f'{temp_list_columns_df=}')

    def save_data(self, **kwargs):
        self._save_path = kwargs.get('path', self._save_path)
        self.data.to_csv(self._save_path, sep=kwargs.get('sep', '\t'), index=False, encoding=kwargs.get('encoding',
                                                                                                        'cp1252'))
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



if __name__ == '__main__':
    folder = r'C:\Arbetsmapp\datasets\Zoobenthos\SHARK_Zoobenthos_1985_KMRS'

    if 0:
        a = ArchiveController(r'C:\Arbetsmapp\datasets')
        # a.set_data_type('zoobenthos').filter_archive_name('.*UMSC.*').create_column_summary('WAVES', 'SEAST', 'WEATH').save_data(
        a.set_data_type('zoobenthos').filter_archive_name('.*SLUA_FORS.*').create_column_summary(all_columns=True,
                                                                                            ).save_data()

        adm_logger.print_log()

    if 0:
        d = DeliveryNote.from_txt_file(
            r"C:\Arbetsmapp\datasets\Zoobenthos\SHARK_Zoobenthos_2022_DEEP\processed_data\delivery_note.txt")

    if 0:
        event.subscribe('workflow', print_workflow)
        a = ArchiveController(r'C:\Arbetsmapp\datasets')
        a.set_data_type('zoobenthos').create_delivery_note_summary() # .save_data().open_file_with_excel()
        # a.set_data_type('zoobenthos').create_delivery_note_summary().filter_columns('archive_directory_name',
        #                                                                             'import_matrix_key', 'format',
        #                                                                             'data_format'
        #                                                                             ).save_data().open_file_with_excel()

    if 0:
        event.subscribe('workflow', print_workflow)
        a = ArchiveController(r'C:\Arbetsmapp\datasets')
        a.set_data_type('zoobenthos').create_files_list().filter_column_content(
            exclude=dict(path='correspondence')).save_data(encoding='utf8').open_file_with_excel()


    if 1:
        # z = ZoobenthosArchiveDataHolder(folder, merge_data=True)
        # z = ZoobenthosArchiveDataHolder(r'C:\Arbetsmapp\datasets\Zoobenthos\SHARK_Zoobenthos_1983_UMSC', merge_data=True)
        # z = ZoobenthosArchiveDataHolder(r'C:\Arbetsmapp\datasets\Zoobenthos\SHARK_Zoobenthos_1983_UMSC', merge_data=True)
        z = get_data_holder(r'C:\Arbetsmapp\datasets\Zoobenthos\SHARK_Zoobenthos_1983_1994_UMSC')

        # botten_data = z._get_data_from_data_source(z._file_data['dataBottenvatten'])
        # hugg_data = z._get_data_from_data_source(z._file_data['dataHugg'])
        # glod_data = z._get_data_from_data_source(z._file_data['dataGlodVatten'])
        # redox_data = z._get_data_from_data_source(z._file_data['dataRedox'])
        # sediment_data = z._get_data_from_data_source(z._file_data['dataSedimentfarg'])
        #
        # visit_key_data = hugg_data.merge(botten_data, on='visit_key')

    if 0:
        path = pathlib.Path(folder, 'processed_data')
        data = []
        for p in path.iterdir():
            if not p.name.startswith('data'):
                continue
            with open(p) as fid:
                split_line = [item.strip() for item in fid.readline().split('\t')]
            for col in split_line:
                data.append(f'{p.name}\t{col}')

        with open(r'C:\mw\git\SHARKadm\temp_data\compare/zb_columns.txt', 'w') as fid:
            fid.write('\n'.join(data))


    def add_concatenated_column(data, new_column: str, columns_to_use: list[str]) -> None:
        """Adds a concatenated column specified in new_column using the columns listed in columns_to_use"""
        for col in columns_to_use:
            if new_column not in data.columns:
                data[new_column] = data[col]
            else:
                data[new_column] = data[new_column] + data[col]

    if 0:
        z = ZoobenthosArchiveDataHolder(folder, merge_data=False)

        data = dict()
        for key, d in z._data_sources.items():
            name = pathlib.Path(key.split()[-1]).stem
            data[name] = d.data

        for name, df in data.items():
            add_concatenated_column(df, 'visit_key', ['visit_date', 'reported_station_name'])

            if name not in ['dataBottenvatten']:
                add_concatenated_column(df, 'sample_key', ['visit_date', 'reported_station_name', 'sample_id'])





    if 0:
        ac = ArchiveController(r'C:\Arbetsmapp\datasets')
        ac.get_column_mapping_summary('Zoobenthos')
        data_holder = get_data_holder(r"C:\mw\git\SHARKadm\temp_data\SHARK_Zoobenthos_2023_MEDINS_NLST_version_2024-01-12")

        c = controller.SHARKadmController()
        c.set_data_holder(data_holder)