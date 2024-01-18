import logging
import pathlib

import pandas as pd

from SHARKadm.data import data_source
from .archive_data_holder import ArchiveDataHolder
from SHARKadm import sharkadm_exceptions

logger = logging.getLogger(__name__)


class ZoobenthosArchiveDataHolder(ArchiveDataHolder):
    _data_type = 'Zoobenthos'
    _data_format = 'Zoobenthos'

    def __init__(self, *args, **kwargs) -> None:
        self._file_paths: dict = dict()
        self._file_data: dict = dict()
        self._kwargs = kwargs

        super().__init__(*args, **kwargs)

        self._date_files_type = ''

    def _load_data(self) -> None:
        self._load_file_paths()
        if self._data_is_in_single_data_file():
            self._load_single_data_file()
        else:
            self._load_several_data_files()
        self._check_columns()
        self._data = self._fix_columns(self._data)

    def _load_file_paths(self) -> None:
        self._file_paths = dict()
        for path in self.processed_data_directory.iterdir():
            if path.name == 'data.txt':
                self._file_paths = dict()
                self._file_paths[path.stem] = path
                return
            if path.suffix == '.skv':
                self._file_paths[path.stem] = path
            elif path.name.startswith('data'):
                self._file_paths[path.stem] = path

        # self._file_paths['station'] = self.processed_data_directory / 'station.skv'
        # self._file_paths['sample'] = self.processed_data_directory / 'sample.skv'
        # self._file_paths['abundance'] = self.processed_data_directory / 'abundance.skv'

    def _data_is_in_single_data_file(self):
        """Returns True if data is in a single data.skv file"""
        if len(self._file_paths) == 1 and self._file_paths.get('data'):
            return True
        return False

    def _load_single_data_file(self) -> None:
        path = self._file_paths['data']
        logger.info(f'Loading single data file: {path}')
        # d_source = data_source.SkvDataFile(path=path, data_type=self.delivery_note.data_type)
        # d_source.map_header(self.import_matrix_mapper)
        d_source = self._get_data_source(path)
        self._set_data_source(d_source)

    def _get_data_source(self, path: pathlib.Path) -> data_source.DataFile:
        if path.suffix == '.skv':
            d_source = self._get_skv_data_source(path)
            self._date_files_type = 'skv'
        else:
            d_source = self._get_txt_data_source(path)
            self._date_files_type = 'txt'
        d_source.map_header(self.import_matrix_mapper)
        return d_source

    def _load_several_data_files(self) -> None:
        self._add_data_sources()
        if self._kwargs.get('merge_data', True):
            self._merge_data()

    def _get_skv_data_source(self, path: pathlib.Path):
        d_source = data_source.SkvDataFile(path=path, data_type=self.delivery_note.data_type)
        return d_source

    def _get_txt_data_source(self, path: pathlib.Path):
        d_source = data_source.TxtRowFormatDataFile(path=path, data_type=self.delivery_note.data_type)
        return d_source

    def _add_data_sources(self) -> None:
        for name, path in self._file_paths.items():
            # print()
            # print(f'{name=}')
            d_source = self._get_data_source(path)
            # for col in sorted(d_source.mapped_columns):
            #     print(f'    {col} -> {d_source.mapped_columns[col]}')
            # d_source = data_source.SkvDataFile(path=path, data_type=self.delivery_note.data_type)
            # d_source.map_header(self.import_matrix_mapper)

            d_source.add_concatenated_column('visit_key', [
                'visit_date',
                'reported_station_name'
            ])
            if name in ['sample', 'abundance', 'dataGlodVatten', 'dataHugg', 'dataRedox', 'dataSedimentfarg']:
                d_source.add_concatenated_column('sample_key', [
                    'visit_date',
                    'reported_station_name',
                    'sample_id'
                ])

            self._add_data_source(d_source)
            self._file_data[name] = d_source

    def _merge_data(self) -> None:
        if self._date_files_type == 'skv':
            self._merge_files_origin_skv()
        else:
            self._merge_files_origin_txt()

    def _merge_files_origin_skv(self):
        sample_data = self._get_data_from_data_source(self._file_data['sample'])
        station_data = self._get_data_from_data_source(self._file_data['station'])
        abundance_data = self._get_data_from_data_source(self._file_data['abundance'])

        # Merge data
        visit_key_data = sample_data.merge(station_data, on='visit_key')
        sample_key_data = abundance_data.merge(visit_key_data, on='sample_key')

        self.sample_data = sample_data
        self.station_data = station_data
        self.abundance_data = abundance_data
        self.visit_key_data = visit_key_data
        self.sample_key_data = sample_key_data

        # Set data
        self._data = sample_key_data

        # Concatenate sources
        source_cols = [col for col in sample_key_data.columns if 'source' in col]
        self._add_concatenated_column('source', source_cols)

    def _merge_files_origin_txt(self):
        botten_data = self._get_data_from_data_source(self._file_data['dataBottenvatten'])
        hugg_data = self._get_data_from_data_source(self._file_data['dataHugg'])
        glod_data = self._get_data_from_data_source(self._file_data['dataGlodVatten'])
        redox_data = self._get_data_from_data_source(self._file_data['dataRedox'])
        sediment_data = self._get_data_from_data_source(self._file_data['dataSedimentfarg'])

        # Merge data
        visit_key_data = hugg_data.merge(botten_data, on='visit_key')
        sample_key_data = glod_data.merge(visit_key_data, on='sample_key')
        sample_key_data = redox_data.merge(sample_key_data, on='sample_key')
        sample_key_data = sediment_data.merge(sample_key_data, on='sample_key')

        # Set data
        self._data = sample_key_data

        # Concatenate sources
        source_cols = [col for col in sample_key_data.columns if 'source' in col]
        self._add_concatenated_column('source', source_cols)

    def _check_columns(self) -> None:
        """Check if same columns from different files has the same value"""
        pass

    def _fix_columns_old(self) -> None:
        """Removes duplicated columns"""
        current_cols = list(self.data.columns)
        new_cols = []
        for col in current_cols:
            if col[-2:] in ['_x', '_y']:
                continue
            new_cols.append(col)
        self._data = self._data[new_cols]

    @staticmethod
    def _fix_columns(df:pd.DataFrame) -> pd.DataFrame:
        """Removes duplicated columns"""
        current_cols = list(df.columns)
        new_cols = []
        for col in current_cols:
            if col[-2:] in ['_x', '_y']:
                continue
            new_cols.append(col)
        df = df[new_cols]
        return df


class ZoobenthosBiomadArchiveDataHolder(ArchiveDataHolder):
    _data_type = 'Zoobenthos'
    _data_format = 'Zoobenthosbiomad'

    def __init__(self, *args, **kwargs) -> None:
        self._file_paths: dict = dict()
        self._file_data: dict = dict()
        self._kwargs = kwargs

        super().__init__(*args, **kwargs)

        self._date_files_type = ''

    def _load_data(self) -> None:
        self._load_file_paths()
        if self._data_is_in_single_data_file():
            self._load_single_data_file()
        else:
            self._load_several_data_files()
        self._check_columns()
        self._data = self._fix_columns(self._data)

    def _load_file_paths(self) -> None:
        self._file_paths = dict()
        for path in self.processed_data_directory.iterdir():
            if path.name == 'data.txt':
                self._file_paths = dict()
                self._file_paths[path.stem] = path
                return
            if path.suffix == '.skv':
                self._file_paths[path.stem] = path
            elif path.name.startswith('data'):
                self._file_paths[path.stem] = path

        # self._file_paths['station'] = self.processed_data_directory / 'station.skv'
        # self._file_paths['sample'] = self.processed_data_directory / 'sample.skv'
        # self._file_paths['abundance'] = self.processed_data_directory / 'abundance.skv'

    def _data_is_in_single_data_file(self):
        """Returns True if data is in a single data.skv file"""
        if len(self._file_paths) == 1 and self._file_paths.get('data'):
            return True
        return False

    def _load_single_data_file(self) -> None:
        path = self._file_paths['data']
        logger.info(f'Loading single data file: {path}')
        # d_source = data_source.SkvDataFile(path=path, data_type=self.delivery_note.data_type)
        # d_source.map_header(self.import_matrix_mapper)
        d_source = self._get_data_source(path)
        self._set_data_source(d_source)

    def _get_data_source(self, path: pathlib.Path) -> data_source.DataFile:
        if path.suffix == '.skv':
            d_source = self._get_skv_data_source(path)
            self._date_files_type = 'skv'
        else:
            d_source = self._get_txt_data_source(path)
            self._date_files_type = 'txt'
        d_source.map_header(self.import_matrix_mapper)
        return d_source

    def _load_several_data_files(self) -> None:
        self._add_data_sources()
        if self._kwargs.get('merge_data', True):
            self._merge_data()

    def _get_skv_data_source(self, path: pathlib.Path):
        d_source = data_source.SkvDataFile(path=path, data_type=self.delivery_note.data_type)
        return d_source

    def _get_txt_data_source(self, path: pathlib.Path):
        d_source = data_source.TxtRowFormatDataFile(path=path, data_type=self.delivery_note.data_type)
        return d_source

    def _add_data_sources(self) -> None:
        for name, path in self._file_paths.items():
            # print()
            # print(f'{name=}')
            d_source = self._get_data_source(path)
            # for col in sorted(d_source.mapped_columns):
            #     print(f'    {col} -> {d_source.mapped_columns[col]}')
            # d_source = data_source.SkvDataFile(path=path, data_type=self.delivery_note.data_type)
            # d_source.map_header(self.import_matrix_mapper)

            d_source.add_concatenated_column('visit_key', [
                'visit_date',
                'reported_station_name'
            ])
            if name in ['sample', 'abundance', 'dataGlodVatten', 'dataHugg', 'dataRedox', 'dataSedimentfarg']:
                d_source.add_concatenated_column('sample_key', [
                    'visit_date',
                    'reported_station_name',
                    'sample_id'
                ])

            self._add_data_source(d_source)
            self._file_data[name] = d_source

    def _merge_data(self) -> None:
        if self._date_files_type == 'skv':
            self._merge_files_origin_skv()
        else:
            self._merge_files_origin_txt()

    def _merge_files_origin_skv(self):
        sample_data = self._get_data_from_data_source(self._file_data['sample'])
        station_data = self._get_data_from_data_source(self._file_data['station'])
        abundance_data = self._get_data_from_data_source(self._file_data['abundance'])

        # Merge data
        visit_key_data = sample_data.merge(station_data, on='visit_key')
        sample_key_data = abundance_data.merge(visit_key_data, on='sample_key')

        # Set data
        self._data = sample_key_data

        # Concatenate sources
        source_cols = [col for col in sample_key_data.columns if 'source' in col]
        self._add_concatenated_column('source', source_cols)

    def _merge_files_origin_txt(self):
        botten_data = self._get_data_from_data_source(self._file_data['dataBottenvatten'])
        hugg_data = self._get_data_from_data_source(self._file_data['dataHugg'])
        glod_data = self._get_data_from_data_source(self._file_data['dataGlodVatten'])
        redox_data = self._get_data_from_data_source(self._file_data['dataRedox'])
        sediment_data = self._get_data_from_data_source(self._file_data['dataSedimentfarg'])

        # Merge data
        visit_key_data = hugg_data.merge(botten_data, on='visit_key')
        sample_key_data = glod_data.merge(visit_key_data, on='sample_key')
        sample_key_data = redox_data.merge(sample_key_data, on='sample_key')
        sample_key_data = sediment_data.merge(sample_key_data, on='sample_key')

        # Set data
        self._data = sample_key_data

        # Concatenate sources
        source_cols = [col for col in sample_key_data.columns if 'source' in col]
        self._add_concatenated_column('source', source_cols)

    def _check_columns(self) -> None:
        """Check if same columns from different files has the same value"""
        pass

    def _fix_columns_old(self) -> None:
        """Removes duplicated columns"""
        current_cols = list(self.data.columns)
        new_cols = []
        for col in current_cols:
            if col[-2:] in ['_x', '_y']:
                continue
            new_cols.append(col)
        self._data = self._data[new_cols]

    @staticmethod
    def _fix_columns(df:pd.DataFrame) -> pd.DataFrame:
        """Removes duplicated columns"""
        current_cols = list(df.columns)
        new_cols = []
        for col in current_cols:
            if col[-2:] in ['_x', '_y']:
                continue
            new_cols.append(col)
        df = df[new_cols]
        return df


class ZoobenthosBedaArchiveDataHolder(ArchiveDataHolder):
    _data_type = 'Zoobenthos'
    _data_format = 'Zoobenthosbeda'

    def __init__(self, *args, **kwargs) -> None:
        self._file_paths: dict = dict()
        self._file_data: dict = dict()
        self._kwargs = kwargs

        super().__init__(*args, **kwargs)

        self._date_files_type = ''

    def _load_data(self) -> None:
        self._load_file_paths()
        if self._data_is_in_single_data_file():
            self._load_single_data_file()
        else:
            self._load_several_data_files()
        self._check_columns()
        self._data = self._fix_columns(self._data)

    def _load_file_paths(self) -> None:
        self._file_paths = dict()
        for path in self.processed_data_directory.iterdir():
            if path.name == 'data.txt':
                self._file_paths = dict()
                self._file_paths[path.stem] = path
                return
            if path.suffix == '.skv':
                self._file_paths[path.stem] = path
            elif path.name.startswith('data'):
                self._file_paths[path.stem] = path

        # self._file_paths['station'] = self.processed_data_directory / 'station.skv'
        # self._file_paths['sample'] = self.processed_data_directory / 'sample.skv'
        # self._file_paths['abundance'] = self.processed_data_directory / 'abundance.skv'

    def _data_is_in_single_data_file(self):
        """Returns True if data is in a single data.skv file"""
        if len(self._file_paths) == 1 and self._file_paths.get('data'):
            return True
        return False

    def _load_single_data_file(self) -> None:
        path = self._file_paths['data']
        logger.info(f'Loading single data file: {path}')
        # d_source = data_source.SkvDataFile(path=path, data_type=self.delivery_note.data_type)
        # d_source.map_header(self.import_matrix_mapper)
        d_source = self._get_data_source(path)
        self._set_data_source(d_source)

    def _get_data_source(self, path: pathlib.Path) -> data_source.DataFile:
        if path.suffix == '.skv':
            d_source = self._get_skv_data_source(path)
            self._date_files_type = 'skv'
        else:
            d_source = self._get_txt_data_source(path)
            self._date_files_type = 'txt'
        d_source.map_header(self.import_matrix_mapper)
        return d_source

    def _load_several_data_files(self) -> None:
        self._add_data_sources()
        if self._kwargs.get('merge_data', True):
            self._merge_data()

    def _get_skv_data_source(self, path: pathlib.Path):
        d_source = data_source.SkvDataFile(path=path, data_type=self.delivery_note.data_type)
        return d_source

    def _get_txt_data_source(self, path: pathlib.Path):
        d_source = data_source.TxtRowFormatDataFile(path=path, data_type=self.delivery_note.data_type)
        return d_source

    def _add_data_sources(self) -> None:
        for name, path in self._file_paths.items():
            d_source = self._get_data_source(path)
            # for col in sorted(d_source.mapped_columns):
            #     print(f'    {col} -> {d_source.mapped_columns[col]}')
            # d_source = data_source.SkvDataFile(path=path, data_type=self.delivery_note.data_type)
            # d_source.map_header(self.import_matrix_mapper)

            d_source.add_concatenated_column('visit_key', [
                'visit_date',
                'reported_station_name'
            ])
            if name in ['sample', 'abundance', 'dataGlodVatten', 'dataHugg', 'dataRedox', 'dataSedimentfarg']:
                d_source.add_concatenated_column('sample_key', [
                    'visit_date',
                    'reported_station_name',
                    'sample_id'
                ])

            self._add_data_source(d_source)
            self._file_data[name] = d_source

    def _merge_data(self) -> None:
        try:
            if self._date_files_type == 'skv':
                self._merge_files_origin_skv()
            else:
                self._merge_files_origin_txt()
        except pd.errors.MergeError as e:
            raise sharkadm_exceptions.ArchiveDataError(e)
        except KeyError as e:
            raise sharkadm_exceptions.ArchiveDataError(e)

    def _merge_files_origin_skv(self):
        sample_data = self._get_data_from_data_source(self._file_data['sample'])
        station_data = self._get_data_from_data_source(self._file_data['station'])
        abundance_data = self._get_data_from_data_source(self._file_data['abundance'])

        # Merge data
        visit_key_data = sample_data.merge(station_data, on='visit_key')
        sample_key_data = abundance_data.merge(visit_key_data, on='sample_key')

        # Set data
        self._data = sample_key_data

        # Concatenate sources
        source_cols = [col for col in sample_key_data.columns if 'source' in col]
        self._add_concatenated_column('source', source_cols)

    def _merge_files_origin_txt(self):
        botten_data = self._get_data_from_data_source(self._file_data['dataBottenvatten'])
        hugg_data = self._get_data_from_data_source(self._file_data['dataHugg'])
        glod_data = self._get_data_from_data_source(self._file_data['dataGlodVatten'])
        redox_data = self._get_data_from_data_source(self._file_data['dataRedox'])
        sediment_data = self._get_data_from_data_source(self._file_data['dataSedimentfarg'])

        # Merge data
        visit_key_data = hugg_data.merge(botten_data, on='visit_key')
        sample_key_data = glod_data.merge(visit_key_data, on='sample_key')
        sample_key_data = redox_data.merge(sample_key_data, on='sample_key')
        sample_key_data = sediment_data.merge(sample_key_data, on='sample_key')

        # Set data
        self._data = sample_key_data

        # Concatenate sources
        source_cols = [col for col in sample_key_data.columns if 'source' in col]
        self._add_concatenated_column('source', source_cols)

    def _check_columns(self) -> None:
        """Check if same columns from different files has the same value"""
        pass

    def _fix_columns_old(self) -> None:
        """Removes duplicated columns"""
        current_cols = list(self.data.columns)
        new_cols = []
        for col in current_cols:
            if col[-2:] in ['_x', '_y']:
                continue
            new_cols.append(col)
        self._data = self._data[new_cols]

    @staticmethod
    def _fix_columns(df:pd.DataFrame) -> pd.DataFrame:
        """Removes duplicated columns"""
        current_cols = list(df.columns)
        new_cols = []
        for col in current_cols:
            if col[-2:] in ['_x', '_y']:
                continue
            new_cols.append(col)
        df = df[new_cols]
        return df
