import logging
import pathlib

from SHARKadm.data import data_source
from .archive_data_holder import ArchiveDataHolder

logger = logging.getLogger(__name__)


class ZoobenthosArchiveSkvDataHolder(ArchiveDataHolder):
    _data_type = 'Zoobenthos'
    _data_format = 'Zoobenthos'

    def __init__(self, *args, **kwargs) -> None:
        self._file_paths: dict = dict()
        self._file_data: dict = dict()

        super().__init__(*args, **kwargs)

    def _load_data(self) -> None:
        self._load_file_paths()
        if self._data_is_in_single_data_file():
            self._load_single_data_file()
        else:
            self._load_several_data_files()
        self._check_columns()
        self._fix_columns()

    def _load_file_paths(self) -> None:
        self._file_paths = dict()
        for path in self.processed_data_directory.iterdir():
            if path.name == 'data.txt':
                self._file_paths[path.stem] = path
                return
            if path.suffix != '.skv':
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
        if path.suffix == '.skv':
            d_source = self._get_skv_data_source(path)
        else:
            d_source = self._get_txt_data_source(path)
        self._set_data_source(d_source)

    def _load_several_data_files(self) -> None:
        self._add_data_sources()
        self._merge_data()

    def _get_skv_data_source(self, path: pathlib.Path):
        d_source = data_source.SkvDataFile(path=path, data_type=self.delivery_note.data_type)
        d_source.map_header(self.import_matrix_mapper)
        return d_source

    def _get_txt_data_source(self, path: pathlib.Path):
        d_source = data_source.TxtRowFormatDataFile(path=path, data_type=self.delivery_note.data_type)
        d_source.map_header(self.import_matrix_mapper)
        return d_source

    def _add_data_sources(self) -> None:
        for name, path in self._file_paths.items():
            d_source = data_source.SkvDataFile(path=path, data_type=self.delivery_note.data_type)
            d_source.map_header(self.import_matrix_mapper)

            d_source.add_concatenated_column('visit_key', [
                'visit_date',
                'reported_station_name'
            ])
            if name in ['sample', 'abundance']:
                d_source.add_concatenated_column('sample_key', [
                    'visit_date',
                    'reported_station_name',
                    'sample_id'
                ])

            self._add_data_source(d_source)
            self._file_data[name] = d_source

    def _merge_data(self) -> None:
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

    def _check_columns(self) -> None:
        """Check if same columns from different files has the same value"""
        pass

    def _fix_columns(self) -> None:
        """Removes duplicated columns"""
        current_cols = list(self.data.columns)
        new_cols = []
        for col in current_cols:
            if col[-2:] in ['_x', '_y']:
                continue
            new_cols.append(col)
        self._data = self._data[new_cols]
