import logging
import pathlib

from sharkadm.data import data_source

from ..data_source.base import DataFile
from .archive_data_holder import ArchiveDataHolder, PolarsArchiveDataHolder

logger = logging.getLogger(__name__)


class PolarsZoobenthosArchiveDataHolder(PolarsArchiveDataHolder):
    _data_type_synonym = "zoobenthos"
    _data_format = "Zoobenthos"


class ZoobenthosArchiveDataHolder(ArchiveDataHolder):
    _data_type = "Zoobenthos"
    _data_format = "Zoobenthos"

    def __init__(self, *args, **kwargs) -> None:
        self._file_paths: dict = dict()
        self._file_data: dict = dict()
        self._kwargs = kwargs

        super().__init__(*args, **kwargs)

        self._date_files_type = ""

    def _load_data(self) -> None:
        self._load_file_paths()
        if self._data_is_in_single_data_file():
            self._load_single_data_file()
        else:
            self._load_several_data_files()
        self._check_columns()
        self._fix_data()

    def _load_file_paths(self) -> None:
        self._file_paths = dict()
        for path in self.processed_data_directory.iterdir():
            if path.name == "data.txt":
                self._file_paths = dict()
                self._file_paths[path.stem] = path
                return
            if path.suffix == ".skv":
                self._file_paths[path.stem] = path
            elif path.name.startswith("data"):
                self._file_paths[path.stem] = path

        # self._file_paths['station'] = self.processed_data_directory / 'station.skv'
        # self._file_paths['sample'] = self.processed_data_directory / 'sample.skv'
        # self._file_paths['abundance'] = self.processed_data_directory / 'abundance.skv'

    def _data_is_in_single_data_file(self):
        """Returns True if data is in a single data.skv file"""
        if len(self._file_paths) == 1 and self._file_paths.get("data"):
            return True
        return False

    def _load_single_data_file(self) -> None:
        path = self._file_paths["data"]
        logger.info(f"Loading single data file: {path}")
        # d_source = data_source.SkvDataFile(
        #     path=path, data_type=self.delivery_note.data_type
        # )
        # d_source.map_header(self.import_matrix_mapper)
        d_source = self._get_data_source(path)
        self._set_data_source(d_source)

    def _get_data_source(self, path: pathlib.Path) -> DataFile:
        if path.suffix == ".skv":
            d_source = self._get_skv_data_source(path)
            self._date_files_type = "skv"
        else:
            d_source = self._get_txt_data_source(path)
            self._date_files_type = "txt"
        d_source.map_header(self.import_matrix_mapper)
        return d_source

    def _load_several_data_files(self) -> None:
        self._add_data_sources()
        if self._kwargs.get("merge_data", True):
            self._merge_data()

    def _get_skv_data_source(self, path: pathlib.Path):
        d_source = data_source.SkvDataFile(
            path=path, data_type=self.delivery_note.data_type
        )
        return d_source

    def _get_txt_data_source(self, path: pathlib.Path):
        d_source = data_source.TxtRowFormatDataFile(
            path=path, data_type=self.delivery_note.data_type
        )
        return d_source

    def _add_visit_column_to_data_source(self, d_source):
        visit_columns = ["visit_date", "reported_station_name"]
        d_source.add_concatenated_column("visit_key", visit_columns)

    def _add_smpno_column_to_data_source(self, d_source):
        visit_columns = ["visit_key", "sample_id"]
        d_source.add_concatenated_column("smpno_key", visit_columns)

    def _add_smlnk_column_to_data_source(self, d_source):
        visit_columns = ["smpno_key", "sample_link"]
        d_source.add_concatenated_column("smlnk_key", visit_columns)

    def _add_data_sources(self) -> None:
        for name, path in self._file_paths.items():
            d_source = self._get_data_source(path)

            self._add_visit_column_to_data_source(d_source)
            self._add_smpno_column_to_data_source(d_source)
            self._add_smlnk_column_to_data_source(d_source)

            self._add_data_source(d_source)
            self._file_data[name] = d_source

    @staticmethod
    def _merge_dataframes(df1, df2, on, how="outer"):
        cols_to_use = [on] + [col for col in df2.columns if col not in df1.columns]
        df = df1.merge(df2[cols_to_use], how=how, on=on)
        return df

    def _merge_data(self) -> None:
        if self._date_files_type == "skv":
            self._merge_files_origin_skv()
        else:
            self._merge_files_origin_txt()

    def _merge_files_origin_skv(self):
        abun = self._get_data_from_data_source(self._file_data["abundance"])
        samp = self._get_data_from_data_source(self._file_data["sample"])
        sedi = self._get_data_from_data_source(self._file_data["sediment"])
        stat = self._get_data_from_data_source(self._file_data["station"])

        print(f"{stat.columns=}")

        # Merge data
        visit_df = self._merge_dataframes(sedi, stat, "visit_key", how="left")
        smpno_df = self._merge_dataframes(samp, visit_df, "smpno_key", how="left")
        self._data = self._merge_dataframes(abun, smpno_df, "smlnk_key", how="left")

    def _merge_files_origin_txt(self):
        print("-" * 100)
        print(f"{self._data_type=}")
        print(f"{self._data_format=}")
        raise

    def _check_columns(self) -> None:
        """Check if same columns from different files has the same value"""
        pass

    def _fix_data(self) -> None:
        self._data = self._data.fillna("")
        self._data.reset_index(inplace=True, drop=True)
        return self._data


class ZoobenthosBiomadArchiveDataHolder(ZoobenthosArchiveDataHolder):
    _data_type = "Zoobenthos"
    _data_format = "Zoobenthosbiomad"

    def _merge_files_origin_skv(self):
        pass
        # sample_data = self._get_data_from_data_source(self._file_data['sample'])
        # station_data = self._get_data_from_data_source(self._file_data['station'])
        # abundance_data = self._get_data_from_data_source(self._file_data['abundance'])
        #
        # # Merge data
        # visit_key_data = sample_data.merge(station_data, on='visit_key')
        # sample_key_data = abundance_data.merge(visit_key_data, on='sample_key')
        #
        # # Set data
        # self._data = sample_key_data
        #
        # # Concatenate sources
        # source_cols = [col for col in sample_key_data.columns if 'source' in col]
        # self._add_concatenated_column('source', source_cols)

    def _merge_files_origin_txt(self):
        raise


class ZoobenthosBedaArchiveDataHolder(ZoobenthosArchiveDataHolder):
    _data_type = "Zoobenthos"
    _data_format = "Zoobenthosbeda"

    def _add_data_sources(self) -> None:
        for name, path in self._file_paths.items():
            print(f"{path.name=}")
            d_source = self._get_data_source(path)

            id_columns = [
                "visit_date",
                "reported_station_name",
                "sample_id",
                "visit_date",
            ]
            # id_columns = [
            #     'datum', 'stnbet', 'provNr', 'bottenvattenProv.provnr', 'besok.datum'
            # ]

            d_source.add_concatenated_column("merge_key", id_columns)

            self._add_data_source(d_source)
            self._file_data[name] = d_source

    def _merge_files_origin_skv(self):
        pass
        # sample_data = self._get_data_from_data_source(self._file_data['sample'])
        # station_data = self._get_data_from_data_source(self._file_data['station'])
        # abundance_data = self._get_data_from_data_source(self._file_data['abundance'])
        #
        # # Merge data
        # visit_key_data = sample_data.merge(station_data, on='visit_key')
        # sample_key_data = abundance_data.merge(visit_key_data, on='sample_key')
        #
        # # Set data
        # self._data = sample_key_data
        #
        # # Concatenate sources
        # source_cols = [col for col in sample_key_data.columns if 'source' in col]
        # self._add_concatenated_column('source', source_cols)

    def _merge_files_origin_txt(self):
        glod = self._get_data_from_data_source(self._file_data["dataGlodVatten"])
        bott = self._get_data_from_data_source(self._file_data["dataBottenvatten"])
        sedi = self._get_data_from_data_source(self._file_data["dataSedimentfarg"])
        redo = self._get_data_from_data_source(self._file_data["dataRedox"])
        hugg = self._get_data_from_data_source(self._file_data["dataHugg"])

        # Merge data
        df = self._merge_dataframes(bott, glod, "merge_key", how="left")
        df = self._merge_dataframes(sedi, df, "merge_key", how="left")
        df = self._merge_dataframes(redo, df, "merge_key", how="left")
        self._data = self._merge_dataframes(hugg, df, "merge_key", how="left")
