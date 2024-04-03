import datetime
import pathlib

from sharkadm import utils, adm_logger
import pandas as pd
try:
    from nodc_station.main import App
    from nodc_station.station_file import DEFAULT_STATION_FILE_PATH
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(f'Could not import package "{module_name}" in module {__name__}. You need to install this dependency if you want to use this module.', level=adm_logger.WARNING)


from .base import Exporter, DataHolderProtocol


class HtmlStationMap(Exporter):
    """Creates a html station map"""

    def __init__(self,
                 export_directory: str | pathlib.Path | None = None,
                 export_file_name: str | pathlib.Path | None = None,
                 open_map: bool = False, **kwargs):
        super().__init__(**kwargs)
        if not export_directory:
            export_directory = utils.get_export_directory()
        self._export_directory = pathlib.Path(export_directory)
        self._export_file_name = export_file_name
        self._open_map = open_map

    def _get_path(self, data_holder: DataHolderProtocol) -> pathlib.Path:
        if not self._export_file_name:
            self._export_file_name = f'station_map_{data_holder.dataset_name}.html'
        path = pathlib.Path(self._export_directory, self._export_file_name)
        if path.suffix != '.html':
            path = pathlib.Path(str(path) + '.html')
        return path

    @staticmethod
    def get_exporter_description() -> str:
        return 'Creates a html station map.'

    def _export(self, data_holder: DataHolderProtocol) -> None:
        app = App()

        # Read master list
        app.read_list(
            DEFAULT_STATION_FILE_PATH,
            reader='shark_master',
            list_name='master'
        )

        list_name = 'sharkadm_data'
        df = self._get_position_dataframe(data_holder.data)
        print(df)
        app.add_list_data(df, list_name=list_name)

        export_path = self._get_path(data_holder)

        print(f'{DEFAULT_STATION_FILE_PATH=}')
        print(f'{export_path=}')

        app.write_list(
            writer='map',
            # list_names=['master'],
            # list_names=['stnreg_import'],
            list_names=['master', list_name],
            new_stations_as_cluster=False,
            file_path=str(export_path)
        )
        if self._open_map:
            utils.open_file_with_default_program(export_path)

    def _get_position_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Removes duplicated positions in dataframe"""
        unique_df = df.drop_duplicates(
            subset=['sample_latitude_dd', 'sample_longitude_dd'],
            keep='last').reset_index(drop=True)
        remove_boolean = (unique_df['sample_latitude_dd'] == '') | (unique_df['sample_longitude_dd'] == '')
        return unique_df[~remove_boolean]

