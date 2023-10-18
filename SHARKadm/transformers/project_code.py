from .base import Transformer, DataHolderProtocol
from micro.stations import get_default_station_object
from SHARKadm import adm_logger
from micro.translate_codes import get_default_translate_codes_object


class AddSwedishProjectName(Transformer):
    col_to_set = 'sample_project_name_sv'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._stations = get_default_station_object()
        self._loaded_code_info = {}
        self._codes = get_default_translate_codes_object()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds project name in swedish'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = data_holder.data['sample_project_code'].apply(lambda row: self._get_code(row), axis=1)

    def _get_code(self, row):
        code = row[self.col_to_set]
        info = self._loaded_code_info.setdefault(code, self._codes.get_info('sample_project_code', code))
        if not info:
            adm_logger.log_transformation(f'Could not find information for sample_project_code: {code}')
            return ''
        return info['swedish']


class AddEnglishProjectName(Transformer):
    col_to_set = 'sample_project_name_en'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._stations = get_default_station_object()
        self._loaded_code_info = {}
        self._codes = get_default_translate_codes_object()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds project name in english'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = data_holder.data['sample_project_code'].apply(lambda row: self._get_code(row), axis=1)

    def _get_code(self, row):
        code = row[self.col_to_set]
        info = self._loaded_code_info.setdefault(code, self._codes.get_info('sample_project_code', code))
        if not info:
            adm_logger.log_transformation(f'Could not find information for sample_project_code: {code}')
            return ''
        return info['english']


