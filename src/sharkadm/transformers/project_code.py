from sharkadm import adm_logger
from .base import Transformer, DataHolderProtocol

try:
    from nodc_codes import get_translate_codes_object
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(f'Could not import package "{module_name}" in module {__name__}. You need to install this dependency if you want to use this module.', level=adm_logger.WARNING)


class AddSwedishProjectName(Transformer):
    col_to_set = 'sample_project_name_sv'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._loaded_code_info = {}
        self._codes = get_translate_codes_object()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds project name in swedish'

    # def _transform(self, data_holder: DataHolderProtocol) -> None:
    #     if 'sample_project_code' in data_holder.data.columns:
    #         data_holder.data[self.col_to_set] = data_holder.data.apply(lambda row: self._get_name_from_code(row), axis=1)
    #     else:
    #         data_holder.data[self.col_to_set] = data_holder.data.apply(lambda row: self._get_name_from_english(row),
    #                                                                    axis=1)

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        col = 'sample_project_code'
        if col not in data_holder.data.columns:
            col = 'sample_project_name_en'
        data_holder.data[self.col_to_set] = data_holder.data.apply(lambda row, col=col: self._get(row, col), axis=1)

    def _get(self, row, col):
        code = row[col]
        info = self._loaded_code_info.setdefault(code, self._codes.get_info('sample_project_code', code))
        if not info:
            adm_logger.log_transformation(f'Could not find information for {col}: {code}')
            return ''
        return info['swedish']

    # def _get_name_from_code(self, row):
    #     code = row['sample_project_code']
    #     info = self._loaded_code_info.setdefault(code, self._codes.get_info('sample_project_code', code))
    #     if not info:
    #         adm_logger.log_transformation(f'Could not find information for sample_project_code: {code}')
    #         return ''
    #     return info['swedish']
    #
    # def _get_name_from_english(self, row):
    #     code = row['sample_project_name_en']
    #     info = self._loaded_code_info.setdefault(code, self._codes.get_info('sample_project_code', code))
    #     if not info:
    #         adm_logger.log_transformation(f'Could not find information for sample_project_code: {code}')
    #         return ''
    #     return info['swedish']


class AddEnglishProjectName(Transformer):
    col_to_set = 'sample_project_name_en'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._loaded_code_info = {}
        self._codes = get_translate_codes_object()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds project name in english'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = data_holder.data.apply(lambda row: self._get_code(row), axis=1)

    def _get_code(self, row):
        code = row['sample_project_code']
        info = self._loaded_code_info.setdefault(code, self._codes.get_info('sample_project_code', code))
        if not info:
            adm_logger.log_transformation(f'Could not find information for sample_project_code: {code}')
            return ''
        return info['english']



