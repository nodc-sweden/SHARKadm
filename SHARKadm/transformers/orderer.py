from SHARKadm import adm_logger
from translate_codes import get_translate_codes_object
from .base import Transformer, DataHolderProtocol


class AddSwedishSampleOrderer(Transformer):
    col_to_set = 'sample_orderer_name_sv'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._loaded_code_info = {}
        self._codes = get_translate_codes_object()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds sample orderer name in swedish'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = data_holder.data.apply(lambda row: self._get_code(row), axis=1)

    def _get_code(self, row):
        code = row['sample_orderer_code']
        info = self._loaded_code_info.setdefault(code, self._codes.get_info('laboratory', code))
        if not info:
            adm_logger.log_transformation(f'Could not find information for sample_orderer_code: {code}')
            return ''
        return info['swedish']


class AddEnglishSampleOrderer(Transformer):
    col_to_set = 'sample_orderer_name_en'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._loaded_code_info = {}
        self._codes = get_translate_codes_object()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds sample orderer name in english'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = data_holder.data.apply(lambda row: self._get_code(row), axis=1)

    def _get_code(self, row):
        code = row['sample_orderer_code']
        info = self._loaded_code_info.setdefault(code, self._codes.get_info('laboratory', code))
        if not info:
            adm_logger.log_transformation(f'Could not find information for sample_orderer_code: {code}')
            return ''
        return info['english']



