from sharkadm import adm_logger
from .base import Transformer, DataHolderProtocol


try:
    from nodc_codes import get_translate_codes_object
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(f'Could not import package "{module_name}" in module {__name__}. You need to install this dependency if you want to use this module.', level=adm_logger.WARNING)

_translate_codes = get_translate_codes_object()


class _AddCodes(Transformer):
    source_cols = ['']
    col_to_set = ''
    lookup_key = ''
    lookup_field = ''

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._loaded_code_info = {}

    @staticmethod
    def get_transformer_description() -> str:
        return f''

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        source_col = ''
        for col in self.source_cols:
            if col in data_holder.data.columns:
                source_col = col
                break
        if not source_col:
            adm_logger.log_transformation(f'None of the source columns {self.source_cols} found when trying to set {self.col_to_set}',
                                          level=adm_logger.WARNING)
            return
        if self.col_to_set not in data_holder.data.columns:
            data_holder.data[self.col_to_set] = ''
        for code in set(data_holder.data[source_col]):
            names = []
            for part in code.split(','):
                part = part.strip()
                info = _translate_codes.get_info(self.lookup_field, part)
                if info:
                    names.append(info[self.lookup_key])
                else:
                    adm_logger.log_transformation(f'Could not find information for {source_col}: {part}',
                                                  level=adm_logger.WARNING)
                    names.append('?')
            index = data_holder.data[source_col] == code
            data_holder.data.loc[index, self.col_to_set] = ', '.join(names)


class _AddCodesLab(_AddCodes):
    lookup_field = 'LAB'


class _AddCodesProj(_AddCodes):
    lookup_field = 'project'