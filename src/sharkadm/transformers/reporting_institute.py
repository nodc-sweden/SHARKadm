from sharkadm import adm_logger
from ._codes import _translate_codes
from .base import Transformer

from typing import Protocol
import pandas as pd

try:
    from nodc_codes import get_translate_codes_object
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(f'Could not import package "{module_name}" in module {__name__}. You need to install this dependency if you want to use this module.', level=adm_logger.WARNING)


class DataHolderProtocol(Protocol):

    @property
    def data(self) -> pd.DataFrame:
        ...

    @property
    def reporting_institute(self) -> str:
        ...


class _ReportingInstitute(Transformer):
    source_cols = ['']
    col_to_set = ''
    lookup_key = ''
    lookup_field = 'LABO'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._loaded_code_info = {}

    @staticmethod
    def get_transformer_description() -> str:
        return f''

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self._set_from_data(data_holder=data_holder):
            return
        if not self._set_from_other(data_holder=data_holder):
            adm_logger.log_transformation(
                f'None of the source columns {self.source_cols} found when trying to set {self.col_to_set}. And no other reporting institute found.',
                level=adm_logger.WARNING)

    def _set_from_other(self, data_holder: DataHolderProtocol) -> bool:
        if hasattr(data_holder, 'reporting_institute') and data_holder.reporting_institute:
            info = _translate_codes.get_info(self.lookup_field, data_holder.reporting_institute)
            adm_logger.log_transformation('Setting')
            data_holder.data[self.col_to_set] = info[self.lookup_key]
            return True
        return False

    def _set_from_data(self, data_holder: DataHolderProtocol) -> bool:
        source_col = ''
        for col in self.source_cols:
            if col in data_holder.data.columns:
                source_col = col
                break
        if not source_col:
            return False
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
        return True


class AddSwedishReportingInstitute(_ReportingInstitute):
    source_cols = ['reporting_institute_code', 'reporting_institute_name_en']
    col_to_set = 'reporting_institute_name_sv'
    lookup_key = 'swedish_name'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds reporting institute name in swedish'


class AddEnglishReportingInstitute(_ReportingInstitute):
    source_cols = ['reporting_institute_code', 'reporting_institute_name_sv']
    col_to_set = 'reporting_institute_name_en'
    lookup_key = 'english_name'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds reporting institute name in english'

