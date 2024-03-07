from SHARKadm import adm_logger
from translate_codes import get_translate_codes_object
from .base import Transformer
from SHARKadm.data import get_archive_data_holder_names

from typing import Protocol
import pandas as pd


class DataHolderProtocol(Protocol):

    @property
    def data(self) -> pd.DataFrame:
        ...

    @property
    def reporting_institute(self) -> str:
        ...


class AddSwedishReportingInstitute(Transformer):
    valid_data_holders = get_archive_data_holder_names()
    col_to_set = 'reporting_institute_name_sv'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._loaded_code_info = {}
        self._codes = get_translate_codes_object()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds reporting institute name in swedish'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if data_holder.reporting_institute:
            data_holder.data[self.col_to_set] = self._get_name(data_holder.reporting_institute)
        elif 'reporting_institute_name_en' in data_holder.data.columns:
            col = 'reporting_institute_name_en'
            data_holder.data[self.col_to_set] = data_holder.data.apply(lambda row, col=col: self._get_from_en(row, col), axis=1)
        else:
            adm_logger.log_transformation(f'No "reporting institute" found. Setting empty string',
                                          level=adm_logger.WARNING)
            data_holder.data[self.col_to_set] = ''
            return

    def _get_from_en(self, row, col):
        code = row[col]
        info = self._loaded_code_info.setdefault(code, self._codes.get_info('laboratory', code))
        if not info:
            adm_logger.log_transformation(f'Could not find information for {col}: {code}')
            return ''
        return info['swedish']

    def _get_name(self, code):
        info = self._loaded_code_info.setdefault(code, self._codes.get_info('laboratory', code))
        if not info:
            adm_logger.log_transformation(f'Could not find information for reporting_institute_code: {code}')
            return ''
        return info['swedish']


class AddEnglishReportingInstitute(Transformer):
    valid_data_holders = get_archive_data_holder_names()
    col_to_set = 'reporting_institute_name_en'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._loaded_code_info = {}
        self._codes = get_translate_codes_object()

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds reporting institute name in english'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if not data_holder.reporting_institute:
            adm_logger.log_transformation(f'No "reporting institute" found. Setting empty string',
                                          level=adm_logger.WARNING)
            data_holder.data[self.col_to_set] = ''
            return
        data_holder.data[self.col_to_set] = self._get_name(data_holder.reporting_institute)

    def _get_name(self, code):
        info = self._loaded_code_info.setdefault(code, self._codes.get_info('laboratory', code))
        if not info:
            adm_logger.log_transformation(f'Could not find information for reporting_institute_code: {code}')
            return ''
        return info['english']



