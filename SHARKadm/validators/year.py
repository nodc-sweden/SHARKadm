from .base import Validator, DataHolderProtocol
from SHARKadm import adm_logger


class ValidateYearNrDigits(Validator):

    @staticmethod
    def get_validator_description() -> str:
        return 'Checks that year is a valid four digit number'

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data['year'] = data_holder.data['year'].apply(self.check)

    @staticmethod
    def check(x):
        if len(x) == 4:
            return
        adm_logger.log_validation(f'Year "{x}" is not of length 4')
