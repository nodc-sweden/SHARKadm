from .base import Validator, DataHolderProtocol

from SHARKadm import adm_logger
import logging

logger = logging.getLogger(__name__)


class ValidateUniqueSampleId(Validator):
    unique_column = 'sharkadm_sample_id'

    @staticmethod
    def get_validator_description() -> str:
        return 'Checks for duplicates in sharkadm_sample_id'

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        if self.unique_column not in data_holder.data.columns:
            adm_logger.log_validation(f'Could not check unique sample id. No column named {self.unique_column}',
                                      level='error')
            return
        duplicates = set(data_holder.data[data_holder.data[self.unique_column].duplicated()][self.unique_column])
        for dup in duplicates:
            adm_logger.log_validation(f'Duplicate in {self.unique_column} = {dup}')

