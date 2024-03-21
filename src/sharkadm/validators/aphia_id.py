from .base import Validator, DataHolderProtocol

from sharkadm import adm_logger
import logging

logger = logging.getLogger(__name__)


class ValidateAphiaId(Validator):
    reported_aphia_id_col = 'reported_aphia_id'
    aphia_id_col = 'aphia_id'

    @staticmethod
    def get_validator_description() -> str:
        return f'Checks if translated {ValidateAphiaId.aphia_id_col} is the same as {ValidateAphiaId.reported_aphia_id_col}'

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        if not all([col in data_holder.data for col in [self.reported_aphia_id_col, self.aphia_id_col]]):
            adm_logger.log_validation(f'One or more columns missing in to execute {self.__class__.__name__}', level=adm_logger.WARNING)
            return

        if not any(data_holder.data[self.reported_aphia_id_col]):
            adm_logger.log_validation(f'No values in {self.reported_aphia_id_col}')
            return

        for (reported, translated), df in data_holder.data.groupby([self.reported_aphia_id_col, self.aphia_id_col]):
            if reported == translated:
                continue
            if not reported:
                if translated:
                    adm_logger.log_validation(f'{self.aphia_id_col} {translated} added {len(df)} times')
                    continue
                adm_logger.log_validation(f'Missing {self.aphia_id_col} in {len(df)} row')
                continue
            adm_logger.log_validation(f'{self.aphia_id_col} differs from reported: {reported} -> {translated} ({len(df)} times)')


