from .base import Transformer, DataHolderProtocol
from SHARKadm import adm_logger


class AddRowNumber(Transformer):
    col_to_set = 'row_number'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds row number. This column can typically be used to reference dta in log. If needed, use this as ' \
               f'one of ' \
               f'the ' \
               f'first transformers!'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.col_to_set in data_holder.data:
            adm_logger.log_transformation(f'Column {self.col_to_set} already present. Will not overwrite')
            return 
        data_holder.data[self.col_to_set] = (data_holder.data.index + 1).astype(str)
