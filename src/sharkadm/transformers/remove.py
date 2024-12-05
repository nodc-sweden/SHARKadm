from .base import Transformer, DataHolderProtocol
from sharkadm import adm_logger


class RemoveValuesInColumns(Transformer):

    def __init__(self, *columns: str, replace_value: int | float | str = '') -> None:
        super().__init__()
        self.apply_on_columns = columns
        if type(columns[0]) == list:
            self.apply_on_columns = columns[0]

        self._replace_value = str(replace_value)

    @staticmethod
    def get_transformer_description() -> str:
        return f'Removes all values in given columns. Option to set replace_value.'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        len_data = len(data_holder.data)
        for col in self.apply_on_columns:
            if col not in data_holder.data:
                continue
            data_holder.data[col] = self._replace_value
            if self._replace_value:
                adm_logger.log_transformation(
                    f'All values in column {col} are set to {self._replace_value} (all {len_data} places)',
                    level=adm_logger.WARNING)
            else:
                adm_logger.log_transformation(
                    f'All values in column {col} are removed (all {len_data} places)',
                    level=adm_logger.WARNING)
