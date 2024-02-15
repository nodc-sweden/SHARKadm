from .base import Transformer, DataHolderProtocol
from SHARKadm import adm_logger


class AddBacteriaAsReportedScientificName(Transformer):
    valid_data_types = ['Bacterioplankton']
    col_to_set = 'reported_scientific_name'
    value_to_set = 'Bacteria'

    def __init__(self, apply_on_columns: list[str] | None = None, **kwargs):
        super().__init__(**kwargs)
        if apply_on_columns:
            self.apply_on_columns = apply_on_columns

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds {AddBacteriaAsReportedScientificName.value_to_set} as {AddBacteriaAsReportedScientificName.col_to_set} if column does not exist'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.col_to_set not in data_holder.data.columns:
            data_holder.data[self.col_to_set] = self.value_to_set
            adm_logger.log_transformation(f'Added column {self.col_to_set} with value {self.value_to_set}')