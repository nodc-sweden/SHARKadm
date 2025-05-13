from ..sharkadm_logger import adm_logger
from .base import DataHolderProtocol, Transformer


class SetBacteriaAsReportedScientificName(Transformer):
    valid_data_types = ("Bacterioplankton",)
    col_to_set = "scientific_name"
    value_to_set = "Bacteria"

    def __init__(self, apply_on_columns: list[str] | None = None, **kwargs):
        super().__init__(**kwargs)
        if apply_on_columns:
            self.apply_on_columns = apply_on_columns

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {SetBacteriaAsReportedScientificName.value_to_set} as "
            f"{SetBacteriaAsReportedScientificName.col_to_set} if column does not exist"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.col_to_set not in data_holder.data.columns:
            data_holder.data[self.col_to_set] = self.value_to_set
            self._log(
                f"Added column {self.col_to_set} with value {self.value_to_set}"
            )
