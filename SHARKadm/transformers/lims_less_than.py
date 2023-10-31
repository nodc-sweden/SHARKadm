
from .base import Transformer, DataHolderProtocol
from SHARKadm import adm_logger


class MoveLessThanFlag(Transformer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f''

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        pass

