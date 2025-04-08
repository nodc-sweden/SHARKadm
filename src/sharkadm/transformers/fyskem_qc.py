from sharkadm.transformers.base import DataHolderProtocol, Transformer
from sharkadm.utils.installations import verify_installation

verify_installation("fyskemqc")


class RunFyskKemQualityControl(Transformer):
    valid_data_types = ("PhysicalChemical",)

    @staticmethod
    def get_transformer_description() -> str:
        return "Applies quality control on physical chemical data"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        # qc_object = fyskemqc.fyskemqc(data_holder.data)
        ...
