from sharkadm.sharkadm_logger import adm_logger

from .base import DataHolderProtocol, Transformer


class AddSampleIdPrefix(Transformer):
    key_to_add = "sample_id_prefix"

    @staticmethod
    def get_transformer_description() -> str:
        return "Sets sample_id_prefix from data source column"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if "source" not in data_holder.data.columns:
            adm_logger.log_transformation("Missing key: source", level="error")
            return
        data_holder.data[self.key_to_add] = data_holder.data["source"].apply(
            self.extract_id
        )

    @staticmethod
    def extract_id(x) -> str:
        return x.split("\\")[-1].split(".")[0].upper().replace("DATA", "")
