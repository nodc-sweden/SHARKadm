from .base import Transformer, DataHolderProtocol


class AddCruiseId(Transformer):
    valid_data_holders = ["LimsDataHolder"]

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds cruise id column"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data["cruise_id"] = (
            data_holder.data["visit_year"]
            .str.cat(data_holder.data["platform_code"], "_")
            .str.cat(data_holder.data["expedition_id"], "_")
        )
