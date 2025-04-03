from .base import Transformer, DataHolderProtocol


class FakeAddPressureFromDepth(Transformer):
    valid_data_holders = ["LimsDataHolder"]

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds pressure = depth to lims data"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data["PRES"] = data_holder.data["DEPH"]
        data_holder.data["Q_PRES"] = data_holder.data["Q_DEPH"]


class FakeAddCTDtagToColumns(Transformer):
    valid_data_holders = ["LimsDataHolder"]

    @staticmethod
    def get_transformer_description() -> str:
        return (
            "Adds _CTD to all parameter columns. This is so that data can be compatible "
            "with the cdtvis bokeh visualization."
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        new_column = []
        for col in data_holder.data.columns:
            if "CTD" in col:
                new_column.append(col)
                continue
            if col.startswith(("Q_", "Q0_")) or f"Q_{col}" in data_holder.data.columns:
                new_column.append(f"{col}_CTD")
            else:
                new_column.append(col)
        data_holder.data.columns = new_column
