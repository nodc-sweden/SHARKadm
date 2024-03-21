from ..base import Transformer, DataHolderProtocol
import pandas as pd


class AddColumnsForAutomaticQC(Transformer):

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds columns for automatic qc (QC0_<PAR>)'

    def _transform(self, data_holder: DataHolderProtocol) -> None:

        new_data = {}
        new_column_order = []

        for col in list(data_holder.data.columns):
            if not col.startswith('Q_'):
                new_column_order.append(col)
                continue

            new_col_name = col.replace('Q_', 'Q0_')
            new_data[new_col_name] = ['00000'] * len(data_holder.data)  # Use a list of scalar values
            new_column_order.append(new_col_name)
            new_column_order.append(col)

        for col in list(data_holder.data.columns):
            if not col.startswith('Q_'):
                new_column_order.append(col)
                continue
            index = list(data_holder.data.columns).index(col)


