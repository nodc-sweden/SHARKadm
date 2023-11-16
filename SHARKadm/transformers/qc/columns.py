from ..base import Transformer, DataHolderProtocol


class AddColumnsForAutomaticQC(Transformer):

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds columns for automatic qc (QC0_<PAR>)'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for col in list(data_holder.data.columns):
            if not col.startswith('Q_'):
                continue
            index = list(data_holder.data.columns).index(col)
            new_col_name = col.replace('Q_', 'Q0_')
            data_holder.data.insert(index, new_col_name, '00000')

