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

        # Create DataFrame from the dictionary with an index
        add_df = pd.DataFrame(new_data, index=data_holder.data.index)
        print()
        print()
        print()
        print(type(list(data_holder.data['datetime'])[0]))
        print(f'{new_column_order=}')

        # Concatenate the new DataFrame with the existing data
        data_holder.data = pd.concat([add_df, data_holder.data], axis=1)
        print(f'{data_holder.data.columns=}')
        data_holder.data.columns = new_column_order
        print(f'{data_holder.data.columns=}')
        print(data_holder.data['datetime'])
        print(list(data_holder.data['datetime'])[0])
        print(type(list(data_holder.data['datetime'])[0]))


        # new_data = dict()
        # new_column_order = []
        #
        # for col in list(data_holder.data.columns):
        #     if not col.startswith('Q_'):
        #         new_column_order.append(col)
        #         continue
        #     # index = list(data_holder.data.columns).index(col)
        #     new_col_name = col.replace('Q_', 'Q0_')
        #     new_data[new_col_name] = ['00000'] * len(data_holder.data)
        #     new_column_order.append(new_col_name)
        #     new_column_order.append(col)
        #
        # # data_holder.data.insert(index, new_col_name, '00000')
        #
        # add_df = pd.DataFrame(new_data, index=data_holder.data.index)
        # print(f'{add_df=}')
        #
        # # Concatenate the new DataFrame with the existing data
        # data_holder.data = pd.concat([add_df, data_holder.data], axis=1)
        # data_holder.data.columns = new_column_order

        # add_df = pd.DataFrame.from_dict(new_data)
        # data_holder.data = pd.concat([add_df, data_holder.data], axis=1)
        # data_holder.data.columns = new_column_order

