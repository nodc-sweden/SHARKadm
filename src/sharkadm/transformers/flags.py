from .base import Transformer, DataHolderProtocol


class ConvertFlagsFromLIMStoSDN(Transformer):
    valid_data_holders = ['LimsDataHolder']
    mapping = {
        '': '1',
        'BLANK': '1',
        'A': '1',
        'E': '2',
        'S': '3',
        'B': '4',
        '<': '6',
        '>': '7',
        'R': '8',
        'M': '9',
    }

    @staticmethod
    def get_transformer_description() -> str:
        return f'Converts values in internal column "quality_flag" from LIMS schema to SeaDataNet schema'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        column = 'quality_flag'
        for value in set(data_holder.data[column]):
            stripped_value = value.strip()
            boolean = data_holder.data[column] == value
            data_holder.data.loc[boolean, column] = self.mapping.get(stripped_value, stripped_value)
        # Missing value
        boolean = data_holder.data['value'] == ''
        data_holder.data.loc[boolean, column] = '9'
