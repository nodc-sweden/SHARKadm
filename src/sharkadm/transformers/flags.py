from .base import Transformer, DataHolderProtocol


class ConvertFlagsFromLIMStoSDN(Transformer):
    valid_data_holders = ['LimsDataHolder']
    mapping = {
        '': '1',
        'A': '1',
        'S': '2',
        'B': '4',
        'R': '5',
        '<': '6',
        '>': '7',
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
