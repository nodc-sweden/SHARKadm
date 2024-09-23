from ._codes import _AddCodesLab


class AddEnglishSampleOrderer(_AddCodesLab):
    source_cols = ['sample_orderer_code', 'sample_orderer_name_sv']
    col_to_set = 'sample_orderer_name_en'
    lookup_key = 'english_name'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds sample orderer name in english'


class AddSwedishSampleOrderer(_AddCodesLab):
    source_cols = ['sample_orderer_code', 'sample_orderer_name_en']
    col_to_set = 'sample_orderer_name_sv'
    lookup_key = 'swedish_name'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds sample orderer name in swedish'

