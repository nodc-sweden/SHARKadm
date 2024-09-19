from ._codes import _AddCodesProj


class AddEnglishProjectName(_AddCodesProj):
    source_cols = ['sample_project_code', 'sample_project_name_en']
    col_to_set = 'sample_project_name_sv'
    lookup_key = 'swedish_name'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds project name in swedish'


class AddSwedishProjectName(_AddCodesProj):
    source_cols = ['sample_project_code', 'sample_project_name_sv']
    col_to_set = 'sample_project_name_en'
    lookup_key = 'english_name'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds project name in english'
