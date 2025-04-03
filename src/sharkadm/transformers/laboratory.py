from ._codes import _AddCodesLab


class AddSwedishSamplingLaboratory(_AddCodesLab):
    source_cols = [
        "sampling_laboratory_code",
        "sampling_laboratory_code_phyche",
        "sampling_laboratory_name_en",
    ]
    col_to_set = "sampling_laboratory_name_sv"
    lookup_key = "swedish_name"

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds sampling laboratory name in swedish"


class AddEnglishSamplingLaboratory(_AddCodesLab):
    source_cols = [
        "sampling_laboratory_code",
        "sampling_laboratory_code_phyche",
        "sampling_laboratory_name_sv",
    ]
    col_to_set = "sampling_laboratory_name_en"
    lookup_key = "english_name"

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds sampling laboratory name in english"


class AddEnglishAnalyticalLaboratory(_AddCodesLab):
    source_cols = ["analytical_laboratory_code", "analytical_laboratory_name_sv"]
    col_to_set = "analytical_laboratory_name_en"
    lookup_key = "english_name"

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds analytical laboratory name in english"


class AddSwedishAnalyticalLaboratory(_AddCodesLab):
    source_cols = ["analytical_laboratory_code", "analytical_laboratory_name_en"]
    col_to_set = "analytical_laboratory_name_sv"
    lookup_key = "swedish_name"

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds analytical laboratory name in swedish"
