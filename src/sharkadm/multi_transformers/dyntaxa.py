from sharkadm import transformers
from sharkadm.multi_transformers.base import PolarsMultiTransformer


class DyntaxaPolars(PolarsMultiTransformer):
    _transformers = (
        transformers.PolarsAddReportedDyntaxaId,
        transformers.PolarsAddReportedScientificNameDyntaxaId,
        transformers.PolarsAddDyntaxaScientificName,
        # transformers.PolarsAddDyntaxaTranslatedScientificNameDyntaxaId,
        # Ska ej vara med
        transformers.PolarsAddTaxonRanks,
        transformers.PolarsAddDyntaxaId,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs the following transformations related to Dyntaxa:"]
        for trans in DyntaxaPolars._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)
