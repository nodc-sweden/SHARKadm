from .base import Transformer, DataHolderProtocol
import pandas as pd


class Occurrence:

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    @property
    def counted(self):
        return int(list(self._df[self._df['parameter'] == '# counted']['value'])[0])

    @property
    def coefficient(self):
        return int(self._df['coefficient'].values[0])

    @property
    def reported_cell_volume_um3(self):
        volume = self._df['reported_cell_volume_um3'].values[0]
        if not volume:
            return
        return int(volume)

    @property
    def reported_cell_volume_mm3(self):
        cell_volume_um3 = self.reported_cell_volume_um3
        if not cell_volume_um3:
            return
        return cell_volume_um3 * 10 ** -9


class Calculate(Transformer):
    occurrence_id_col = 'custom_occurrence_id'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Calculate abundance and other stuf'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        series_to_add = []
        for _id, df in data_holder.data.groupby(self.occurrence_id_col):
            occ = Occurrence(df)
            series = df.loc[df.index[0]].squeeze()

            calc_abundance = occ.counted * occ.coefficient
            series['CALC_Abundance'] = str(calc_abundance)
            series_to_add.append(series)


            # Från JAVA-kod biovol
            # class BvolObject {
            #
            # private String referenceList = "";
            # private String scientificName = "";
            # private String aphiaId = "";
            # private String sizeClass = "";
            # private String trophicType = "";
            # private String calculatedVolume = "";
            # private String calculatedCarbon = "";