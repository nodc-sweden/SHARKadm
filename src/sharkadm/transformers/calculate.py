from .base import Transformer, DataHolderProtocol
import pandas as pd
from sharkadm import adm_logger


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
    def reported_cell_volume_um3(self) -> float | None:
        volume = self._df['reported_cell_volume_um3'].values[0]
        if not volume:
            return
        return float(volume)

    @property
    def reported_cell_volume_mm3(self) -> float | None:
        cell_volume_um3 = self.reported_cell_volume_um3
        if not cell_volume_um3:
            return
        return cell_volume_um3 * 10 ** -9

    @property
    def bvol_calculated_volume_um3(self) -> float | None:
        volume = self._df['bvol_calculated_volume_um3'].values[0]
        if not volume:
            return
        return float(volume)

    @property
    def bvol_calculated_volume_mm3(self) -> float | None:
        cell_volume_um3 = self.bvol_calculated_volume_um3
        if not cell_volume_um3:
            return
        return cell_volume_um3 * 10 ** -9

    @property
    def cell_volume_mm3(self) -> float | None:
        volume = self.bvol_calculated_volume_mm3
        if volume:
            return volume
        return self.reported_cell_volume_mm3


class Calculate(Transformer):
    occurrence_id_col = 'custom_occurrence_id'
    col_to_set_abundance = 'CALC-Abundance'
    col_to_set_biovolume_concentration = 'CALC-Biovolume concentration'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adding calculated columns: {Calculate.col_to_set_abundance}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        series_to_add = []
        for _id, df in data_holder.data.groupby(self.occurrence_id_col):
            occ = Occurrence(df)

            counted = occ.counted
            coefficient = occ.coefficient

            if not any([counted, coefficient]):
                adm_logger.log_transformation(f'Could not calculate anything. Missing counted and coefficient.', level=adm_logger.WARNING)
                continue
            elif not counted:
                adm_logger.log_transformation(f'Could not calculate anything. Missing counted.', level=adm_logger.WARNING)
                continue
            elif not coefficient:
                adm_logger.log_transformation(f'Could not calculate anything. Missing coefficient.', level=adm_logger.WARNING)
                continue

            series = df.loc[df.index[0]].squeeze()
            calc_abundance = occ.counted * occ.coefficient
            series[self.col_to_set_abundance] = str(calc_abundance)

            cell_volume_mm3 = occ.cell_volume_mm3
            if cell_volume_mm3:
                calc_bio_volume = cell_volume_mm3 * calc_abundance
                series[self.col_to_set_biovolume_concentration] = str(calc_bio_volume)
            else:
                adm_logger.log_transformation(f'Could not calculate anything. Missing coefficient.',
                                              level=adm_logger.WARNING)

            series_to_add.append(series)

        if not series_to_add:
            adm_logger.log_transformation('No calculations made', level=adm_logger.DEBUG)

        adm_logger.log_transformation(f'Adding {len(series_to_add)} calculated rows.', level=adm_logger.DEBUG)
        adm_logger.data = pd.concat([data_holder.data, pd.DataFrame(series_to_add)])




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