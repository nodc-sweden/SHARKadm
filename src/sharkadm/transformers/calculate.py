from .base import Transformer, DataHolderProtocol
import pandas as pd
from sharkadm import adm_logger


class PhytoplanktonOccurrence:

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    @property
    def counted(self):
        df = self._df[self._df['parameter'] == '# counted']
        if not len(df):
            return
        return int(list(df['value'])[0])

    @property
    def coefficient(self):
        return float(self._df['coefficient'].values[0])

    @property
    def reported_cell_volume_um3(self) -> float | None:
        if 'reported_cell_volume_um3' not in self._df:
            return
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


class ZooOccurrence:

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    @property
    def counted(self):
        return int(list(self._df[self._df['parameter'] == '# counted']['value'])[0])

    @property
    def sampler_area_cm2(self):
        return int(self._df['sampler_area_cm2'].values[0])

    @property
    def wet_weight(self):
        df = self._df[self._df['parameter'] == 'Wet weight']
        if not len(df):
            return None
        print('HITTAT')
        return float(list(self._df[self._df['parameter'] == 'Wet weight']['value'])[0])

    # @property
    # def reported_cell_volume_um3(self) -> float | None:
    #     volume = self._df['reported_cell_volume_um3'].values[0]
    #     if not volume:
    #         return
    #     return float(volume)
    #
    # @property
    # def reported_cell_volume_mm3(self) -> float | None:
    #     cell_volume_um3 = self.reported_cell_volume_um3
    #     if not cell_volume_um3:
    #         return
    #     return cell_volume_um3 * 10 ** -9
    #
    # @property
    # def bvol_calculated_volume_um3(self) -> float | None:
    #     volume = self._df['bvol_calculated_volume_um3'].values[0]
    #     if not volume:
    #         return
    #     return float(volume)
    #
    # @property
    # def bvol_calculated_volume_mm3(self) -> float | None:
    #     cell_volume_um3 = self.bvol_calculated_volume_um3
    #     if not cell_volume_um3:
    #         return
    #     return cell_volume_um3 * 10 ** -9
    #
    # @property
    # def cell_volume_mm3(self) -> float | None:
    #     volume = self.bvol_calculated_volume_mm3
    #     if volume:
    #         return volume
    #     return self.reported_cell_volume_mm3


class old_CalculatePhytoplankton(Transformer):
    col_must_exist = 'reported_value'
    occurrence_id_col = 'custom_occurrence_id'
    # col_to_set_abundance = 'CALC-Abundance'
    col_to_set_abundance = 'Abundance'
    col_to_set_abundance_unit = 'ind/m2'
    # col_to_set_biovolume_concentration = 'CALC-Biovolume concentration'
    col_to_set_biovolume_concentration = 'Biovolume concentration'
    col_to_set_biovolume_concentration_unit = 'mm3/l'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adding calculated columns: {old_CalculatePhytoplankton.col_to_set_abundance}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.col_must_exist not in data_holder.data:
            adm_logger.log_transformation(f'Could not calculate anything. Missing {self.col_must_exist} column',
                                          level=adm_logger.ERROR)
            return
        series_to_add = []
        for _id, df in data_holder.data.groupby(self.occurrence_id_col):
            occ = PhytoplanktonOccurrence(df)

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

            calc_abundance = counted * coefficient

            index = df.loc[df['parameter'] == 'Abundance'].index
            data_holder.data.loc[index, 'value'] = calc_abundance

            # series = df.loc[df.index[0]].squeeze()
            # series['parameter'] = self.col_to_set_abundance
            # series['value'] = str(calc_abundance)
            # series['unit'] = self.col_to_set_abundance_unit
            # series['calc_by_dc'] = 'Y'
            # series_to_add.append(series)

            # cell_volume_mm3 = occ.reported_cell_volume_mm3
            # if cell_volume_mm3:
            #     calc_bio_volume = cell_volume_mm3 * calc_abundance
            #     series['parameter'] = self.col_to_set_biovolume_concentration
            #     series['value'] = str(calc_bio_volume)
            #     series['unit'] = self.col_to_set_biovolume_concentration_unit
            #     series['calc_by_dc'] = 'Y'
            #     series_to_add.append(series)
            # else:
            #     adm_logger.log_transformation(f'Could not calculate anything. Missing coefficient.',
            #                                   level=adm_logger.WARNING)

        # if not series_to_add:
        #     adm_logger.log_transformation('No calculations made', level=adm_logger.DEBUG)
        #
        # adm_logger.log_transformation(f'Adding {len(series_to_add)} calculated rows.', level=adm_logger.DEBUG)
        # data_holder.data = pd.concat([data_holder.data, pd.DataFrame(series_to_add)])


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


class CalculateAbundance(Transformer):
    count_col = 'COPY_VARIABLE.# counted.ind/analysed sample fraction'
    coef_col = 'coefficient'
    col_must_exist = 'reported_value'
    parameter = 'Abundance'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Replacing {CalculateAbundance.parameter} with calculated value'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.col_must_exist not in data_holder.data:
            adm_logger.log_transformation(f'Could not calculate anything. Missing {self.col_must_exist} column',
                                          level=adm_logger.ERROR)
            return

        data_holder.data['calc_by_dc'] = ''
        boolean = (data_holder.data[self.count_col] != '') & (data_holder.data[self.coef_col] != '') & (data_holder.data['parameter'] == self.parameter)
        data_holder.data.loc[boolean, 'value'] = (data_holder.data.loc[boolean, self.count_col].astype(float) * data_holder.data.loc[boolean, self.coef_col].astype(float)).round(1)
        data_holder.data.loc[boolean, 'calc_by_dc'] = 'Y'


class old_CalculateZooplankton(Transformer):
    occurrence_id_col = 'custom_occurrence_id'
    # col_to_set_abundance = 'CALCULATED_Abundance'
    col_to_set_abundance = 'Abundance'
    col_to_set_abundance_unit = 'ind/m2'

    col_to_set_wet_weight_per_area = 'Wet weight/area'
    col_to_set_wet_weight_per_area_unit = 'g wet weight/m2'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adding calculated columns: {old_CalculateZooplankton.col_to_set_abundance}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        series_to_add = []
        for _id, df in data_holder.data.groupby(self.occurrence_id_col):
            occ = ZooOccurrence(df)

            counted = occ.counted
            sampler_area_cm2 = occ.sampler_area_cm2

            if not any([counted, sampler_area_cm2]):
                adm_logger.log_transformation(f'Could not calculate anything. Missing counted and sampler_area_cm2.', level=adm_logger.WARNING)
                continue
            elif not counted:
                adm_logger.log_transformation(f'Could not calculate anything. Missing counted.', level=adm_logger.WARNING)
                continue
            elif not sampler_area_cm2:
                adm_logger.log_transformation(f'Could not calculate anything. Missing sampler_area_cm2.', level=adm_logger.WARNING)
                continue

            #############################################################
            series = df.loc[df.index[0]].squeeze()
            calc_abundance = occ.counted / occ.sampler_area_cm2 * 10000.0
            series['parameter'] = self.col_to_set_abundance
            series['value'] = str(calc_abundance)
            series['unit'] = self.col_to_set_abundance_unit
            series['calc_by_dc'] = 'Y'
            series_to_add.append(series)
            #############################################################
            series = df.loc[df.index[0]].squeeze()
            wet_weight = occ.wet_weight
            if wet_weight:
                calc_wet_weight = occ.counted / occ.wet_weight * 10000.0
                series['parameter'] = self.col_to_set_wet_weight_per_area
                series['value'] = str(calc_wet_weight)
                series['unit'] = self.col_to_set_wet_weight_per_area_unit
                series['calc_by_dc'] = 'Y'
                series_to_add.append(series)
            #############################################################

        if not series_to_add:
            adm_logger.log_transformation('No calculations made', level=adm_logger.DEBUG)

        adm_logger.log_transformation(f'Adding {len(series_to_add)} calculated rows.', level=adm_logger.DEBUG)
        data_holder.data = pd.concat([data_holder.data, pd.DataFrame(series_to_add)])


# class CopyCalculated(Transformer):
#     source_cols = ['CALCULATED_Abundance']
#     target_cols = ['Abundance']
#
#     @staticmethod
#     def get_transformer_description() -> str:
#         return f'Copies columns {CopyCalculated.source_cols} to {CopyCalculated.target_cols}'
#
#     def _transform(self, data_holder: DataHolderProtocol) -> None:
#         for scol, tcol in zip(self.source_cols, self.target_cols):
#             data_holder.data[tcol] = data_holder.data[scol]
