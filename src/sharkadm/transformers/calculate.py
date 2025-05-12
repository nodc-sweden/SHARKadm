import numpy as np

from .base import Transformer, DataHolderProtocol
import pandas as pd
from sharkadm import adm_logger

try:
    from nodc_bvol import get_bvol_nomp_object

    _nomp = get_bvol_nomp_object()
except ModuleNotFoundError as e:
    _nomp = None
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. You need to install this dependency if you want to use this module.',
        level=adm_logger.WARNING)


#
# class PhytoplanktonOccurrence:
#
#     def __init__(self, df: pd.DataFrame) -> None:
#         self._df = df
#
#     @property
#     def counted(self):
#         df = self._df[self._df['parameter'] == '# counted']
#         if not len(df):
#             return
#         return int(list(df['value'])[0])
#
#     @property
#     def coefficient(self):
#         return float(self._df['coefficient'].values[0])
#
#     @property
#     def reported_cell_volume_um3(self) -> float | None:
#         if 'reported_cell_volume_um3' not in self._df:
#             return
#         volume = self._df['reported_cell_volume_um3'].values[0]
#         if not volume:
#             return
#         return float(volume)
#
#     @property
#     def reported_cell_volume_mm3(self) -> float | None:
#         cell_volume_um3 = self.reported_cell_volume_um3
#         if not cell_volume_um3:
#             return
#         return cell_volume_um3 * 10 ** -9
#
#
# class ZooOccurrence:
#
#     def __init__(self, df: pd.DataFrame) -> None:
#         self._df = df
#
#     @property
#     def counted(self):
#         return int(list(self._df[self._df['parameter'] == '# counted']['value'])[0])
#
#     @property
#     def sampler_area_cm2(self):
#         return int(self._df['sampler_area_cm2'].values[0])
#
#     @property
#     def wet_weight(self):
#         df = self._df[self._df['parameter'] == 'Wet weight']
#         if not len(df):
#             return None
#         print('HITTAT')
#         return float(list(self._df[self._df['parameter'] == 'Wet weight']['value'])[0])
#
#     # @property
#     # def reported_cell_volume_um3(self) -> float | None:
#     #     volume = self._df['reported_cell_volume_um3'].values[0]
#     #     if not volume:
#     #         return
#     #     return float(volume)
#     #
#     # @property
#     # def reported_cell_volume_mm3(self) -> float | None:
#     #     cell_volume_um3 = self.reported_cell_volume_um3
#     #     if not cell_volume_um3:
#     #         return
#     #     return cell_volume_um3 * 10 ** -9
#     #
#     # @property
#     # def bvol_calculated_volume_um3(self) -> float | None:
#     #     volume = self._df['bvol_calculated_volume_um3'].values[0]
#     #     if not volume:
#     #         return
#     #     return float(volume)
#     #
#     # @property
#     # def bvol_calculated_volume_mm3(self) -> float | None:
#     #     cell_volume_um3 = self.bvol_calculated_volume_um3
#     #     if not cell_volume_um3:
#     #         return
#     #     return cell_volume_um3 * 10 ** -9
#     #
#     # @property
#     # def cell_volume_mm3(self) -> float | None:
#     #     volume = self.bvol_calculated_volume_mm3
#     #     if volume:
#     #         return volume
#     #     return self.reported_cell_volume_mm3
#
#
# class old_CalculatePhytoplankton(Transformer):
#     col_must_exist = 'reported_value'
#     occurrence_id_col = 'custom_occurrence_id'
#     # col_to_set_abundance = 'CALC-Abundance'
#     col_to_set_abundance = 'Abundance'
#     col_to_set_abundance_unit = 'ind/m2'
#     # col_to_set_biovolume_concentration = 'CALC-Biovolume concentration'
#     col_to_set_biovolume_concentration = 'Biovolume concentration'
#     col_to_set_biovolume_concentration_unit = 'mm3/l'
#
#     @staticmethod
#     def get_transformer_description() -> str:
#         return f'Adding calculated columns: {old_CalculatePhytoplankton.col_to_set_abundance}'
#
#     def _transform(self, data_holder: DataHolderProtocol) -> None:
#         if self.col_must_exist not in data_holder.data:
#             adm_logger.log_transformation(f'Could not calculate anything. Missing {self.col_must_exist} column',
#                                           level=adm_logger.ERROR)
#             return
#         series_to_add = []
#         for _id, df in data_holder.data.groupby(self.occurrence_id_col):
#             occ = PhytoplanktonOccurrence(df)
#
#             counted = occ.counted
#             coefficient = occ.coefficient
#
#             if not any([counted, coefficient]):
#                 adm_logger.log_transformation(f'Could not calculate anything. Missing counted and coefficient.', level=adm_logger.WARNING)
#                 continue
#             elif not counted:
#                 adm_logger.log_transformation(f'Could not calculate anything. Missing counted.', level=adm_logger.WARNING)
#                 continue
#             elif not coefficient:
#                 adm_logger.log_transformation(f'Could not calculate anything. Missing coefficient.', level=adm_logger.WARNING)
#                 continue
#
#             calc_abundance = counted * coefficient
#
#             index = df.loc[df['parameter'] == 'Abundance'].index
#             data_holder.data.loc[index, 'value'] = calc_abundance

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

class ColCalculateAbundance(Transformer):
    count_col = 'COPY_VARIABLE.# counted.ind/analysed sample fraction'
    abundance_col = 'COPY_VARIABLE.Abundance.ind/l or 100 um pieces/l'
    coef_col = 'coefficient'
    # col_must_exist = 'reported_value'
    # parameter = 'Abundance'
    # col_to_set = 'calculated_value'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Calculating {CalculateAbundance.parameter}. Setting value to column {CalculateAbundance.col_to_set}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data['reported_abundance'] = data_holder.data[self.abundance_col]
        boolean = (data_holder.data[self.count_col] != '') & (data_holder.data[self.coef_col] != '')
        data_holder.data.loc[boolean, 'calculated_abundance'] = (
                    data_holder.data.loc[boolean, self.count_col].astype(float) * data_holder.data.loc[boolean, self.coef_col].astype(float)).round(1)

        max_boolean = data_holder.data.loc[boolean, 'calculated_abundance'] > (
                    data_holder.data.loc[boolean, self.abundance_col].astype(float) * 2)
        min_boolean = data_holder.data.loc[boolean, 'calculated_abundance'] < (
                    data_holder.data.loc[boolean, self.abundance_col].astype(float) * 0.5)
        out_of_range_boolean = max_boolean | min_boolean
        for i, row in data_holder.data[out_of_range_boolean].iterrows():
            # Log here
            pass
        data_holder.data.loc[out_of_range_boolean, self.abundance_col] = data_holder.data.loc[out_of_range_boolean, 'calculated_abundance']
        data_holder.data.loc[out_of_range_boolean, 'calc_by_dc_abundance'] = 'Y'


class ColCalculateBiovolume(Transformer):
    abundance_col = 'COPY_VARIABLE.Abundance.ind/l or 100 um pieces/l'
    reported_cell_volume_col = 'reported_cell_volume_um3'
    calculated_cell_volume_col = 'bvol_cell_volume_um3'
    # size_class_col = 'size_class'
    # aphia_id_col = 'aphia_id'
    # reported_cell_volume_col = 'reported_cell_volume_um3'
    # col_must_exist = 'reported_value'
    # parameter = 'Biovolume concentration'
    # col_to_set = 'calculated_value'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Calculating {CalculateBiovolume.parameter}. Setting value to column {CalculateBiovolume.col_to_set}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data['cell_volume'] = data_holder.data[self.reported_cell_volume_col]
        bool_cell = data_holder.data[self.calculated_cell_volume_col] != ''
        data_holder.data.loc[bool_cell, 'cell_volume'] = data_holder.data.loc[bool_cell, self.calculated_cell_volume_col]

        data_holder.data['COPY_VARIABLE.Biovolume concentration.mm3/l'] = data_holder.data[self.abundance_col].astype(float) * data_holder.data['cell_volume'].astype(float)
        data_holder.data['calculated_biovolume'] = data_holder.data['COPY_VARIABLE.Biovolume concentration.mm3/l']
        data_holder.data['calc_by_dc_biovolume'] = 'Y'


class ColCalculateCarbon(Transformer):
    abundance_col = 'COPY_VARIABLE.Abundance.ind/l or 100 um pieces/l'
    carbon_col = 'COPY_VARIABLE.Carbon concentration.ugC/l'
    carbon_per_volume_col = 'bvol_carbon_per_volume'

    # size_class_col = 'size_class'
    # aphia_id_col = 'aphia_id'
    # reported_cell_volume_col = 'reported_cell_volume_um3'
    # col_must_exist = 'reported_value'
    # parameter = 'Carbon concentration'
    # col_to_set = 'calculated_value'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Calculating {CalculateCarbon.parameter}. Setting value to column {CalculateCarbon.col_to_set}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data['reported_carbon'] = data_holder.data[self.carbon_col]
        bool_carbon = data_holder.data[self.carbon_per_volume_col] != ''

        data_holder.data['calculated_carbon'] = data_holder.data.loc[bool_carbon, self.abundance_col].astype(float) * data_holder.data.loc[
            bool_carbon, self.carbon_per_volume_col].astype(float)
        max_boolean = data_holder.data.loc[bool_carbon, 'calculated_carbon'] > (data_holder.data.loc[bool_carbon, self.carbon_col].astype(float) * 2)
        min_boolean = data_holder.data.loc[bool_carbon, 'calculated_carbon'] < (data_holder.data.loc[bool_carbon, self.carbon_col].astype(float) * 0.5)
        out_of_range_boolean = max_boolean | min_boolean
        for i, row in data_holder.data[out_of_range_boolean].iterrows():
            # Log here
            pass
        data_holder.data.loc[out_of_range_boolean, self.abundance_col] = data_holder.data.loc[out_of_range_boolean, 'calculated_carbon']
        data_holder.data.loc[out_of_range_boolean, 'calc_by_dc_carbon'] = 'Y'
        data_holder.data['carbon_unit'] = 'ugC/l'


class FixReportedValueAfterCalculations(Transformer):

    @staticmethod
    def get_transformer_description() -> str:
        return f'Setting reported_value after calculations'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if data_holder.data_structure != 'row':
            adm_logger.log_transformation(
                f'Could not fix reported value. Data is not in row format',
                level=adm_logger.ERROR)
            return
        data_holder.data['calc_by_dc'] = ''
        data_holder.data['reported_value'] = ''

        self._fix_abundance(data_holder)
        self._fix_carbon(data_holder)

    @staticmethod
    def _fix_abundance(data_holder: DataHolderProtocol) -> None:
        boolean = (data_holder.data['parameter'] == 'Abundance') & (data_holder.data['calc_by_dc_abundance'] == 'Y')
        data_holder.data.loc[boolean, 'calc_by_dc'] = 'Y'
        data_holder.data.loc[boolean, 'reported_value'] = data_holder.data.loc[boolean, 'reported_abundance']

    @staticmethod
    def _fix_carbon(data_holder: DataHolderProtocol) -> None:
        boolean = (data_holder.data['parameter'] == 'Carbon concentration') & (data_holder.data['calc_by_dc_carbon'] == 'Y')
        data_holder.data.loc[boolean, 'calc_by_dc'] = 'Y'
        data_holder.data.loc[boolean, 'reported_value'] = data_holder.data.loc[boolean, 'reported_carbon']


class CalculateAbundance(Transformer):
    count_col = 'COPY_VARIABLE.# counted.ind/analysed sample fraction'
    coef_col = 'coefficient'
    col_must_exist = 'reported_value'
    parameter = 'Abundance'
    col_to_set = 'calculated_value'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Calculating {CalculateAbundance.parameter}. Setting value to column {CalculateAbundance.col_to_set}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.col_must_exist not in data_holder.data:
            adm_logger.log_transformation(
                f'Could not calculate {CalculateAbundance.parameter}. Missing {self.col_must_exist} column',
                level=adm_logger.ERROR)
            return
        boolean = (data_holder.data[self.count_col] != '') & (data_holder.data[self.coef_col] != '') & (
                    data_holder.data['parameter'] == self.parameter)
        data_holder.data.loc[boolean, 'calculated_value'] = (
                    data_holder.data.loc[boolean, self.count_col].astype(float) * data_holder.data.loc[
                boolean, self.coef_col].astype(float)).round(1)
        self._check_calculated_value(data_holder)

    def _check_calculated_value(self, data_holder: DataHolderProtocol):
        abundance_boolean = data_holder.data['parameter'] == self.parameter
        df = data_holder.data.loc[abundance_boolean]
        reported = df['reported_value'].apply(lambda x: np.nan if x == '' else float(x))
        calculated = df['calculated_value']
        max_boolean = calculated > (reported * 2)
        min_boolean = calculated < (reported * 0.5)
        boolean = max_boolean | min_boolean
        red_df = df[boolean]
        for i, row in red_df.iterrows():
            adm_logger.log_transformation(
                f"Calculated {self.parameter.lower()} differs to much from reported value. Setting calculated value {row['reported_value']} -> {row['calculated_value']} (row number {row['row_number']})",
                level=adm_logger.INFO)

        data_holder.data.loc[red_df.index, 'value'] = data_holder.data.loc[red_df.index, 'calculated_value']
        data_holder.data.loc[red_df.index, 'calc_by_dc'] = 'Y'


class CalculateBiovolume(Transformer):
    size_class_col = 'size_class'
    aphia_id_col = 'aphia_id'
    reported_cell_volume_col = 'reported_cell_volume_um3'
    col_must_exist = 'reported_value'
    parameter = 'Biovolume concentration'
    col_to_set = 'calculated_value'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Calculating {CalculateBiovolume.parameter}. Setting value to column {CalculateBiovolume.col_to_set}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if 'custom_occurrence_id' not in data_holder.data:
            adm_logger.log_transformation(
                f'Could not calculate {CalculateBiovolume.parameter}. Missing custom_occurrence_id column',
                level=adm_logger.ERROR)
            return
        # series_to_add = []
        for (aphia_id, size_class, occurrence_id), df in data_holder.data.groupby(
                ['bvol_aphia_id', 'bvol_size_class', 'custom_occurrence_id']):
            if not aphia_id:
                adm_logger.log_transformation(
                    f'Could not calculate {CalculateBiovolume.parameter}. Missing bvol_aphia_id',
                    level=adm_logger.WARNING)
                continue
            if not size_class:
                adm_logger.log_transformation(
                    f'Could not calculate {CalculateBiovolume.parameter}. Missing bvol_size_class',
                    level=adm_logger.WARNING)
                continue
            cell_volume = self._get_bvol_cell_volume(aphia_id, size_class)
            # if cell_volume:
            #     adm_logger.log_transformation(
            #         f'Using cell volume from nomp for aphia_id="{aphia_id}" and size_class="{size_class}"',
            #         level=adm_logger.DEBUG)
            if not cell_volume:
                cell_volume = self._get_reported_cell_volume(df, aphia_id, size_class)
                if cell_volume:
                    adm_logger.log_transformation(
                        f'Using reported cell volume for aphia_id="{aphia_id}" and size_class="{size_class}"',
                        level=adm_logger.DEBUG)
            if not cell_volume:
                adm_logger.log_transformation(
                    f'Could not calculate {CalculateBiovolume.parameter}. No cell volume in nomp or in data for aphia_id="{aphia_id}" and size_class="{size_class}"',
                    level=adm_logger.WARNING)
                continue

            abundance = list(df.loc[df['parameter'] == 'Abundance']['value'])[0]
            index_list = list(df.loc[df['parameter'] == self.parameter].index)
            bio_volume = float(abundance) * float(cell_volume)
            if index_list:
                index_to_set = index_list[0]
                data_holder.data.at[index_to_set, self.col_to_set] = bio_volume
            else:
                series = df.loc[df['parameter'] == 'Abundance'].squeeze(axis=0)
                series['parameter'] = self.parameter
                series[self.col_to_set] = bio_volume
                series['unit'] = 'mm3/l'
                # series_to_add.append(series)
                data_holder.data = pd.concat([data_holder.data, series.to_frame().T])
        # if series_to_add:
        # This is slower...
        #     data_holder.data = pd.concat([data_holder.data] + series_to_add, axis=1)
        self._check_calculated_value(data_holder)

    def _check_calculated_value(self, data_holder: DataHolderProtocol):
        abundance_boolean = data_holder.data['parameter'] == self.parameter
        df = data_holder.data.loc[abundance_boolean]
        reported = df['reported_value'].apply(lambda x: np.nan if x == '' else float(x))
        calculated = df['calculated_value']
        max_boolean = calculated > (reported * 2)
        min_boolean = calculated < (reported * 0.5)
        boolean = max_boolean | min_boolean
        red_df = df[boolean]
        for i, row in red_df.iterrows():
            adm_logger.log_transformation(
                f"Calculated {self.parameter.lower()} differs to much from reported value. Setting calculated value {row['reported_value']} -> {row['calculated_value']} (row number {row['row_number']})",
                level=adm_logger.INFO)

        data_holder.data.loc[red_df.index, 'value'] = data_holder.data.loc[red_df.index, 'calculated_value']
        data_holder.data.loc[red_df.index, 'calc_by_dc'] = 'Y'

    def _get_bvol_cell_volume(self, aphia_id: int | str, size_class: int | str) -> float | None:
        info = _nomp.get_info(AphiaID=str(aphia_id), SizeClassNo=str(size_class))
        use_volume = None
        if not info:
            pass
            # adm_logger.log_transformation(
            #     f'Could not calculate {CalculateBiovolume.parameter} with aphia_id="{aphia_id}" and size_class="{size_class}"',
            #     level=adm_logger.WARNING)
        else:
            volume = info.get('Calculated_volume_µm3')  # This is in um^3
            if volume is None:
                pass
                # adm_logger.log_transformation(
                #     f'Could not calculate {CalculateBiovolume.parameter}. No Calculated_volume_µm3 found in nomp list for aphia_id="{aphia_id}" and size_class="{size_class}"',
                #     level=adm_logger.WARNING)
            else:
                use_volume = volume.replace(',', '.')
        if not use_volume:
            return
        return float(use_volume) * 10e-9

    def _get_reported_cell_volume(self, df: pd.DataFrame, aphia_id: int, size_class: int) -> float | None:
        values = set(df[self.reported_cell_volume_col])
        if len(values) != 1:
            pass
            # adm_logger.log_transformation(
            #     f'Could not calculate {CalculateBiovolume.parameter}. {self.reported_cell_volume_col} has several values for aphia_id="{aphia_id}" and size_class="{size_class}"',
            #     level=adm_logger.WARNING)
            return
        return float(values.pop()) * 10e-9


class CalculateCarbon(Transformer):
    size_class_col = 'size_class'
    aphia_id_col = 'aphia_id'
    reported_cell_volume_col = 'reported_cell_volume_um3'
    col_must_exist = 'reported_value'
    parameter = 'Carbon concentration'
    col_to_set = 'calculated_value'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Calculating {CalculateCarbon.parameter}. Setting value to column {CalculateCarbon.col_to_set}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if 'custom_occurrence_id' not in data_holder.data:
            adm_logger.log_transformation(
                f'Could not calculate {CalculateCarbon.parameter}. Missing custom_occurrence_id column',
                level=adm_logger.ERROR)
            return
        for (aphia_id, size_class, occurrence_id), df in data_holder.data.groupby(
                ['bvol_aphia_id', 'bvol_size_class', 'custom_occurrence_id']):
            if not aphia_id:
                adm_logger.log_transformation(
                    f'Could not calculate {CalculateCarbon.parameter}. Missing aphia_id',
                    level=adm_logger.WARNING)
                continue
            if not size_class:
                adm_logger.log_transformation(
                    f'Could not calculate {CalculateCarbon.parameter}. Missing size_class',
                    level=adm_logger.WARNING)
                continue
            carbon = self._get_carbon_per_unit_volume(aphia_id, size_class)
            if not carbon:
                adm_logger.log_transformation(
                    f'Could not calculate {CalculateCarbon.parameter}. No carbon per unit volume info in nomp for aphia_id="{aphia_id}" and size_class="{size_class}"',
                    level=adm_logger.WARNING)
                continue

            abundance = list(df.loc[df['parameter'] == 'Abundance']['value'])[0]
            index_list = list(df.loc[df['parameter'] == self.parameter].index)
            carbon_concentration = float(abundance) * float(carbon)
            if index_list:
                index_to_set = index_list[0]
                data_holder.data.at[index_to_set, self.col_to_set] = carbon_concentration
            else:
                series = df.loc[df['parameter'] == 'Abundance'].squeeze(axis=0)
                series['parameter'] = self.parameter
                series[self.col_to_set] = carbon_concentration
                series['unit'] = 'mm3/l'
                data_holder.data = pd.concat([data_holder.data, series.to_frame().T])

            # abundance = list(df.loc[df['parameter'] == 'Abundance']['value'])[0]
            # index_to_set = list(df.loc[df['parameter'] == self.parameter].index)[0]
            # carbon_concentration = abundance * carbon
            # data_holder.data.at[index_to_set, self.col_to_set] = carbon_concentration
            self._check_calculated_value(data_holder)

    def _check_calculated_value(self, data_holder: DataHolderProtocol):
        abundance_boolean = data_holder.data['parameter'] == self.parameter
        df = data_holder.data.loc[abundance_boolean]
        reported = df['reported_value'].apply(lambda x: np.nan if x == '' else float(x))
        calculated = df['calculated_value']
        max_boolean = calculated > (reported * 2)
        min_boolean = calculated < (reported * 0.5)
        boolean = max_boolean | min_boolean
        red_df = df[boolean]
        for i, row in red_df.iterrows():
            adm_logger.log_transformation(
                f"Calculated {self.parameter.lower()} differs to much from reported value. Setting calculated value {row['reported_value']} -> {row['calculated_value']} (row number {row['row_number']})",
                level=adm_logger.INFO)

        data_holder.data.loc[red_df.index, 'value'] = data_holder.data.loc[red_df.index, 'calculated_value']
        data_holder.data.loc[red_df.index, 'calc_by_dc'] = 'Y'

    def _get_carbon_per_unit_volume(self, aphia_id: int | str, size_class: int | str) -> float | None:
        info = _nomp.get_info(AphiaID=str(aphia_id), SizeClassNo=str(size_class))
        use_volume = None
        if not info:
            adm_logger.log_transformation(
                f'Could not calculate {CalculateCarbon.parameter} with aphia_id="{aphia_id}" and size_class="{size_class}"',
                level=adm_logger.WARNING)
        else:
            volume = info.get('Calculated_Carbon_pg/counting_unit')
            if volume is None:
                adm_logger.log_transformation(
                    f'Could not calculate {CalculateCarbon.parameter}. No Calculated_Carbon_pg/counting_unit found in nomp list for aphia_id="{aphia_id}" and size_class="{size_class}"',
                    level=adm_logger.WARNING)
            else:
                use_volume = volume.replace(',', '.')
        if not use_volume:
            return
        return float(use_volume) / 1_000_000


class old_CleanupCalculations(Transformer):

    @staticmethod
    def get_transformer_description() -> str:
        return f'Compares reported and calculated values, setting value column and gives warnings if needed'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for col in ['reported_value', 'calculated_value', 'parameter']:
            if col not in data_holder.data:
                adm_logger.log_transformation(
                    f'Could not cleanup calculations. Missing column {col}',
                    level=adm_logger.ERROR)
                return

        if 'original_calculated_value' in data_holder.data:
            adm_logger.log_transformation(
                f'CleanupCalculations already made. You dont want to do this again!',
                level=adm_logger.WARNING)
            return

        data_holder.data['original_reported_value'] = data_holder.data['reported_value']
        data_holder.data['original_reported_unit'] = data_holder.data['reported_unit']
        data_holder.data['original_reported_parameter'] = data_holder.data['reported_parameter']
        data_holder.data['original_calculated_value'] = data_holder.data['calculated_value']

        abundance_boolean = data_holder.data['parameter'] == 'Abundance'
        df = data_holder.data.loc[abundance_boolean]
        reported = df['reported_value'].apply(lambda x: np.nan if x == '' else float(x))
        calculated = df['calculated_value'].astype(float)
        max_boolean = calculated > (reported * 2)
        min_boolean = calculated < (reported * 0.5)
        boolean = max_boolean | min_boolean
        red_df = df[boolean]
        for i, row in red_df.iterrows():
            adm_logger.log_transformation(
                f"Calculated abundance differs to much from reported value. Setting calculated value {row['reported_value']} -> {row['value']} (row number {row['row_number']})",
                level=adm_logger.INFO)

        data_holder.data['value'] = data_holder.data['original_reported_value']
        data_holder.data['reported_value'] = ''
        data_holder.data['reported_parameter'] = ''
        data_holder.data['reported_unit'] = ''

        data_holder.data.loc[red_df.index, 'reported_value'] = data_holder.data.loc[
            red_df.index, 'original_reported_value']
        data_holder.data.loc[red_df.index, 'reported_parameter'] = data_holder.data.loc[
            red_df.index, 'original_reported_parameter']
        data_holder.data.loc[red_df.index, 'reported_unit'] = data_holder.data.loc[
            red_df.index, 'original_reported_unit']
        data_holder.data.loc[red_df.index, 'value'] = data_holder.data.loc[red_df.index, 'original_calculated_value']
        data_holder.data.loc[red_df.index, 'calc_by_dc'] = 'Y'

        # Remove non-handled reported values
        # data_holder.data.loc[~abundance_boolean, 'reported_value'] = ''

        # calc_by_dc


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
                adm_logger.log_transformation(f'Could not calculate anything. Missing counted and sampler_area_cm2.',
                                              level=adm_logger.WARNING)
                continue
            elif not counted:
                adm_logger.log_transformation(f'Could not calculate anything. Missing counted.',
                                              level=adm_logger.WARNING)
                continue
            elif not sampler_area_cm2:
                adm_logger.log_transformation(f'Could not calculate anything. Missing sampler_area_cm2.',
                                              level=adm_logger.WARNING)
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
