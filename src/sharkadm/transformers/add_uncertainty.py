from .base import Transformer, DataHolderProtocol
import pandas as pd
import numpy as np
import re


class AddUncertainty(Transformer):
    valid_data_types = ['physicalchemical']

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds the uncertainty in float associated to a parameter value'

    def _clean_uncert_str(self, uncert_str: str) -> str:
        uncert_str = uncert_str.replace('nivå', '')
        uncert_str = uncert_str.replace('enheter', '')
        uncert_str = uncert_str.replace('µM', 'umol/l')
        uncert_str = re.sub(r'[a-zA-Z]', lambda match: match.group(0).lower() if match.group(0).lower() != 'c' else 'C',
                            uncert_str)
        uncert_str = uncert_str.replace('psu', 'o/oo psu')
        uncert_str = uncert_str.replace('fnu', 'FNU')
        uncert_str = uncert_str.replace('µ', 'u')
        uncert_str = uncert_str.replace('°', '')
        uncert_str = uncert_str.replace('(', '')
        uncert_str = uncert_str.replace(')', '')
        uncert_str = uncert_str.replace('<', '')
        uncert_str = uncert_str.replace('≥', '')
        uncert_str = uncert_str.replace('>=', '')
        uncert_str = re.sub(r'(?<=\d),(?=\d)', '.', uncert_str)
        return uncert_str

    def _conversions_to_shark_units(self, val: float, element: str) -> float:
        # Unit conversions from ICES: https://www.ices.dk/data/tools/Pages/Unit-conversions.aspx
        # O2 from mg/l to ml/l
        # N from ug/l to umol/l
        # P from ug/l to umol/l
        # S from ug/l to umol/l
        # Si from ug/l to umol/l, from
        # C from ug/l to umol/l, based on standard atomic weigth: 12.011

        conversions = {
            "O": 0.700,
            "N": 0.071394,
            "P": 0.032285,
            "S": 0.031187,
            "Si": 0.035606,
            "C": 0.083257
        }
        converted_val = val * conversions[element]
        return converted_val

    def _parse_uncert(self, uncert_str: str, name: str):
        uncertainties = pd.DataFrame()
        uncert_str = self._clean_uncert_str(uncert_str)
        parameter_name_list = ['Dissolved oxygen O2 bottle', 'Dissolved oxygen O2 CTD', 'Nitrite NO2-N',
                               'Nitrate NO3-N', 'Nitrite+Nitrate NO2+NO3-N', 'Ammonium NH4-N', 'Total Nitrogen Tot-N',
                               'Particulate organic nitrogen PON', 'Phosphate PO4-P', 'Total phosphorus Tot-P',
                               'Silicate SiO3-Si', 'Dissolved organic carbon DOC', 'Particulate organic carbon POC']
        element_list = ['O', 'O', 'N', 'N', 'N', 'N', 'N', 'N', 'P', 'P', 'Si', 'C', 'C']
        element = ''
        if name in parameter_name_list:
            element = element_list[parameter_name_list.index(name)]
        # fall med annan separator kan behöva hanteras, ok för DEEP, UMSC, SMHI i arkivmapp ej original.
        uncert_levels = uncert_str.split(",")
        i = 0
        for level in uncert_levels:
            # fall med <= tecken kan behöva hanteras, inte stött på i DEEP och UMSC
            if '>' in level:
                prior_endpoint = 'yes'
                level = level.replace('>', '')
            else:
                prior_endpoint = 'no'
            pattern = r'(\d+\.\d+|\d+)([^\d]*)((\d+\.\d+|\d+)(?:-(\d+\.\d+|\d+))?)?\s*([^\d]*)'
            matches = re.search(pattern, level.strip())
            item = [matches.group(1),
                    matches.group(2).strip() if matches.group(2) else '',
                    matches.group(4) if matches.group(4) else '',
                    matches.group(6).strip() if matches.group(6) else ''
                    ]
            if matches.group(5):
                item[2] = matches.group(5).strip() if matches.group(5) else ''

            if (element != '') & (str(item[1]) == 'ug/l'):
                uncertainties.loc[i, 'unc'] = self._conversions_to_shark_units(float(item[0]), element)
                uncertainties.loc[i, 'unc_unit'] = 'umol/l'
            elif (element != '') & (str(item[1]) == 'mg/l'):
                if element == 'O':
                    uncertainties.loc[i, 'unc'] = self._conversions_to_shark_units(float(item[0]), element)
                    uncertainties.loc[i, 'unc_unit'] = 'ml/l'
                else:
                    uncertainties.loc[i, 'unc'] = self._conversions_to_shark_units(float(item[0])*1000, element)
                    uncertainties.loc[i, 'unc_unit'] = 'umol/l'
            else:
                uncertainties.loc[i, 'unc'] = float(item[0])
                uncertainties.loc[i, 'unc_unit'] = str(item[1])

            if (element != '') & (str(item[3]) == 'ug/l'):
                uncertainties.loc[i, 'limit'] = self._conversions_to_shark_units(float(item[2]), element)
                uncertainties.loc[i, 'limit_unit'] = 'umol/l'
            elif (element != '') & (str(item[3]) == 'mg/l'):
                if element == 'O':
                    uncertainties.loc[i, 'limit'] = self._conversions_to_shark_units(float(item[2]), element)
                    uncertainties.loc[i, 'limit_unit'] = 'ml/l'
                else:
                    uncertainties.loc[i, 'limit'] = self._conversions_to_shark_units(float(item[2])*1000, element)
                    uncertainties.loc[i, 'limit_unit'] = 'umol/l'
            elif item[2]:
                uncertainties.loc[i, 'limit'] = float(item[2])
                uncertainties.loc[i, 'limit_unit'] = str(item[3])
            i = i + 1
        if len(uncertainties) > 1:
            uncertainties = uncertainties.sort_values(by='limit', ascending=True).reset_index(drop=True)
        return uncertainties, prior_endpoint

    def _get_uncert(self, param_vals: pd.Series, unit: str, unc_val: float, unc_unit: str):
        try:
            if unc_unit == '%':
                return float(unc_val) / 100 * param_vals
            elif unc_unit == unit or unc_unit == '':
                return np.full_like(param_vals, float(unc_val), dtype=float)
            elif not unc_unit:
                return np.full_like(param_vals, float(unc_val), dtype=float)
            else:
                return np.full_like(param_vals, np.nan, dtype=float)
        except (ValueError, TypeError):
            return np.full_like(param_vals, np.nan, dtype=float)

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for param in data_holder.data['parameter'].unique():
            param_bool = data_holder.data['parameter'] == param
            param_vals = data_holder.data.loc[param_bool, 'value'].astype(float)
            uncert_vals = pd.Series(np.nan, index=param_vals.index)
            temp = data_holder.data.loc[param_bool, 'estimation_uncertainty']
            uncert_str = temp.iloc[0]
            if not uncert_str or uncert_str.isspace():
                data_holder.data.loc[param_bool, 'UNCERT_VAL'] = float('nan')
                continue
            temp = data_holder.data.loc[param_bool, 'unit']
            unit = str(temp.iloc[0])
            uncertainties, prior_endpoint = self._parse_uncert(uncert_str, str(param))


            if len(uncertainties) == 3:
                if ((uncertainties['limit_unit'] == unit) | (uncertainties['limit_unit'] == '')).all():
                    bool_range_low = (param_vals < uncertainties['limit'][0])
                    if prior_endpoint == 'yes':
                        bool_range_middle = ((param_vals <= uncertainties['limit'][1]) &
                                             (param_vals >= uncertainties['limit'][0]))
                        bool_range_high = (param_vals > uncertainties['limit'][2])

                    else:
                        bool_range_middle = ((param_vals < uncertainties['limit'][1]) &
                                             (param_vals >= uncertainties['limit'][0]))
                        bool_range_high = (param_vals >= uncertainties['limit'][2])
                    uncert_vals[bool_range_low] = self._get_uncert(param_vals[bool_range_low], unit,
                                                                   uncertainties['unc'][0],
                                                                   str(uncertainties['unc_unit'][0]))
                    uncert_vals[bool_range_middle] = self._get_uncert(param_vals[bool_range_middle], unit,
                                                                      uncertainties['unc'][1],
                                                                      str(uncertainties['unc_unit'][1]))
                    uncert_vals[bool_range_high] = self._get_uncert(param_vals[bool_range_high], unit,
                                                                    uncertainties['unc'][2],
                                                                    str(uncertainties['unc_unit'][2]))

            elif len(uncertainties) == 2:
                if ((uncertainties['limit_unit'] == unit) | (uncertainties['limit_unit'] == '')).all():
                    # note that UMSC does not have ranges, but gives two different values at two levels.
                    # the code below assumes values below the first level has the uncertainty of that level,
                    # whereas values above the first level have the uncertainty of the second level.
                    if prior_endpoint == 'yes':
                        bool_range_low = (param_vals <= uncertainties['limit'][0])
                        bool_range_high = (param_vals > uncertainties['limit'][0])

                    else:
                        bool_range_low = (param_vals < uncertainties['limit'][0])
                        bool_range_high = (param_vals >= uncertainties['limit'][0])

                    uncert_vals.loc[bool_range_low] = self._get_uncert(param_vals[bool_range_low], unit,
                                                                       uncertainties['unc'][0],
                                                                       str(uncertainties['unc_unit'][0]))
                    uncert_vals[bool_range_high] = self._get_uncert(param_vals[bool_range_high], unit,
                                                                    uncertainties['unc'][1],
                                                                    str(uncertainties['unc_unit'][1]))

            elif len(uncertainties) == 1:
                if 'unc_unit' not in uncertainties.columns:
                    uncert_vals = self._get_uncert(param_vals, unit, uncertainties['unc'][0], '')
                else:
                    uncert_vals = self._get_uncert(param_vals, unit, str(uncertainties['unc'][0]),
                                                   uncertainties['unc_unit'][0])
            data_holder.data.loc[param_bool, 'UNCERT_VAL'] = uncert_vals






