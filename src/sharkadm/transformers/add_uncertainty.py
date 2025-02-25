from .base import Transformer, DataHolderProtocol
import pandas as pd
import re


class AddUncertainty(Transformer):
    valid_data_types = ['physicalchemical']

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds the uncertainty in float associated to a parameter value'

    def _clean_uncert_str(self, uncert_str: str) -> str:
        uncert_str = uncert_str.replace('nivå', '')
        uncert_str = uncert_str.replace('enheter', '')
        uncert_str = uncert_str.replace('psu', 'o/oo psu')
        uncert_str = uncert_str.replace('ml/L', 'ml/l')
        uncert_str = uncert_str.replace('mg/L', 'mg/l')
        uncert_str = uncert_str.replace('µmol/L', 'umol/l')
        uncert_str = uncert_str.replace('µM', 'umol/l')
        uncert_str = uncert_str.replace('µg/L', 'ug/l')
        uncert_str = uncert_str.replace('°', '')
        uncert_str = uncert_str.replace('(', '')
        uncert_str = uncert_str.replace(')', '')
        uncert_str = uncert_str.replace('<', '')
        uncert_str = uncert_str.replace('≥', '')
        uncert_str = uncert_str.replace('>=', '')
        return uncert_str

    def _ices_conversions_to_shark_units(self, val: float, element: str) -> float:
        # O2 from mg/l to ml/l
        # N from ug/l to umol/l
        # P from ug/l to umol/l
        # S from ug/l to umol/l
        # Si from ug/l to umol/l

        conversions = {
            "O": 0.700,
            "N": 0.071394,
            "P": 0.032285,
            "S": 0.031187,
            "Si": 0.035606,
        }
        converted_val = val * conversions[element]
        return converted_val

    def _parse_uncert(self, uncert_str: str, name: str, source: str):
        uncertainties = pd.DataFrame()
        uncert_str = self._clean_uncert_str(uncert_str)
        parameter_name_list = ['Dissolved oxygen O2 bottle', 'Dissolved oxygen O2 CTD', 'Nitrite NO2-N',
                               'Nitrate NO3-N', 'Nitrite+Nitrate NO2+NO3-N', 'Ammonium NH4-N', 'Total Nitrogen Tot-N',
                               'Phosphate PO4-P', 'Total phosphorus Tot-P', 'Silicate SiO3-Si']
        element_list = ['O', 'O', 'N', 'N', 'N', 'N', 'N', 'P', 'P', 'Si']
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
                uncertainties.loc[i, 'unc'] = self._ices_conversions_to_shark_units(float(item[0]), element)
                uncertainties.loc[i, 'unc_unit'] = 'umol/l'
            elif (element != '') & (str(item[1]) == 'mg/l'):
                if element == 'O':
                    uncertainties.loc[i, 'unc'] = self._ices_conversions_to_shark_units(float(item[0]), element)
                    uncertainties.loc[i, 'unc_unit'] = 'ml/l'
                else:
                    uncertainties.loc[i, 'unc'] = self._ices_conversions_to_shark_units(float(item[0])*1000, element)
                    uncertainties.loc[i, 'unc_unit'] = 'umol/l'
            else:
                uncertainties.loc[i, 'unc'] = float(item[0])
                uncertainties.loc[i, 'unc_unit'] = str(item[1])

            if (element != '') & (str(item[3]) == 'ug/l'):
                uncertainties.loc[i, 'limit'] = self._ices_conversions_to_shark_units(float(item[2]), element)
                uncertainties.loc[i, 'limit_unit'] = 'umol/l'
            elif (element != '') & (str(item[3]) == 'mg/l'):
                if element == 'O':
                    uncertainties.loc[i, 'limit'] = self._ices_conversions_to_shark_units(float(item[2]), element)
                    uncertainties.loc[i, 'limit_unit'] = 'ml/l'
                else:
                    uncertainties.loc[i, 'limit'] = self._ices_conversions_to_shark_units(float(item[2])*1000, element)
                    uncertainties.loc[i, 'limit_unit'] = 'umol/l'
            elif item[2]:
                uncertainties.loc[i, 'limit'] = float(item[2])
                uncertainties.loc[i, 'limit_unit'] = str(item[3])
            i = i + 1
        if len(uncertainties) > 1:
            uncertainties = uncertainties.sort_values(by='limit', ascending=True).reset_index(drop=True)
        return uncertainties, prior_endpoint

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        source = data_holder.data['source'][0]
        for idx, row in data_holder.data.iterrows():
            uncert_str = row['estimation_uncertainty']
            if not uncert_str or uncert_str.isspace():
                data_holder.data.loc[idx, 'UNCERT_VAL'] = float('nan')
                continue

            name = str(row['parameter'])
            value = float(row['value'])
            unit = str(row['unit'])

            uncertainties, prior_endpoint = self._parse_uncert(uncert_str, name, source)

            if len(uncertainties) > 1:
                for i, r in uncertainties.iterrows():
                    if (((value < r['limit']) & (r['limit_unit'] == unit)) |
                            ((value == r['limit']) & (prior_endpoint == 'yes') & (i == (len(uncertainties)-2)) &
                             (r['limit_unit'] == unit)) |
                            ((i == (len(uncertainties)-1)) & (r['limit_unit'] == unit))):
                        if r['unc_unit'] == '%':
                            uncert_val = float(r['unc']) / 100 * value
                            data_holder.data.loc[idx, 'UNCERT_VAL'] = uncert_val if uncert_val is not None else float(
                                'nan')
                        elif r['unc_unit'] == unit:
                            uncert_val = float(r['unc'])
                            data_holder.data.loc[idx, 'UNCERT_VAL'] = uncert_val if uncert_val is not None else float('nan')
                        elif r['unc_unit'] == '':
                            uncert_val = float(r['unc'])
                            data_holder.data.loc[idx, 'UNCERT_VAL'] = uncert_val if uncert_val is not None else float('nan')
                        break
            else:
                if 'unc_unit' not in uncertainties.columns:
                    uncert_val = float(uncertainties['unc'].iloc[0])
                    data_holder.data.loc[idx, 'UNCERT_VAL'] = uncert_val if uncert_val is not None else float('nan')
                elif uncertainties['unc_unit'][0] == '':
                    uncert_val = float(uncertainties['unc'].iloc[0])
                    data_holder.data.loc[idx, 'UNCERT_VAL'] = uncert_val if uncert_val is not None else float('nan')
                elif uncertainties['unc_unit'][0] == '%':
                    uncert_val = float(uncertainties['unc'].iloc[0]) / 100 * value
                    data_holder.data.loc[idx, 'UNCERT_VAL'] = uncert_val if uncert_val is not None else float('nan')
                elif uncertainties['unc_unit'][0] == unit:
                    uncert_val = float(uncertainties['unc'].iloc[0])
                    data_holder.data.loc[idx, 'UNCERT_VAL'] = uncert_val if uncert_val is not None else float('nan')


