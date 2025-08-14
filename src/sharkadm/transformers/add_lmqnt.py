import re

import pandas as pd
import polars as pl

from .base import DataHolderProtocol, Transformer


class AddLmqnt(Transformer):
    valid_data_types = ("physicalchemical",)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            "Adds the limit of quantification (lmqnt) in float "
            "associated to a parameter value"
        )

    def _clean_lmqnt_str(self, lmqnt_str: str) -> str:
        if not any(char.isdigit() for char in lmqnt_str):
            return ""

        lmqnt_str = re.sub(
            r"[a-zA-Z]",
            lambda match: "C"
            if match.group(0).lower() == "c"
            else match.group(0).lower(),
            lmqnt_str,
        )

        string_replacements = {
            "kininsulfat": "",
            "µm": "umol/l",
            "psu": "o/oo psu",
            "fnu": "FNU",
            "µ": "u",
            "°": "",
            "(": "",
            ")": "",
            "<": "",
            "≥": "",
            ">=": "",
            ",": ".",
        }
        for old, new in string_replacements.items():
            lmqnt_str = lmqnt_str.replace(old, new)

        return lmqnt_str

    def _conversions_to_shark_units(self, val: float, element: str) -> float:
        # Unit conversions from ICES:
        #     https://www.ices.dk/data/tools/Pages/Unit-conversions.aspx
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
            "C": 0.083257,
        }
        converted_val = val * conversions[element]
        return converted_val

    def _parse_lmqnt(self, lmqnt_str: str, name: str):
        lmqnt_str = self._clean_lmqnt_str(lmqnt_str)
        parameter_element_list = {
            "Dissolved oxygen O2 bottle": "O",
            "Dissolved oxygen O2 CTD": "O",
            "Nitrite NO2-N": "N",
            "Nitrate NO3-N": "N",
            "Nitrite+Nitrate NO2+NO3-N": "N",
            "Ammonium NH4-N": "N",
            "Total Nitrogen Tot-N": "N",
            "Particulate organic nitrogen PON": "N",
            "Phosphate PO4-P": "P",
            "Total phosphorus Tot-P": "P",
            "Silicate SiO3-Si": "Si",
            "Dissolved organic carbon DOC": "C",
            "Particulate organic carbon POC": "C",
        }

        element = parameter_element_list.get(name, "")

        pattern = r"(-?\d+(?:\.\d+)?)(?:\s*)([^\d]*)"
        matches = re.search(pattern, lmqnt_str.strip())

        if not matches:
            return None, ""

        lmqnt = float(matches.group(1))
        lmqnt_unit = matches.group(2).strip()

        if element and lmqnt_unit in ("ug/l", "mg/l"):
            factor = (
                1000
                if lmqnt_unit == "mg/l" and element != "O"
                else 0.001
                if lmqnt_unit == "ug/l" and element == "O"
                else 1
            )
            lmqnt *= factor
            lmqnt = self._conversions_to_shark_units(lmqnt, element)
            new_lmqnt_unit = (
                "ml/l" if element == "O" and lmqnt_unit == "mg/l" else "umol/l"
            )
            return lmqnt, new_lmqnt_unit
        return lmqnt, lmqnt_unit

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for unique_lmqnt in data_holder.data["quantification_limit"].unique():
            lmqnt_bool = data_holder.data["quantification_limit"] == unique_lmqnt

            if (
                unique_lmqnt is None
                or pd.isna(unique_lmqnt)
                or (isinstance(unique_lmqnt, str) and not unique_lmqnt.strip())
            ):
                data_holder.data.loc[lmqnt_bool, "LMQNT_VAL"] = float("nan")
                continue

            temp = data_holder.data.loc[lmqnt_bool, "quantification_limit"]
            lmqnt_str = temp.iloc[0]
            temp = data_holder.data.loc[lmqnt_bool, "unit"]
            unit = str(temp.iloc[0])
            temp = data_holder.data.loc[lmqnt_bool, "parameter"]
            name = str(temp.iloc[0])

            lmqnt, lmqnt_unit = self._parse_lmqnt(lmqnt_str, name)

            if lmqnt_unit == "":
                data_holder.data.loc[lmqnt_bool, "LMQNT_VAL"] = (
                    lmqnt if lmqnt is not None else float("nan")
                )
            elif lmqnt_unit == unit:
                data_holder.data.loc[lmqnt_bool, "LMQNT_VAL"] = (
                    lmqnt if lmqnt is not None else float("nan")
                )


class PolarsAddLmqnt(Transformer):
    valid_data_types = ("physicalchemical",)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            "Adds the limit of quantification (lmqnt) in float "
            "associated to a parameter value"
        )

    def _clean_lmqnt_str(self, lmqnt_str: str) -> str:
        if not any(char.isdigit() for char in lmqnt_str):
            return ""

        lmqnt_str = re.sub(
            r"[a-zA-Z]",
            lambda match: "C"
            if match.group(0).lower() == "c"
            else match.group(0).lower(),
            lmqnt_str,
        )

        string_replacements = {
            "kininsulfat": "",
            "µm": "umol/l",
            "psu": "o/oo psu",
            "fnu": "FNU",
            "µ": "u",
            "°": "",
            "(": "",
            ")": "",
            "<": "",
            "≥": "",
            ">=": "",
            ",": ".",
        }
        for old, new in string_replacements.items():
            lmqnt_str = lmqnt_str.replace(old, new)

        return lmqnt_str

    def _conversions_to_shark_units(self, val: float, element: str) -> float:
        # Unit conversions from ICES:
        #     https://www.ices.dk/data/tools/Pages/Unit-conversions.aspx
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
            "C": 0.083257,
        }
        converted_val = val * conversions[element]
        return converted_val

    def _parse_lmqnt(self, lmqnt_str: str, name: str):
        lmqnt_str = self._clean_lmqnt_str(lmqnt_str)
        parameter_element_list = {
            "Dissolved oxygen O2 bottle": "O",
            "Dissolved oxygen O2 CTD": "O",
            "Nitrite NO2-N": "N",
            "Nitrate NO3-N": "N",
            "Nitrite+Nitrate NO2+NO3-N": "N",
            "Ammonium NH4-N": "N",
            "Total Nitrogen Tot-N": "N",
            "Particulate organic nitrogen PON": "N",
            "Phosphate PO4-P": "P",
            "Total phosphorus Tot-P": "P",
            "Silicate SiO3-Si": "Si",
            "Dissolved organic carbon DOC": "C",
            "Particulate organic carbon POC": "C",
        }

        element = parameter_element_list.get(name, "")

        pattern = r"(-?\d+(?:\.\d+)?)(?:\s*)([^\d]*)"
        matches = re.search(pattern, lmqnt_str.strip())

        if not matches:
            return None, ""

        lmqnt = float(matches.group(1))
        lmqnt_unit = matches.group(2).strip()

        if element and lmqnt_unit in ("ug/l", "mg/l"):
            factor = (
                1000
                if lmqnt_unit == "mg/l" and element != "O"
                else 0.001
                if lmqnt_unit == "ug/l" and element == "O"
                else 1
            )
            lmqnt *= factor
            lmqnt = self._conversions_to_shark_units(lmqnt, element)
            new_lmqnt_unit = (
                "ml/l" if element == "O" and lmqnt_unit == "mg/l" else "umol/l"
            )
            return lmqnt, new_lmqnt_unit
        return lmqnt, lmqnt_unit

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for unique_lmqnt in data_holder.data["quantification_limit"].unique():
            lmqnt_bool = data_holder.data["quantification_limit"] == unique_lmqnt

            if (
                unique_lmqnt is None
                or pd.isna(unique_lmqnt)
                or (isinstance(unique_lmqnt, str) and not unique_lmqnt.strip())
            ):
                data_holder.data.loc[lmqnt_bool, "LMQNT_VAL"] = float("nan")
                continue

            temp = data_holder.data.loc[lmqnt_bool, "quantification_limit"]
            lmqnt_str = temp.iloc[0]
            temp = data_holder.data.loc[lmqnt_bool, "unit"]
            unit = str(temp.iloc[0])
            temp = data_holder.data.loc[lmqnt_bool, "parameter"]
            name = str(temp.iloc[0])

            lmqnt, lmqnt_unit = self._parse_lmqnt(lmqnt_str, name)

            if lmqnt_unit == "":
                data_holder.data = data_holder.data.with_columns(
                    pl.when(lmqnt_bool).then(lmqnt).alias("LMQNT_VAL")
                )
            elif lmqnt_unit == unit:
                data_holder.data = data_holder.data.with_columns(
                    pl.when(lmqnt_bool).then(lmqnt).alias("LMQNT_VAL")
                )
