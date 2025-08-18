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
        if any(char.isdigit() for char in lmqnt_str):
            lmqnt_str = lmqnt_str.replace("kininsulfat", "")
            lmqnt_str = lmqnt_str.replace("µM", "umol/l")
            lmqnt_str = re.sub(
                r"[a-zA-Z]",
                lambda match: match.group(0).lower()
                if match.group(0).lower() != "c"
                else "C",
                lmqnt_str,
            )
            lmqnt_str = lmqnt_str.replace("psu", "o/oo psu")
            lmqnt_str = lmqnt_str.replace("fnu", "FNU")
            lmqnt_str = lmqnt_str.replace("µ", "u")
            lmqnt_str = lmqnt_str.replace("°", "")
            lmqnt_str = lmqnt_str.replace("(", "")
            lmqnt_str = lmqnt_str.replace(")", "")
            lmqnt_str = lmqnt_str.replace("<", "")
            lmqnt_str = lmqnt_str.replace("≥", "")
            lmqnt_str = lmqnt_str.replace(">=", "")
            lmqnt_str = lmqnt_str.replace(",", ".")
        else:
            lmqnt_str = ""
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
        if any(char.isdigit() for char in lmqnt_str):
            lmqnt_str = lmqnt_str.replace("kininsulfat", "")
            lmqnt_str = lmqnt_str.replace("µM", "umol/l")
            lmqnt_str = re.sub(
                r"[a-zA-Z]",
                lambda match: match.group(0).lower()
                if match.group(0).lower() != "c"
                else "C",
                lmqnt_str,
            )
            lmqnt_str = lmqnt_str.replace("psu", "o/oo psu")
            lmqnt_str = lmqnt_str.replace("fnu", "FNU")
            lmqnt_str = lmqnt_str.replace("µ", "u")
            lmqnt_str = lmqnt_str.replace("°", "")
            lmqnt_str = lmqnt_str.replace("(", "")
            lmqnt_str = lmqnt_str.replace(")", "")
            lmqnt_str = lmqnt_str.replace("<", "")
            lmqnt_str = lmqnt_str.replace("≥", "")
            lmqnt_str = lmqnt_str.replace(">=", "")
            lmqnt_str = lmqnt_str.replace(",", ".")
        else:
            lmqnt_str = ""
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

    def _extract_lmqnt(self, row):
        lmqnt_str = row["quantification_limit"]
        unit = row["unit"]
        name = row["parameter"]

        if lmqnt_str is None or (isinstance(lmqnt_str, str) and not lmqnt_str.strip()):
            return None

        lmqnt_val, lmqnt_unit = self._parse_lmqnt(lmqnt_str, name)

        if lmqnt_unit == "" or lmqnt_unit == unit:
            return lmqnt_val
        return None

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        unique_lmqnt_df = data_holder.data.select(
            ["quantification_limit", "parameter", "unit"]
        ).unique()
        unique_lmqnt_df = unique_lmqnt_df.with_columns(
            pl.struct(["quantification_limit", "parameter", "unit"])
            .map_elements(self._extract_lmqnt, return_dtype=pl.Float64)
            .alias("LMQNT_VAL")
        )
        data_holder.data = data_holder.data.join(
            unique_lmqnt_df, on=["quantification_limit", "parameter", "unit"], how="left"
        )
