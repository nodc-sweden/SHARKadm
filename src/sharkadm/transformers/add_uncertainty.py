import re

import numpy as np
import pandas as pd

from sharkadm.sharkadm_logger import adm_logger

from .base import DataHolderProtocol, Transformer


class AddUncertainty(Transformer):
    valid_data_types = ("physicalchemical",)

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds the uncertainty in float associated to a parameter value"

    def _clean_uncert_str(self, uncert_str: str) -> str:
        uncert_str = uncert_str.replace("nivå", "")
        uncert_str = uncert_str.replace("enheter", "")
        uncert_str = uncert_str.replace("µM", "umol/l")
        uncert_str = re.sub(
            r"[a-zA-Z]",
            lambda match: match.group(0).lower()
            if match.group(0).lower() != "c"
            else "C",
            uncert_str,
        )
        uncert_str = uncert_str.replace("psu", "o/oo psu")
        uncert_str = uncert_str.replace("fnu", "FNU")
        uncert_str = uncert_str.replace("µ", "u")
        uncert_str = uncert_str.replace("°", "")
        uncert_str = uncert_str.replace("(", "")
        uncert_str = uncert_str.replace(")", "")
        uncert_str = uncert_str.replace(">=", "≥")
        uncert_str = uncert_str.replace("<=", "≤")
        uncert_str = re.sub(r"(?<=\d),(?=\d)", ".", uncert_str)
        return uncert_str

    def _conversions_to_shark_units(self, val: float, unit: str, element: str) -> float:
        # Unit conversions from ICES:
        #     https://www.ices.dk/data/tools/Pages/Unit-conversions.aspx
        # O2 from mg/l to ml/l
        # N from ug/l to umol/l
        # P from ug/l to umol/l
        # S from ug/l to umol/l, for H2S-S,
        # Note H2S conversion from ug/l to umol/l:  0.029342
        # Si from ug/l to umol/l, from
        # C from ug/l to umol/l, based on standard atomic weigth: 12.011
        if unit == "mg/l":
            val = val if element == "O" else val * 1000

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

    def _parse_uncert(self, uncert_str: str, name: str):
        uncert_str = self._clean_uncert_str(uncert_str)
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
            "Hydrogen sulphide H2S": "S",
        }
        element = parameter_element_list.get(name, "")

        uncert_levels = [
            level.strip() for level in uncert_str.split(",") if level.strip()
        ]
        uncertainties = pd.DataFrame()

        pattern = (
            r"(?P<uncert_val>\d+\.\d+|\d+)"
            r"(?:\s*)"
            r"(?P<uncert_unit>1/(?:[^\d|<|>|≥|≤)]+)|[^\d|<|>|≥|≤]+)?"
            r"(?:\s*)"
            r"(?P<limit_operator><|>|≥|≤)?"
            r"(?P<limit_val1>\d+\.\d+|\d+)?"
            r"(?:-)?"
            r"(?P<limit_val2>\d+\.\d+|\d+)?"
            r"(?:\s*)"
            r"(?P<limit_unit>.*)?"
        )

        for i, level in enumerate(uncert_levels):
            groups = re.search(pattern, level.strip())
            groups = groups.groupdict() if groups else {}

            uncert_val = groups.get("uncert_val")
            uncert_unit = (groups.get("uncert_unit") or "").strip()
            limit_operator = (groups.get("limit_operator") or "").strip()
            limit_val1 = groups.get("limit_val1")
            limit_val2 = groups.get("limit_val2")
            limit_val = limit_val2 if limit_val2 else limit_val1
            limit_val_lower = limit_val1 if limit_val2 and limit_val1 else None
            limit_unit = (groups.get("limit_unit") or "").strip()
            limit_unit_lower = (groups.get("limit_unit") or "").strip()

            if uncert_val:
                uncert_val = float(uncert_val)
                if element and (uncert_unit in ("ug/l", "mg/l")):
                    uncertainties.loc[i, "unc"] = self._conversions_to_shark_units(
                        uncert_val, uncert_unit, element
                    )
                    uncert_unit = "ml/l" if element == "O" else "umol/l"
                    uncertainties.loc[i, "unc_unit"] = uncert_unit
                else:
                    uncertainties.loc[i, "unc"] = uncert_val
                    uncertainties.loc[i, "unc_unit"] = uncert_unit
            else:
                uncertainties.loc[i, "unc"] = None
                uncertainties.loc[i, "unc_unit"] = ""

            if limit_val:
                limit_val = float(limit_val)
                if element and (limit_unit in ("ug/l", "mg/l")):
                    uncertainties.loc[i, "limit"] = self._conversions_to_shark_units(
                        limit_val, limit_unit, element
                    )
                    limit_unit = "ml/l" if element == "O" else "umol/l"
                    uncertainties.loc[i, "limit_unit"] = limit_unit
                else:
                    uncertainties.loc[i, "limit"] = limit_val
                    uncertainties.loc[i, "limit_unit"] = limit_unit
            else:
                uncertainties.loc[i, "limit"] = None
                uncertainties.loc[i, "limit_unit"] = ""

            if limit_val_lower:
                limit_val_lower = float(limit_val_lower)
                if element and (limit_unit_lower in ("ug/l", "mg/l")):
                    uncertainties.loc[i, "limit_lower"] = (
                        self._conversions_to_shark_units(
                            limit_val_lower, limit_unit_lower, element
                        )
                    )
                    limit_unit_lower = "ml/l" if element == "O" else "umol/l"
                    uncertainties.loc[i, "limit_unit_lower"] = limit_unit_lower
                else:
                    uncertainties.loc[i, "limit_lower"] = limit_val_lower
                    uncertainties.loc[i, "limit_unit_lower"] = limit_unit_lower
            else:
                uncertainties.loc[i, "limit_lower"] = None
                uncertainties.loc[i, "limit_unit_lower"] = ""

            if limit_operator:
                uncertainties.loc[i, "limit_operator"] = limit_operator
            else:
                uncertainties.loc[i, "limit_operator"] = ""

        if len(uncertainties) > 1:
            uncertainties = uncertainties.sort_values(
                by="limit", ascending=True
            ).reset_index(drop=True)
        return uncertainties

    def _get_selection(
        self,
        param_vals: pd.Series,
        unc: float,
        unit: str,
        limit: float,
        limit_unit: str,
        limit_lower: float,
        limit_lower_unit: str,
        limit_operator: str,
        limit_operator_lower: str,
        limit_operator_upper: str,
        selection_type: str,
    ):
        selection = pd.Series([False] * len(param_vals), index=param_vals.index)

        if pd.notnull(limit) and (limit_unit == unit or limit_unit == ""):
            if selection_type == "below":
                selection = (
                    param_vals < limit
                    if limit_operator == "<"
                    else param_vals < limit
                    if ("≥" in (limit_operator_upper or ""))
                    else param_vals <= limit
                )
            elif selection_type == "above":
                selection = (
                    param_vals > limit
                    if limit_operator == ">"
                    else param_vals > limit
                    if ("≤" in (limit_operator_lower or ""))
                    else param_vals >= limit
                )
            elif selection_type == "between":
                if limit_lower_unit == unit or limit_lower_unit == "":
                    if (
                        ("<" in (limit_operator_lower or "")) or not limit_operator_lower
                    ) and (("≤" in (limit_operator or "")) or not limit_operator):
                        selection = (param_vals >= limit_lower) & (param_vals <= limit)
                    elif (
                        ("<" in (limit_operator_lower or "")) or not limit_operator_lower
                    ) and ("<" in (limit_operator or "")):
                        selection = (param_vals >= limit_lower) & (param_vals < limit)
                    elif ("≤" in (limit_operator_lower or "")) and (
                        "<" in (limit_operator or "")
                    ):
                        selection = (param_vals > limit_lower) & (param_vals < limit)
                    elif ("≤" in (limit_operator_lower or "")) and (
                        ("≤" in (limit_operator or "")) or not limit_operator
                    ):
                        selection = (param_vals > limit_lower) & (param_vals <= limit)
                    else:
                        adm_logger.log_transformation(
                            "Could not select rows to add uncertainty values to, "
                            "check the given uncertainty in original file",
                            level=adm_logger.WARNING,
                        )
        elif pd.notnull(unc) and pd.isnull(limit):
            selection = pd.Series([True] * len(param_vals), index=param_vals.index)
        else:
            adm_logger.log_transformation(
                "Could not select rows to add uncertainty values to, "
                "check the given uncertainty in original file",
                level=adm_logger.WARNING,
            )
        return selection

    def _get_uncert(
        self, param_vals: pd.Series, unit: str, unc_val: float, unc_unit: str
    ):
        try:
            if unc_unit == "%":
                return float(unc_val) / 100 * param_vals
            elif unc_unit == unit or unc_unit == "":
                return np.full_like(param_vals, float(unc_val), dtype=float)
            elif not unc_unit:
                return np.full_like(param_vals, float(unc_val), dtype=float)
            else:
                return np.full_like(param_vals, np.nan, dtype=float)
        except (ValueError, TypeError):
            return np.full_like(param_vals, np.nan, dtype=float)

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        unique_uncert_df = data_holder.data[
            ["parameter", "unit", "estimation_uncertainty"]
        ].drop_duplicates()
        for _, row in unique_uncert_df.iterrows():
            param, unit, uncert_str = (
                row["parameter"],
                row["unit"],
                row["estimation_uncertainty"],
            )
            uncert_bool = (
                (data_holder.data["parameter"] == param)
                & (data_holder.data["unit"] == unit)
                & (data_holder.data["estimation_uncertainty"] == uncert_str)
            )
            if not uncert_str or uncert_str.strip() == "":
                data_holder.data.loc[uncert_bool, "UNCERT_VAL"] = np.nan
                adm_logger.log_transformation(
                    f"Uncertainty value not extracted from dataset for parameter: "
                    f"{param}",
                    level=adm_logger.WARNING,
                )
                continue

            param_vals = data_holder.data.loc[uncert_bool, "value"].astype(float)
            uncert_vals = pd.Series(np.nan, index=param_vals.index)

            uncertainties = self._parse_uncert(uncert_str, str(param))

            if (
                uncertainties.empty
                or uncertainties["unc"].isnull().all()
                or (
                    sum(uncertainties["unc"].notnull()) > 1
                    and uncertainties["limit"].isnull().all()
                )
            ):
                data_holder.data.loc[uncert_bool, "UNCERT_VAL"] = np.nan
                adm_logger.log_transformation(
                    f"Uncertainty value not extracted from dataset for parameter: "
                    f"{param}",
                    level=adm_logger.WARNING,
                )
                continue

            number_of_levels = len(uncertainties)

            for i in range(number_of_levels):
                unc = uncertainties["unc"][i]
                unc_unit = uncertainties["unc_unit"][i]

                limit = uncertainties["limit"][i]
                limit_unit = uncertainties["limit_unit"][i]

                limit_lower = (
                    uncertainties["limit"][i - 1]
                    if i > 0
                    else uncertainties["limit_lower"][0]
                )
                limit_lower_unit = (
                    uncertainties["limit_unit"][i - 1]
                    if i > 0
                    else uncertainties["limit_unit_lower"][0]
                )

                limit_operator_lower = (
                    uncertainties["limit_operator"][i - 1] if i > 0 else None
                )
                limit_operator_upper = (
                    uncertainties["limit_operator"][i + 1]
                    if i < number_of_levels - 1
                    else None
                )
                limit_operator = uncertainties["limit_operator"][i]
                if i == 0:
                    selection_type = "below" if pd.isnull(limit_lower) else "between"
                    selection = self._get_selection(
                        param_vals,
                        unc,
                        unit,
                        limit,
                        limit_unit,
                        limit_lower,
                        limit_lower_unit,
                        limit_operator,
                        limit_operator_lower,
                        limit_operator_upper,
                        selection_type,
                    )
                elif i == (number_of_levels - 1):
                    selection_type = "above" if limit == limit_lower else "between"
                    selection = self._get_selection(
                        param_vals,
                        unc,
                        unit,
                        limit,
                        limit_unit,
                        limit_lower,
                        limit_lower_unit,
                        limit_operator,
                        limit_operator_lower,
                        limit_operator_upper,
                        selection_type,
                    )
                else:
                    selection_type = "between"
                    selection = self._get_selection(
                        param_vals,
                        unc,
                        unit,
                        limit,
                        limit_unit,
                        limit_lower,
                        limit_lower_unit,
                        limit_operator,
                        limit_operator_lower,
                        limit_operator_upper,
                        selection_type,
                    )

                if "unc_unit" not in uncertainties.columns:
                    uncert_vals[selection] = self._get_uncert(
                        param_vals[selection], unit, unc, ""
                    )
                else:
                    uncert_vals[selection] = self._get_uncert(
                        param_vals[selection],
                        unit,
                        unc,
                        str(unc_unit),
                    )

            data_holder.data.loc[uncert_bool, "UNCERT_VAL"] = uncert_vals
