import re

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
        parameter_name_list = [
            "Dissolved oxygen O2 bottle",
            "Dissolved oxygen O2 CTD",
            "Nitrite NO2-N",
            "Nitrate NO3-N",
            "Nitrite+Nitrate NO2+NO3-N",
            "Ammonium NH4-N",
            "Total Nitrogen Tot-N",
            "Particulate organic nitrogen PON",
            "Phosphate PO4-P",
            "Total phosphorus Tot-P",
            "Silicate SiO3-Si",
            "Dissolved organic carbon DOC",
            "Particulate organic carbon POC",
        ]
        element_list = ["O", "O", "N", "N", "N", "N", "N", "N", "P", "P", "Si", "C", "C"]
        element = ""
        if name in parameter_name_list:
            element = element_list[parameter_name_list.index(name)]

        pattern = r"(\d+\.\d+|\d+)([^\d]*)"
        matches = re.search(pattern, lmqnt_str.strip())
        item = [
            matches.group(1) if matches.group(1) else float("nan"),
            matches.group(2).strip() if matches.group(2) else "",
        ]

        if (element != "") & (str(item[1]) == "ug/l"):
            lmqnt = self._conversions_to_shark_units(float(item[0]), element)
            lmqnt_unit = "umol/l"
        elif (element != "") & (str(item[1]) == "mg/l"):
            if element == "O":
                lmqnt = self._conversions_to_shark_units(float(item[0]), element)
                lmqnt_unit = "ml/l"
            else:
                lmqnt = self._conversions_to_shark_units(float(item[0]) * 1000, element)
                lmqnt_unit = "umol/l"
        else:
            lmqnt = float(item[0])
            lmqnt_unit = str(item[1])
        return lmqnt, lmqnt_unit

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for param in data_holder.data["parameter"].unique():
            param_bool = data_holder.data["parameter"] == param
            temp = data_holder.data.loc[param_bool, "quantification_limit"]
            lmqnt_str = temp.iloc[0]
            if not lmqnt_str or lmqnt_str.isspace():
                data_holder.data.loc[param_bool, "LMQNT_VAL"] = float("nan")
                continue
            temp = data_holder.data.loc[param_bool, "unit"]
            unit = str(temp.iloc[0])
            name = param
            lmqnt, lmqnt_unit = self._parse_lmqnt(lmqnt_str, name)

            if lmqnt_unit == "":
                data_holder.data.loc[param_bool, "LMQNT_VAL"] = (
                    lmqnt if lmqnt is not None else float("nan")
                )
            elif lmqnt_unit == unit:
                data_holder.data.loc[param_bool, "LMQNT_VAL"] = (
                    lmqnt if lmqnt is not None else float("nan")
                )
