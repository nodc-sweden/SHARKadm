import pandas as pd
import polars as pl

from ..data import PolarsDataHolder
from .base import PolarsTransformer


# TODO: This is still written for pandas
class LongToWide(PolarsTransformer):
    valid_data_types = ("physicalchemical",)
    valid_data_formats = ("row",)

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds visit key column"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        non_unique_columns = ["sample_orderer_name_en"]

        not_meta_columns = ["parameter", "value", "unit", "depth"]
        meta_columns = [
            col for col in data_holder.data.columns if col not in not_meta_columns
        ]

        #        t0 = time.time()
        merge_key = ["visit_date", "sample_time", "sample_depth_m"]
        unique_pars = sorted(set(data_holder.data["parameter"]))
        unit_mapping = {}
        data_list = []
        for key, key_df in data_holder.data.groupby(merge_key):
            meta_data = []
            par_data = []
            for meta_col in meta_columns:
                if meta_col in non_unique_columns:
                    meta_value = ", ".join(sorted(set(key_df[meta_col])))
                else:
                    meta_value = key_df[meta_col].values[0]
                meta_data.append(meta_value)
            for par in unique_pars:
                row = key_df[key_df["parameter"] == par]
                if not len(row):
                    par_data.extend(["", ""])
                else:
                    par_data.extend(
                        [row["value"].values[0], row["quality_flag"].values[0]]
                    )
                    unit_mapping[par] = row["unit"].values[0]
            data_list.append(meta_data + par_data)

        par_columns = []
        for par in unique_pars:
            unit = unit_mapping[par]
            if par in data_holder.not_mapped_columns:
                qpar = f"{data_holder.original_qf_column_prefix}{par}"
            else:
                qpar = f"QFLAG.{par}"
                par = f"{par}.{unit}"
            par_columns.append(par)
            par_columns.append(qpar)

        data_holder.data = pd.DataFrame(
            data=data_list, columns=meta_columns + par_columns
        )


class PolarsLongToWide(PolarsTransformer):
    valid_data_formats = "row"

    @staticmethod
    def get_transformer_description() -> str:
        return "Transposes data from row data to column data"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        df = data_holder.data
        self._metadata_columns = []

        df2 = data_holder.data.select(
            ["row_number", "parameter", "value", "quality_flag", "unit"]
        ).pivot(
            index="row_number",
            columns="parameter",
            values=["value", "quality_flag", "unit"],
        )

        rename_dict = {
            col: col.replace("value_", "COPY_VARIABLE.")
            for col in df2.columns
            if "value" in col
        }
        df2 = df2.rename(rename_dict)

        rename_dict = {
            col: col.replace("quality_flag_", "QFLAG.")
            for col in df2.columns
            if "quality_flag" in col
        }
        df2 = df2.rename(rename_dict)

        unit_mapping = {
            "Secchi depth": "m",
            "Pressure CTD": "dbar",
            "Pressure": "dbar",
            "Temperature bottle": "C",
            "Temperature CTD": "C",
            "Salinity bottle": "o/oo psu",
            "Salinity CTD": "o/oo psu",
            "Conductivity_25 bottle": "mS/m",
            "Conductivity CTD": "mS/m",
            "Dissolved oxygen O2 bottle": "ml/l",
            "Dissolved oxygen O2 CTD": "ml/l",
            "Hydrogen sulphide H2S": "umol/l",
            "pH": "",
            "pH Laboratory": "",
            "Temperature pH Laboratory": "C",
            "Alkalinity": "mmol/kg",
            "Alkalinity_2": "mmol/l",
            "Phosphate PO4-P": "umol/l",
            "Total phosphorus Tot-P": "umol/l",
            "Nitrite NO2-N": "umol/l",
            "Nitrate NO3-N": "umol/l",
            "Nitrite+Nitrate NO2+NO3-N": "umol/l",
            "Ammonium NH4-N": "umol/l",
            "Total Nitrogen Tot-N": "umol/l",
            "Silicate SiO3-Si": "umol/l",
            "Humus": "ug/l",
            "Chlorophyll-a bottle": "ug/l",
            "Dissolved organic carbon DOC": "umol/l",
            "Particulate organic carbon POC": "umol/l",
            "Total organic carbon TOC": "mg/l",
            "Particulate organic nitrogen PON": "umol/l",
            "Current direction": "deca degrees",
            "Current velocity": "cm/s",
            "Lignin": "mg/l",
            "Yellow substance": "1/l",
            "Aluminium": "ug/l",
            "Urea": "umol/l",
            "Coloured dissolved organic matter CDOM": "1/m",
            "Turbidity TURB": "FNU",
        }

        rename_dict = {}

        for col in df2.columns:
            if col.startswith("COPY_VARIABLE."):
                parameter = col.replace("COPY_VARIABLE.", "")
                unit = unit_mapping.get(parameter, "")
                new_col_name = f"{col}.{unit}"
                rename_dict[col] = new_col_name

        df2 = df2.rename(rename_dict)

        df2 = df2.select([col for col in df2.columns if "unit" not in col.lower()])

        not_meta_columns = [
            "row_number",
            "parameter",
            "value",
            "quality_flag",
            "unit",
            "reported_parameter",
            "reported_value",
            "reported_quality_flag",
            "reported_unit",
        ]

        _metadata_columns = [col for col in df.columns if col not in not_meta_columns]

        df_filtered = df.select(
            ["row_number"] + [col for col in _metadata_columns if col in df.columns]
        )
        df3 = df_filtered.join(df2, on="row_number", how="left")
        df3 = df3.group_by("row_number").first()
        df3 = df3.with_columns(pl.col("row_number").cast(pl.Int64))
        df3 = df3.sort("row_number")
        df3 = df3.select([col for col in df3.columns if "_right" not in col.lower()])

        data_holder.data = df3
