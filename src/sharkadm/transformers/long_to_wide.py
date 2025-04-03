from .base import Transformer, DataHolderProtocol
import pandas as pd


class LongToWide(Transformer):
    valid_data_types = ["physicalchemical"]
    valid_data_formats = ["row"]

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds visit key column"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        non_unique_columns = ["sample_orderer_name_en"]

        not_meta_columns = ["parameter", "value", "unit", "depth"]
        meta_columns = [
            col for col in data_holder.data.columns if col not in not_meta_columns
        ]

        import time

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


#        print(time.time() - t0)
