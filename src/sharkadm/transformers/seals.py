import polars as pl

from sharkadm.sharkadm_logger import adm_logger
from sharkadm.sharkadm_operator import OperatorInfo

from ..data import PolarsDataHolder
from .base import PolarsTransformer

VISIT_COLUMNS = [
    "sample_latitude_dd",
    "sample_longitude_dd",
    "datetime",
    "reported_station_name",
]


class AddNrCountedSealsIfMissing(PolarsTransformer):
    valid_data_types = ("greyseal",)
    valid_data_structures = ("row",)

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds a '# counted' row if missing."

    def _transform(self, data_holder: PolarsDataHolder) -> OperatorInfo:
        rows_to_add = []
        for (lat, lon, dtime, statn), df in data_holder.data.group_by(VISIT_COLUMNS):
            if "# counted" in df["parameter"]:
                continue
            try:
                sum_seals = sum(df["value"].cast(pl.Int64))
                row = df.to_dicts()[0]
                row["parameter"] = "# counted"
                row["value"] = str(sum_seals)
                rows_to_add.append(row)
            except pl.exceptions.InvalidOperationError:
                self._log(
                    f"Could not sum up number of seals for visit {statn} {dtime}",
                    level=adm_logger.WARNING,
                )
                continue

        if not rows_to_add:
            return OperatorInfo(msg="No missing '# counted' found")
        self._log(
            f"Adding sum of counted seals to missing parameter '# counted'"
            f" ({len(rows_to_add)} places)",
            level=adm_logger.INFO,
        )
        data_holder.data = pl.concat([data_holder.data, pl.DataFrame(rows_to_add)])
        return OperatorInfo(msg=f"{len(rows_to_add)} missing '# counted' found and fixed")
