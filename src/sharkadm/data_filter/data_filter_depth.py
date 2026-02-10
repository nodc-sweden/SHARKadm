import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.data_filter.base import PolarsDataFilter
from sharkadm.sharkadm_logger import adm_logger


class PolarsDataFilterDeepestDepthAtEachVisit(PolarsDataFilter):
    visit_id_columns = (
        "shark_sample_id_md5",
        "visit_date",
        "sample_date",
        "sample_time",
        "sample_latitude_dd",
        "sample_longitude_dd",
        "platform_code",
        "visit_id",
    )
    potential_depth_columns = (
        "sample_depth_m",
        "sample_max_depth_m",
    )

    _temp_id_column = "temp_visit_id_column"
    _temp_boolean_column = "is_deepest_for_visit"

    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series:
        depth_column = None
        for col in self.potential_depth_columns:
            if col in data_holder.data.columns:
                depth_column = col
                break
        if not depth_column:
            raise Exception("No depth column found in data")
        id_cols = [col for col in self.visit_id_columns if col in data_holder.data]
        data_holder.data = data_holder.data.with_columns(
            pl.concat_str([pl.col(col) for col in id_cols]).alias(self._temp_id_column),
            pl.lit(False).alias(self._temp_boolean_column),
        )
        for _id, df in data_holder.data.group_by(id_cols):
            max_depth = max(df[depth_column], key=float)
            id_str = "".join(_id)
            data_holder.data = data_holder.data.with_columns(
                pl.when(
                    (pl.col(depth_column) == max_depth)
                    & (pl.col(self._temp_id_column) == id_str)
                )
                .then(pl.lit(True))
                .otherwise(pl.col(self._temp_boolean_column))
                .alias(self._temp_boolean_column)
            )

        mask = data_holder.data.select(pl.col(self._temp_boolean_column)).to_series()
        data_holder.data = data_holder.data.drop(
            [self._temp_boolean_column, self._temp_id_column]
        )
        return mask


class PolarsDataFilterDepthDeeperThanWaterDepth(PolarsDataFilter):
    water_depth_column = "water_depth_m"

    def __init__(self, depth_column: str, margin: float | int = 0):
        super().__init__(depth_column=depth_column, margin=margin)
        self.depth_column = depth_column
        self.margin = margin

    def _get_filter_mask(
        self,
        data_holder: PolarsDataHolder,
    ) -> pl.Series:
        if self.depth_column not in data_holder.data.columns:
            raise Exception(f"Missing column {self.depth_column}")
        return data_holder.data.select(
            (pl.col(self.depth_column).cast(float) + self.margin)
            >= pl.col(self.water_depth_column).cast(float)
        ).to_series()


class PolarsDataFilterDepthDeeperThanIobisDepth(PolarsDataFilter):
    ref_depth_column = "iobis_depth"

    def __init__(self, depth_column: str, margin: float | int = 0):
        super().__init__(depth_column=depth_column, margin=margin)
        self.depth_column = depth_column
        self.margin = margin

    def _get_filter_mask(
        self,
        data_holder: PolarsDataHolder,
    ) -> pl.Series:
        if self.depth_column not in data_holder.data.columns:
            adm_logger.log_transformation(f"Missing column {self.depth_column}",
                                          level=adm_logger.ERROR)
            return pl.Series()
        return data_holder.data.select(
            (pl.col(self.depth_column).cast(float) + self.margin)
            >= pl.col(self.ref_depth_column).cast(float)
        ).to_series()
