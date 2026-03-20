import polars as pl

from sharkadm.utils import iobis

from ..data import PolarsDataHolder
from .base import PolarsTransformer


class AddSampleMinAndMaxDepth(PolarsTransformer):
    depth_col = "sample_depth_m"
    min_depth_col = "sample_min_depth_m"
    max_depth_col = "sample_max_depth_m"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {AddSampleMinAndMaxDepth.min_depth_col} and "
            f"{AddSampleMinAndMaxDepth.max_depth_col} if missing. "
            f"Depth is set from {AddSampleMinAndMaxDepth.depth_col}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        for col in [self.min_depth_col, self.max_depth_col]:
            nr_rows = len(data_holder.data.filter(pl.col(col) == ""))
            if not nr_rows:
                continue
            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(col) == "")
                .then(pl.col(self.depth_col))
                .otherwise(pl.col(col))
                .alias(col)
            )
            self._log(f"{col} set from {self.depth_col} ({nr_rows} places)")


# class AddSectionStartAndEndDepth(Transformer):
#     depth_col = "sample_depth_m"
#     min_depth_col = "sample_min_depth_m"
#     max_depth_col = "sample_max_depth_m"
#     section_start_depth_col = "section_start_depth_m"
#     section_end_depth_col = "section_end_depth_m"
#
#     @staticmethod
#     def get_transformer_description() -> str:
#         return "Adds section start and end depth"
#
#     def _transform(self, data_holder: PolarsDataHolder) -> None:
#         data_holder.data[self.min_depth_par] = data_holder.data.apply(
#             lambda row: self.add_min_depth(row), axis=1
#         )
#         data_holder.data[self.max_depth_par] = data_holder.data.apply(
#             lambda row: self.add_max_depth(row), axis=1
#         )
#
#     def add_min_depth(self, row: pd.Series) -> str:
#         if row.get(self.min_depth_par):
#             return row[self.min_depth_par]
#         if row.get(self.depth_par):
#             self._log(f"Added {self.min_depth_par} from {self.depth_par}")
#             return row[self.depth_par]
#         if row.get(self.section_end_depth_par):
#             self._log(f"Added {self.min_depth_par} from {self.section_end_depth_par}")
#             return row[self.section_end_depth_par]
#
#     def add_max_depth(self, row: pd.Series) -> str:
#         if row.get(self.max_depth_par):
#             return row[self.max_depth_par]
#         if row.get(self.depth_par):
#             self._log(f"Added {self.max_depth_par} from {self.depth_par}")
#             return row[self.depth_par]
#         if row.get(self.section_start_depth_par):
#             self._log(f"Added {self.max_depth_par} from {self.section_start_depth_par}")
#             return row[self.section_start_depth_par]


class ReorderSampleMinAndMaxDepth(PolarsTransformer):
    invalid_data_holders = ("EpibenthosMartransArchiveDataHolder",)
    min_depth_col = "sample_min_depth_m"
    max_depth_col = "sample_max_depth_m"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Reorders sample {ReorderSampleMinAndMaxDepth.min_depth_col} "
            f"and {ReorderSampleMinAndMaxDepth.max_depth_col} "
            f"if they are in wrong order."
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        boolean = pl.col(self.min_depth_col).cast(float) > pl.col(
            self.max_depth_col
        ).cast(float)
        nr_rows = len(data_holder.data.filter(boolean))
        if not nr_rows:
            return
        temp_col = "_min_temp_col"
        data_holder.data = data_holder.data.with_columns(
            pl.col(self.min_depth_col).alias(temp_col)
        )
        data_holder.data = data_holder.data.with_columns(
            pl.when(boolean)
            .then(pl.col(self.max_depth_col))
            .otherwise(pl.col(self.min_depth_col))
            .alias(self.min_depth_col)
        )

        data_holder.data = data_holder.data.with_columns(
            pl.when(boolean)
            .then(pl.col(temp_col))
            .otherwise(pl.col(self.max_depth_col))
            .alias(self.max_depth_col)
        )
        data_holder.data = data_holder.data.drop(temp_col)


class PolarsAddIObisDepth(PolarsTransformer):
    col_to_set = "iobis_depth"
    lat_col = "sample_latitude_dd"
    lon_col = "sample_longitude_dd"

    def __init__(self, verify_ssl: bool = True, **kwargs):
        self._verify_ssl = verify_ssl
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds {PolarsAddIObisDepth.col_to_set} from iobis api."

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        self._add_empty_col_to_set(data_holder)
        for (lat, lon), df in data_holder.data.group_by([self.lat_col, self.lon_col]):
            depth = iobis.get_obis_depth(lat, lon, verify_ssl=self._verify_ssl)
            if not depth:
                continue
            data_holder.data = data_holder.data.with_columns(
                pl.when(
                    [
                        pl.col(self.lat_col) == lat,
                        pl.col(self.lon_col) == lon,
                    ]
                )
                .then(pl.lit(str(depth)))
                .otherwise(pl.col(self.col_to_set))
                .alias(self.col_to_set)
            )
