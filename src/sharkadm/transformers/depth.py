import pandas as pd
import polars as pl

from sharkadm.utils import iobis

from ..data import PolarsDataHolder
from .base import DataHolderProtocol, PolarsTransformer, Transformer


class AddSampleMinAndMaxDepth(Transformer):
    depth_par = "sample_depth_m"
    min_depth_par = "sample_min_depth_m"
    max_depth_par = "sample_max_depth_m"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds min and max depth if missing. Depth is set from sample_depth_m"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for par in [self.min_depth_par, self.max_depth_par]:
            data_holder.data[par] = data_holder.data.apply(
                lambda row, p=par: self.add_if_missing(p, row), axis=1
            )

    def add_if_missing(self, par: str, row: pd.Series) -> str:
        if row.get(par):
            return row[par]
        if row.get(self.depth_par):
            self._log(f"Added {par} from {self.depth_par}")
            return row[self.depth_par]


class AddSectionStartAndEndDepth(Transformer):
    depth_par = "sample_depth_m"
    min_depth_par = "sample_min_depth_m"
    max_depth_par = "sample_max_depth_m"
    section_start_depth_par = "section_start_depth_m"
    section_end_depth_par = "section_end_depth_m"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds section start and end depth"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.min_depth_par] = data_holder.data.apply(
            lambda row: self.add_min_depth(row), axis=1
        )
        data_holder.data[self.max_depth_par] = data_holder.data.apply(
            lambda row: self.add_max_depth(row), axis=1
        )

    def add_min_depth(self, row: pd.Series) -> str:
        if row.get(self.min_depth_par):
            return row[self.min_depth_par]
        if row.get(self.depth_par):
            self._log(f"Added {self.min_depth_par} from {self.depth_par}")
            return row[self.depth_par]
        if row.get(self.section_end_depth_par):
            self._log(f"Added {self.min_depth_par} from {self.section_end_depth_par}")
            return row[self.section_end_depth_par]

    def add_max_depth(self, row: pd.Series) -> str:
        if row.get(self.max_depth_par):
            return row[self.max_depth_par]
        if row.get(self.depth_par):
            self._log(f"Added {self.max_depth_par} from {self.depth_par}")
            return row[self.depth_par]
        if row.get(self.section_start_depth_par):
            self._log(f"Added {self.max_depth_par} from {self.section_start_depth_par}")
            return row[self.section_start_depth_par]


class ReorderSampleMinAndMaxDepth(Transformer):
    invalid_data_holders = ("EpibenthosMartransArchiveDataHolder",)
    min_depth_par = "sample_min_depth_m"
    max_depth_par = "sample_max_depth_m"

    @staticmethod
    def get_transformer_description() -> str:
        return "Reorders sample min and max depth if they are in wrong order."

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        print(data_holder.data[[self.min_depth_par, self.max_depth_par]])
        data_holder.data[[self.min_depth_par, self.max_depth_par]] = (
            data_holder.data.apply(
                lambda row: self.reorder(row), axis=1, result_type="expand"
            )
        )

    def reorder(self, row: pd.Series) -> list[str, str]:
        if row[self.min_depth_par] > row[self.max_depth_par]:
            self._log(
                f"Changed value order of {self.min_depth_par} and {self.max_depth_par}"
            )
            return [row[self.max_depth_par], row[self.min_depth_par]]
        return [row[self.min_depth_par], row[self.max_depth_par]]


class PolarsAddIOdisDepth(PolarsTransformer):
    col_to_set = "iobis_depth"
    lat_col = "sample_latitude_dd"
    lon_col = "sample_longitude_dd"

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds {PolarsAddIOdisDepth.col_to_set} from iobis api."

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        self._add_empty_col_to_set(data_holder)
        for (lat, lon), df in data_holder.data.group_by([self.lat_col, self.lon_col]):
            depth = iobis.get_obis_depth(lat, lon)
            if not depth:
                continue
            print(lat, lon, depth)
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
