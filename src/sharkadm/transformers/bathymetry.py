import nodc_geography
import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.transformers.base import PolarsTransformer


class PolarsAddEmodnetBathymetryDepth(PolarsTransformer):
    col_to_set = "emodnet_bathymetry_depth"

    @staticmethod
    def get_transformer_description() -> str:
        return (f"Adding depth from EMODnet Bathymetry "
                f"to column {PolarsAddEmodnetBathymetryDepth.col_to_set}")

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        lat_col = "sample_latitude_dd"
        lon_col = "sample_longitude_dd"
        self._add_empty_col_to_set(data_holder)
        for (lat, lon), df in data_holder.data.group_by([lat_col, lon_col]):
            try:
                emodnet_depth = nodc_geography.get_bathymetry_depth_at_position(float(lat),
                                                                                float(lon))
                # print(f"{lat=} {lon=} {emodnet_depth=}")
                data_holder.data = data_holder.data.with_columns(
                    pl.when(pl.col(lat_col) == lat, pl.col(lon_col) == lon)
                    .then(pl.lit(emodnet_depth))
                    .otherwise(pl.col(self.col_to_set))
                    .alias(self.col_to_set)
                )
            except FileNotFoundError:
                self._log(f"Could not find EMODnet depth for {lat}, {lon}")
                continue

