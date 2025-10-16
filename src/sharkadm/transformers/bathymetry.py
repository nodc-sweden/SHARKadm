import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers.base import PolarsTransformer

try:
    import nodc_geography
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. '
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class PolarsAddEmodnetBathymetryDepth(PolarsTransformer):
    col_to_set = "emodnet_bathymetry_depth"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adding depth from EMODnet Bathymetry "
            f"to column {PolarsAddEmodnetBathymetryDepth.col_to_set}"
        )

    # def _transform(self, data_holder: PolarsDataHolder) -> None:
    #     lat_col = "sample_latitude_dd"
    #     lon_col = "sample_longitude_dd"
    #     self._add_empty_col_to_set(data_holder)
    #     for (lat, lon), df in data_holder.data.group_by([lat_col, lon_col]):
    #         try:
    #             emodnet_depth = nodc_geography.get_bathymetry_depth_at_position(
    #                 float(lat), float(lon)
    #             )
    #             data_holder.data = data_holder.data.with_columns(
    #                 pl.when(pl.col(lat_col) == lat, pl.col(lon_col) == lon)
    #                 .then(pl.lit(emodnet_depth))
    #                 .otherwise(pl.col(self.col_to_set))
    #                 .alias(self.col_to_set)
    #             )
    #         except FileNotFoundError:
    #             self._log(f"Could not find EMODnet depth for {lat}, {lon}")
    #             continue

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        lat_col = "sample_latitude_dd"
        lon_col = "sample_longitude_dd"
        # self._add_empty_col_to_set(data_holder)
        for (lat, lon), df in data_holder.data.group_by([lat_col, lon_col]):
            try:
                emodnet_info = nodc_geography.get_bathymetry_depth_at_position(
                    float(lat), float(lon)
                )
                # print("=" * 50)
                # print(f"{emodnet_info=}")
                boolean = (pl.col(lat_col) == lat) & (pl.col(lon_col) == lon)
                # print("-"*50)
                # print(data_holder.data.filter(boolean))
                for key, value in emodnet_info.items():
                    if key == "z":
                        key = "depth"
                        value = -value
                    col = f"emodnet_bathymetry_{key}"

                    self._add_empty_col(data_holder, col)
                    data_holder.data = data_holder.data.with_columns(
                        pl.when(boolean)
                        .then(pl.lit(value))
                        .otherwise(pl.col(col))
                        .alias(col)
                    )
            except FileNotFoundError:
                self._log(f"Could not find EMODnet depth for {lat}, {lon}")
                continue
