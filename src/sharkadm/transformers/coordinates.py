from sharkadm import data_filter

from ..data import PolarsDataHolder
from .base import PolarsTransformer


class PolarsSetBoundingBox(PolarsTransformer):

    def __init__(self,
                 lat_min: float | str = None,
                 lat_max: float | str = None,
                 lon_min: float | str = None,
                 lon_max: float | str = None,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        self._lat_min = kwargs.get("min_lat", lat_min)
        self._lat_max = kwargs.get("max_lat", lat_max)
        self._lon_min = kwargs.get("min_lon", lon_min)
        self._lon_max = kwargs.get("max_lon", lon_max)

    @staticmethod
    def get_transformer_description() -> str:
        return "Filtering data to given bounding box."

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        bb_filter = data_filter.PolarsDataFilterBoundingBox(
            lat_min=self._lat_min,
            lat_max=self._lat_max,
            lon_min=self._lon_min,
            lon_max=self._lon_max,
        )
        mask = bb_filter.get_filter_mask(data_holder)
        data_holder.data = data_holder.data.remove(~mask)
