import polars as pl

from ..data import PolarsDataHolder
from ..utils.add_column import add_float_column
from .base import PolarsTransformer


class AddCalculatedSamplerArea(PolarsTransformer):
    sampler_area_col = "sampler_area_m2"
    section_distance_start_col = "section_distance_start_m"
    section_distance_end_col = "section_distance_end_m"
    transect_width_col = "transect_width_m"

    section_distance_start_col_float = "section_distance_start_m_float"
    section_distance_end_col_float = "section_distance_end_m_float"
    transect_width_col_float = "transect_width_m_float"

    @staticmethod
    def get_transformer_description() -> str:
        return "Calculates sampler area"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        missing = False
        for col in [
            self.section_distance_start_col,
            self.section_distance_end_col,
            self.transect_width_col,
        ]:
            if col not in data_holder.data.columns:
                self._log(f"Missing key: {col}", level="warning")
                missing = True
        if missing:
            return

        add_float_column(
            data_holder.data,
            self.section_distance_start_col,
            column_name=self.section_distance_start_col_float,
        )
        add_float_column(
            data_holder.data,
            self.section_distance_end_col,
            column_name=self.section_distance_end_col_float,
        )
        add_float_column(
            data_holder.data,
            self.transect_width_col,
            column_name=self.transect_width_col_float,
        )
        data_holder.data = data_holder.data.with_columns(
            (
                pl.col(self.section_distance_end_col_float)
                - pl.col(self.section_distance_start_col_float)
            )
            * pl.col(self.transect_width_col_float).cast(str).alias(self.sampler_area_col)
        )
