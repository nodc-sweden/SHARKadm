import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.transformers.base import PolarsTransformer
from sharkadm.config import trophic_type_smhi
from sharkadm.config import get_trophic_type_smhi_object
from sharkadm.sharkadm_logger import adm_logger


class PolarsSetTrophicTypeSMHI(PolarsTransformer):
    valid_data_types = ("phytoplankton", )
    source_col_name = "bvol_scientific_name"
    source_col_size_class = "bvol_size_class"
    col_to_set = "trophic_type_code"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Sets updated trophic_type from {PolarsSetTrophicTypeSMHI.source_col_name} "
            f"and {PolarsSetTrophicTypeSMHI.source_col_size_class}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        tt_object = get_trophic_type_smhi_object()
        mapper = tt_object.get_mapper()
        if self.col_to_set not in data_holder.data.columns:
            self._add_empty_col_to_set(data_holder)
        for (name, size, value), df in data_holder.data.group_by([self.source_col_name,
                                                           self.source_col_size_class,
                                                           self.col_to_set]):
            key = trophic_type_smhi.NAME_AND_SIZE_SEPARATOR.join([name, size])
            new_value = mapper.get(key)
            if new_value == value:
                continue
            if new_value:
                data_holder.data = data_holder.data.with_columns(
                    pl.when((pl.col(self.source_col_name) == name) &
                            (pl.col(self.source_col_size_class) == size))
                    .then(pl.lit(new_value))
                    .otherwise(pl.col(self.col_to_set))
                    .alias(self.col_to_set)
                )
                self._log(f"Reported trophic type replaced. "
                          f"Scientific name/size: {name}/{size} "
                          f"Old: {value} "
                          f"New {new_value} ({len(df)} places)", level=adm_logger.INFO)


