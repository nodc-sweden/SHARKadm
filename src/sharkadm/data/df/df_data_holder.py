import polars as pl

from sharkadm.config import ImportMatrixMapper
from sharkadm.data.data_holder import PolarsDataHolder


class PolarsDataFrameDataHolder(PolarsDataHolder):
    def __init__(
        self,
        df: pl.DataFrame,
        import_matrix_key: str = "",
        header_mapper: ImportMatrixMapper = None,
        data_type: str = "unknown",
        data_structure: str = "row",
    ):
        assert isinstance(df, pl.DataFrame), (
            f"Invalid input datatype {type(df)}, must be polars.DataFrame"
        )
        super().__init__(data_type=data_type, data_structure=data_structure)

        self._import_matrix_key = import_matrix_key
        self._header_mapper = header_mapper
        if self._import_matrix_key:
            self._header_mapper = self.data_type_obj.get_mapper(self._import_matrix_key)

        self._data: pl.DataFrame = df
        self._dataset_name = "From polars dataframe"

    def get_data_holder_description(self) -> str:
        return "Data holder holding given polars dataframe"

    @property
    def dataset_name(self) -> str:
        return self._dataset_name
