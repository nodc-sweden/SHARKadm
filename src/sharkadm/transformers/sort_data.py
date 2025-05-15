from sharkadm.sharkadm_logger import adm_logger

from .base import DataHolderProtocol, Transformer


class SortData(Transformer):
    sort_by_columns: tuple[str] = (
        "sample_date",
        "sample_time",
        "station_name",
        "sample_min_depth_m",
        "sample_max_depth_m",
        "scientific_name",
        "parameter",
    )
    ascending: bool | tuple[bool] = True

    def __init__(
        self,
        sort_by_columns: tuple[str] | None = None,
        ascending: bool | tuple[bool] | None = None,
    ) -> None:
        super().__init__()
        if sort_by_columns:
            self.sort_by_columns = sort_by_columns
        if ascending is not None:
            self.ascending = ascending

    @staticmethod
    def get_transformer_description() -> str:
        return (
            "Sorts data by: sample_date -> sample_time -> sample_min_depth_m -> "
            "sample_max_depth_m"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        print(
            "MISSING",
            f"{[col for col in self.sort_by_columns if col not in data_holder.data]}",
        )
        sort_by_columns = [
            col for col in self.sort_by_columns if col in data_holder.data.columns
        ]
        column_string = ", ".join(sort_by_columns)
        self._log(
            f"Sorting data based on columns: {column_string}", level=adm_logger.DEBUG
        )
        data_holder.data.sort_values(
            sort_by_columns, ascending=self.ascending, inplace=True
        )


class SortDataPlanktonImaging(SortData):
    valid_data_types = ("plankton_imaging",)

    sort_by_columns = (
        "image_verified_by",
        "sample_date",
        "sample_time",
        "station_name",
        "sample_min_depth_m",
        "sample_max_depth_m",
        "scientific_name",
        "parameter",
    )

    ascending = (
        True,
        True,
        True,
        True,
        True,
        True,
        False,
        True,
    )
