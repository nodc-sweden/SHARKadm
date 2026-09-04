import polars as pl

from sharkadm.sharkadm_logger import adm_logger

from ..data import PolarsDataHolder
from . import PolarsTransformer

delivery_data = None
try:
    from shark_metadata import delivery_data
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. '
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class PolarsAddSharkMetadataInfo(PolarsTransformer):
    exclude_columns = ("stations",)

    def __init__(
        self,
        prefix: str = "shark_metadata",
        overwrite: bool = False,
        include_all: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._prefix = prefix
        self._overwrite = overwrite
        self.exclude_columns = [] if include_all else self.exclude_columns

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds info from metadata_metadata. "
            f"Excludes columns {PolarsAddSharkMetadataInfo.exclude_columns} if "
            f"include_all not set to True."
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        try:
            ddata = delivery_data.DeliveryData(data_holder.data)
            meta = ddata.generate_metadata()
        except Exception as e:
            self._log(f"Could not get shark_metadata: {e}", level=adm_logger.WARNING)
            return
        exps = []
        for key, value in meta.items():
            if key in self.exclude_columns:
                continue
            for col_name, val in get_col_and_value(key, value).items():
                if col_name in self.exclude_columns:
                    continue
                if not val:
                    continue
                if self._prefix:
                    col_name = f"{self._prefix}_{col_name}"
                exps.append(pl.lit(val).alias(col_name))
        data_holder.data = data_holder.data.with_columns(exps)


def get_col_and_value(key: str, value: str | list | dict) -> dict[str, str]:
    if type(value) is str:
        return {key: value}
    if type(value) is list:
        return {key: ";".join(value)}
    if type(value) is dict:
        data = dict()
        for k, v in value.items():
            data.update(get_col_and_value(f"{key}-{k}", v))
        return data
    return {key: ""}
