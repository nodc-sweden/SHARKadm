import polars as pl

from sharkadm.sharkadm_logger import adm_logger

from .base import DataHolderProtocol, Transformer, PolarsTransformer

try:
    from nodc_codes import get_translate_codes_object

    _translate_codes = get_translate_codes_object()
except ModuleNotFoundError as e:
    _translate_codes = None
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. '
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class _AddCodes(Transformer):
    source_cols = ("",)
    col_to_set = ""
    lookup_key = ""
    lookup_field = ""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._loaded_code_info = {}

    @staticmethod
    def get_transformer_description() -> str:
        return ""

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        source_col = ""
        for col in self.source_cols:
            if col in data_holder.data.columns:
                source_col = col
                break
        if not source_col:
            adm_logger.log_transformation(
                f"None of the source columns {self.source_cols} found "
                f"when trying to set {self.col_to_set}",
                level=adm_logger.WARNING,
            )
            return
        if self.col_to_set not in data_holder.data.columns:
            data_holder.data[self.col_to_set] = ""

        for code, df in data_holder.data.groupby(source_col):
            len_df = len(df)
            code = str(code)
            names = []
            info = _translate_codes.get_info(self.lookup_field, code.strip())
            if info:
                names = [info[self.lookup_key]]
                adm_logger.log_transformation(
                    f"{source_col} {code} translated to {info[self.lookup_key]} "
                    f"({len_df} places)",
                    level=adm_logger.INFO,
                )
            else:
                for part in code.split(","):
                    part = part.strip()
                    info = _translate_codes.get_info(self.lookup_field, part)
                    if info:
                        names.append(info[self.lookup_key])
                        adm_logger.log_transformation(
                            f"{source_col} {part} translated to {info[self.lookup_key]} "
                            f"({len_df} places)",
                            level=adm_logger.INFO,
                        )
                    else:
                        adm_logger.log_transformation(
                            f"Could not find information for {source_col}: {part}",
                            level=adm_logger.WARNING,
                        )
            data_holder.data.loc[df.index, self.col_to_set] = ", ".join(names)


class _PolarsAddCodes(PolarsTransformer):
    source_cols = ("",)
    col_to_set = ""
    lookup_key = ""
    lookup_field = ""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._loaded_code_info = {}

    @staticmethod
    def get_transformer_description() -> str:
        return ""

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        source_col = ""
        for col in self.source_cols:
            if col in data_holder.data.columns:
                source_col = col
                break
        print(f"{source_col=}")
        if not source_col:
            adm_logger.log_transformation(
                f"None of the source columns {self.source_cols} found "
                f"when trying to set {self.col_to_set}",
                level=adm_logger.WARNING,
            )
            return
        if self.col_to_set not in data_holder.data.columns:
            data_holder.data = data_holder.data.with_columns(
                pl.lit("").alias(self.col_to_set)
            )

        # operations = []
        for (code,), df in data_holder.data.group_by(source_col):
            len_df = len(df)
            code = str(code)
            names = []
            info = _translate_codes.get_info(self.lookup_field, code.strip())
            if info:
                names = [info[self.lookup_key]]
                adm_logger.log_transformation(
                    f"{source_col} {code} translated to {info[self.lookup_key]} "
                    f"({len_df} places)",
                    level=adm_logger.INFO,
                )
            else:
                for part in code.split(","):
                    part = part.strip()
                    info = _translate_codes.get_info(self.lookup_field, part)
                    if info:
                        names.append(info[self.lookup_key])
                        adm_logger.log_transformation(
                            f"{source_col} {part} translated to {info[self.lookup_key]} "
                            f"({len_df} places)",
                            level=adm_logger.INFO,
                        )
                    else:
                        adm_logger.log_transformation(
                            f"Could not find information for {source_col}: {part}",
                            level=adm_logger.WARNING,
                        )
            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(source_col) == code)
                .then(pl.lit(", ".join(names)))
                .otherwise(pl.col(self.col_to_set))
                .alias(self.col_to_set)
            )
            # op = pl.when(pl.col(source_col) == code).then(pl.lit(", ".join(names))).alias(self.col_to_set)
        #     operations.append(op)
        # data_holder.data = data_holder.data.with_columns(operations)
        # data_holder.data.loc[df.index, self.col_to_set] = ", ".join(names)


class _AddCodesLab(_AddCodes):
    lookup_field = "LABO"


class _AddCodesProj(_AddCodes):
    lookup_field = "project"


class _PolarsAddCodesLab(_PolarsAddCodes):
    lookup_field = "LABO"


class _PolarsAddCodesProj(_PolarsAddCodes):
    lookup_field = "project"
