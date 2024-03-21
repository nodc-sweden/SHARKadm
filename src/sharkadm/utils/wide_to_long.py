import pandas as pd


class WideToLong:
    """
    Converts a pandas dataframe from wide to long format.
    """
    def __init__(self, df: pd.DataFrame, prefix: str = 'COPY_VARIABLE', var_col_name: str = 'parameter', unit_col_name: str = 'unit'):
        self._df = df.copy()
        self._prefix = prefix
        self._var_col_name = var_col_name
        self._unit_col_name = unit_col_name

    @property
    def data(self) -> pd.DataFrame:
        return self._df

    def convert(self) -> None:
        self._remove_columns()
        self._wide_to_long()
        self._cleanup()

    def _remove_columns(self) -> None:
        for col in ['parameter', 'value', 'unit']:
            if col in self._df.columns:
                self._df.drop(col, axis=1, inplace=True)

    def _wide_to_long(self) -> None:
        self._df = self._df.melt(id_vars=[col for col in self._df.columns if not col.startswith(self._prefix)],
                                                 var_name=self._var_col_name)

    def _cleanup(self) -> None:
        self._df[self._unit_col_name] = self._df[self._var_col_name].apply(self._fix_unit)
        self._df[self._var_col_name] = self._df[self._var_col_name].apply(self._fix_parameter)

        self._df = self._df.fillna('')
        self._df.reset_index(inplace=True, drop=True)



    def _fix_unit(self, x) -> str:
        return x.split('.')[-1]

    def _fix_parameter(self, x) -> str:
        return x.split('.')[1]


def wide_to_long(df: pd.DataFrame) -> pd.DataFrame:
    wtl = WideToLong(df)
    wtl.convert()
    return wtl.data
