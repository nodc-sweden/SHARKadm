import datetime
import re

import polars as pl

from ..data import PolarsDataHolder
from ..sharkadm_logger import adm_logger
from .base import (
    DataHolderProtocol,
    PolarsDataHolderProtocol,
    PolarsTransformer,
    Transformer,
)

DATETIME_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d")


class FixTimeFormat(Transformer):
    time_cols = ("sample_time", "visit_time", "sample_endtime")

    @staticmethod
    def get_transformer_description() -> str:
        cols_str = ", ".join(FixTimeFormat.time_cols)
        return f"Reformat time values in columns: {cols_str}"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        self._buffer = {}
        for col in self.time_cols:
            if col not in data_holder.data:
                continue
            self._current_col = col
            data_holder.data[col] = data_holder.data[col].apply(self._fix)

    def _fix(self, x: str):
        xx = x.strip()
        if not xx:
            return ""
        fixed_x = self._buffer.get(xx)
        if fixed_x:
            return fixed_x
        xx = xx.replace(".", ":")

        parts = xx.split(":")
        if len(parts) == 3:
            xx = ":".join(parts[:2])
        if ":" in xx:
            if len(xx) > 5:
                self._log(
                    f"Cant handle time format {self._current_col} = {x}",
                    item=x,
                    level=adm_logger.ERROR,
                )
                return x
            xx = xx.zfill(5)
        else:
            if len(xx) > 4:
                self._log(
                    f"Cant handle time format in column {self._current_col}",
                    item=x,
                    level=adm_logger.ERROR,
                )
                return x
            xx = xx.zfill(4)
            xx = f"{xx[:2]}:{xx[2:]}"
        if int(xx.split(":")[1]) > 59:
            self._log(
                f"Invalid minutes in column {self._current_col}",
                item=x,
                level=adm_logger.ERROR,
            )
            return x
        self._buffer[x.strip()] = xx
        return xx


class FixDateFormat(Transformer):
    dates_to_check = ("sample_date", "visit_date", "analysis_date", "observation_date")

    from_format = "%Y%m%d"
    to_format = "%Y-%m-%d"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mapping = {}

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Changes date format from {FixDateFormat.from_format} "
            f"to {FixDateFormat.to_format}"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for col in self.dates_to_check:
            if col not in data_holder.data:
                continue
            tot_rows = 0
            for d, df in data_holder.data.groupby(col):
                dd = d.split()[0]
                msg = (
                    f"Changing date format in column {col} from {d} "
                    f"to {dd} in {len(df)} places"
                )
                try:
                    dd = datetime.datetime.strptime(dd, self.from_format).strftime(
                        self.to_format
                    )
                    data_holder.data.loc[df.index, col] = dd
                    self._log(msg, level=adm_logger.DEBUG)
                    tot_rows += len(df)
                except ValueError:
                    if d != dd:
                        data_holder.data.loc[df.index, col] = dd
                        self._log(msg, level=adm_logger.DEBUG)
                        tot_rows += len(df)
            if tot_rows:
                self._log(
                    f"Changing date format in column {col} "
                    f"in a total of {tot_rows} places",
                    level=adm_logger.INFO,
                )

    def _set_new_format(self, x: str):
        try:
            return self.mapping.setdefault(
                x,
                datetime.datetime.strptime(x, self.from_format).strftime(self.to_format),
            )
        except ValueError:
            return x


class AddSampleTime(Transformer):
    @staticmethod
    def get_transformer_description() -> str:
        return "Adding time format sample_time"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if "sample_time" in data_holder.data:
            data_holder.data["reported_sample_time"] = data_holder.data["sample_time"]
            if "visit_time" in data_holder.data:
                has_no_sample_time = data_holder.data["sample_time"].str.strip() == ""
                data_holder.data.loc[has_no_sample_time, "sample_time"] = (
                    data_holder.data.loc[has_no_sample_time, "visit_time"]
                )
        else:
            data_holder.data["sample_time"] = data_holder.data["visit_time"]


class PolarsAddSampleTime(PolarsTransformer):
    source_col = "visit_time"
    col_to_set = "sample_time"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adding {PolarsAddSampleTime.col_to_set} "
            f"from {PolarsAddSampleTime.source_col} if missing"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.col_to_set in data_holder.data:
            data_holder.data = data_holder.data.with_columns(
                pl.col(self.col_to_set).alias(f"reported_{self.col_to_set}")
            )
            if self.source_col in data_holder.data:
                has_no_sample_date = (
                    data_holder.data[self.col_to_set].str.strip_chars() == ""
                )
                data_holder.data = data_holder.data.with_columns(
                    pl.when(has_no_sample_date)
                    .then(pl.col(self.source_col))
                    .otherwise(pl.col(self.col_to_set))
                    .alias(self.col_to_set)
                )
        else:
            data_holder.data = data_holder.data.with_columns(
                pl.col(self.source_col).alias(self.col_to_set)
            )


class AddSampleDate(Transformer):
    @staticmethod
    def get_transformer_description() -> str:
        return "Adding sample_date if missing"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if "sample_date" in data_holder.data:
            data_holder.data["reported_sample_date"] = data_holder.data["sample_date"]
            if "visit_date" in data_holder.data:
                has_no_sample_date = data_holder.data["sample_date"].str.strip() == ""
                data_holder.data.loc[has_no_sample_date, "sample_date"] = (
                    data_holder.data.loc[has_no_sample_date, "visit_date"]
                )
        else:
            data_holder.data["sample_date"] = data_holder.data["visit_date"]


class PolarsAddSampleDate(PolarsTransformer):
    source_col = "visit_date"
    col_to_set = "sample_date"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adding {PolarsAddSampleDate.col_to_set} "
            f"from {PolarsAddSampleDate.source_col} if missing"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.col_to_set in data_holder.data:
            data_holder.data = data_holder.data.with_columns(
                pl.col(self.col_to_set).alias(f"reported_{self.col_to_set}")
            )
            if self.source_col in data_holder.data:
                has_no_sample_date = (
                    data_holder.data[self.col_to_set].str.strip_chars() == ""
                )
                data_holder.data = data_holder.data.with_columns(
                    pl.when(has_no_sample_date)
                    .then(pl.col(self.source_col))
                    .otherwise(pl.col(self.col_to_set))
                    .alias(self.col_to_set)
                )
        else:
            data_holder.data = data_holder.data.with_columns(
                pl.col(self.source_col).alias(self.col_to_set)
            )


class AddDatetime(Transformer):
    def __init__(
        self,
        date_source_column: str = "sample_date",
        time_source_column: str = "sample_time",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.date_source_column = date_source_column
        self.time_source_column = time_source_column

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds column datetime. Time is taken from sample_date"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data["datetime_str"] = data_holder.data[self.date_source_column]
        if self.time_source_column in data_holder.data.columns:
            data_holder.data["datetime_str"] = (
                data_holder.data["datetime_str"].str[:10]
                + " "
                + data_holder.data[self.time_source_column]
            )
        data_holder.data["datetime"] = data_holder.data["datetime_str"].apply(
            self.to_datetime
        )
        # data_holder.data.drop('date_and_time', axis=1, inplace=True)

    @staticmethod
    def to_datetime(x: str) -> datetime.datetime | str:
        for form in DATETIME_FORMATS:
            try:
                return datetime.datetime.strptime(x.strip(), form)
            except ValueError:
                continue
        return ""


class PolarsAddDatetime(PolarsTransformer):
    date_source_column: str = "sample_date"
    time_source_column: str = "sample_time"

    def __init__(
        self,
        date_source_column: str | None = None,
        time_source_column: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.date_source_column = date_source_column or self.date_source_column
        self.time_source_column = time_source_column or self.time_source_column

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds column datetime. Date and time is taken "
            f"from {PolarsAddDatetime.date_source_column} "
            f"and {PolarsAddDatetime.time_source_column}, if no other columns are given"
        )

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
        data_holder.data = data_holder.data.with_columns(
            datetime_str=pl.col(self.date_source_column)
        )
        if self.time_source_column in data_holder.data.columns:
            data_holder.data = data_holder.data.with_columns(
                pl.concat_str(
                    [
                        pl.col("datetime_str").str.slice(0, 10),
                        pl.col(self.time_source_column),
                    ],
                    separator=" ",
                ).alias("datetime_str")
            )
        data_holder.data = data_holder.data.with_columns(
            datetime=pl.col("datetime_str").str.to_datetime()
        )


class AddMonth(Transformer):
    month_columns = ("sample_month", "visit_month")

    @staticmethod
    def get_transformer_description() -> str:
        return (
            "Adds month column to data. Month is taken from the datetime column "
            "and will overwrite old value"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if "datetime" not in data_holder.data.columns:
            self._log("Missing column: datetime", level=adm_logger.WARNING)
            return
        for col in self.month_columns:
            data_holder.data[col] = data_holder.data["datetime"].apply(
                lambda x, dh=data_holder: self.get_month(x, dh)
            )

    def get_month(self, x: datetime.datetime, data_holder: DataHolderProtocol) -> str:
        if not x:
            self._log(f"Missing datetime in {data_holder}")
            return ""
        return str(x.month).zfill(2)


class AddReportedDates(Transformer):
    source_columns = ("visit_date", "sample_date")
    reported_col_prefix = "reported"

    @staticmethod
    def get_transformer_description() -> str:
        rep_cols = [
            f"{AddReportedDates.reported_col_prefix}_{item}"
            for item in AddReportedDates.source_columns
        ]
        return f"Copies columns {AddReportedDates.source_columns} to columns {rep_cols}"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for source_col in self.source_columns:
            if source_col not in data_holder.data.columns:
                self._log(f"Missing column: {source_col}", level=adm_logger.WARNING)
                continue
            target_col = f"{self.reported_col_prefix}_{source_col}"
            if target_col in data_holder.data.columns:
                self._log(
                    f"Column already present. Will do nothing: {target_col}",
                    level=adm_logger.DEBUG,
                )
                continue
            data_holder.data[target_col] = data_holder.data[source_col]


class PolarsAddReportedDates(Transformer):
    source_columns = ("visit_date", "sample_date")
    reported_col_prefix = "reported"

    @staticmethod
    def get_transformer_description() -> str:
        rep_cols = [
            f"{PolarsAddReportedDates.reported_col_prefix}_{item}"
            for item in PolarsAddReportedDates.source_columns
        ]
        return (
            f"Copies columns {PolarsAddReportedDates.source_columns} "
            f"to columns {rep_cols}"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for source_col in self.source_columns:
            if source_col not in data_holder.data.columns:
                self._log(f"Missing column: {source_col}", level=adm_logger.WARNING)
                continue
            target_col = f"{self.reported_col_prefix}_{source_col}"
            if target_col in data_holder.data.columns:
                self._log(
                    f"Column already present. Will do nothing: {target_col}",
                    level=adm_logger.DEBUG,
                )
                continue

            data_holder.data = data_holder.data.with_columns(
                [pl.col(source_col).alias(target_col)]
            )
            self._log(
                f"Column {target_col} set from source column {source_col}",
                level=adm_logger.DEBUG,
            )


class CreateFakeFullDates(Transformer):
    shark_comment_column = "shark_comment"
    mandatory_col_prefix = "reported"
    source_columns = ("visit_date", "sample_date")
    date_format = "%Y-%m-%d"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Creates fake date in columns {CreateFakeFullDates.source_columns} "
            f"if incomplete. Sets first date in month or year depending of precision"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.shark_comment_column not in data_holder.data.columns:
            data_holder.data[self.shark_comment_column] = ""
        for source_col in self.source_columns:
            if source_col not in data_holder.data.columns:
                self._log(
                    f"Could not transform {self.__class__.__name__}. "
                    f"Missing column {source_col}",
                    level=adm_logger.INFO,
                )
                continue
            mandatory_col = f"{self.mandatory_col_prefix}_{source_col}"
            if mandatory_col not in data_holder.data.columns:
                self._log(
                    f"Could not transform {self.__class__.__name__}. "
                    f"Missing column {mandatory_col}",
                    level=adm_logger.INFO,
                )
                continue
            for date_str in set(data_holder.data[source_col]):
                date_str = date_str.strip()
                try:
                    datetime.datetime.strptime(date_str, self.date_format)
                    # All is good
                except ValueError:
                    new_date_str = None
                    if len(date_str) == 4:
                        # Probably only year
                        self._log(
                            f"{source_col} is {date_str}. Will be handled as <YEAR>. "
                            f"First day of year will be set!",
                            level=adm_logger.WARNING,
                        )
                        new_date_str = f"{date_str}-01-01"
                    elif len(date_str) == 6:
                        # Probably only year
                        self._log(
                            f"{source_col} is {date_str}. Will be handled as "
                            f"<YEAR><MONTH>. "
                            f"First day of month in that year will be set!",
                            level=adm_logger.WARNING,
                        )
                        new_date_str = f"{date_str[:4]}-{date_str[4:]}-01"
                    else:
                        date_parts = date_str.split("-")
                        if len(date_parts) == 2:
                            self._log(
                                f"{source_col} is {date_str}. Will be handled as "
                                f"<YEAR>-<MONTH>. "
                                f"First day of month in that year will be set!",
                                level=adm_logger.WARNING,
                            )
                            new_date_str = f"{date_parts[0]}-{date_parts[1]}-01"

                    index = data_holder.data[source_col].str.strip() == date_str

                    if new_date_str is None:
                        comment_str = f'Unable to interpret {source_col} "{date_str}"'
                        self._log(comment_str, level=adm_logger.ERROR)
                    else:
                        data_holder.data.loc[index, source_col] = new_date_str
                        comment_str = (
                            f"Fake {source_col} set from {date_str} to {new_date_str}"
                        )
                    data_holder.data.loc[index, self.shark_comment_column] = (
                        data_holder.data.loc[index, self.shark_comment_column]
                        + comment_str
                        + "; "
                    )


class PolarsCreateFakeFullDates(PolarsTransformer):
    shark_comment_column = "shark_comment"
    mandatory_col_prefix = "reported"
    source_columns = ("visit_date", "sample_date")
    date_format = "%Y-%m-%d"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Creates fake date in columns {PolarsCreateFakeFullDates.source_columns} "
            f"if incomplete. Sets first date in month or year depending of precision"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self.shark_comment_column not in data_holder.data.columns:
            self._add_empty_col(data_holder, self.shark_comment_column)

        for source_col in self.source_columns:
            if source_col not in data_holder.data.columns:
                self._log(
                    f"Could not transform {self.__class__.__name__}. "
                    f"Missing column {source_col}",
                    level=adm_logger.INFO,
                )
                continue
            mandatory_col = f"{self.mandatory_col_prefix}_{source_col}"
            if mandatory_col not in data_holder.data.columns:
                self._log(
                    f"Could not transform {self.__class__.__name__}. "
                    f"Missing column {mandatory_col}",
                    level=adm_logger.INFO,
                )
                continue
            for date_str in set(data_holder.data[source_col]):
                date_str = date_str.strip()
                try:
                    datetime.datetime.strptime(date_str, self.date_format)
                    # All is good
                except ValueError:
                    new_date_str = None
                    if len(date_str) == 4:
                        # Probably only year
                        self._log(
                            f"{source_col} is {date_str}. Will be handled as <YEAR>. "
                            f"First day of year will be set!",
                            level=adm_logger.WARNING,
                        )
                        new_date_str = f"{date_str}-01-01"
                    elif len(date_str) == 6:
                        # Probably only year
                        self._log(
                            f"{source_col} is {date_str}. Will be handled as "
                            f"<YEAR><MONTH>. "
                            f"First day of month in that year will be set!",
                            level=adm_logger.WARNING,
                        )
                        new_date_str = f"{date_str[:4]}-{date_str[4:]}-01"
                    else:
                        date_parts = date_str.split("-")
                        if len(date_parts) == 2:
                            self._log(
                                f"{source_col} is {date_str}. Will be handled as "
                                f"<YEAR>-<MONTH>. "
                                f"First day of month in that year will be set!",
                                level=adm_logger.WARNING,
                            )
                            new_date_str = f"{date_parts[0]}-{date_parts[1]}-01"

                    # index = data_holder.data[source_col].str.strip() == date_str

                    if new_date_str is None:
                        comment_str = f'Unable to interpret {source_col} "{date_str}"'
                        self._log(comment_str, level=adm_logger.ERROR)
                    else:
                        data_holder.data = data_holder.data.with_columns(
                            pl.when(pl.col(source_col).str.strip_chars() == date_str)
                            .then(pl.lit(new_date_str))
                            .otherwise(pl.col(source_col))
                            .alias(source_col)
                        )

                        # data_holder.data.loc[index, source_col] = new_date_str
                        comment_str = (
                            f"Fake {source_col} set from {date_str} to {new_date_str}"
                        )

                    data_holder.data = data_holder.data.with_columns(
                        pl.when(pl.col(source_col).str.strip_chars() == date_str)
                        .then(
                            pl.concat_str(
                                [pl.col(self.shark_comment_column), pl.lit(comment_str)],
                                separator="; ",
                            )
                        )
                        .otherwise(pl.col(self.shark_comment_column))
                        .alias(self.shark_comment_column)
                    )


class AddVisitDateFromObservationDate(Transformer):
    valid_data_types = ("HarbourPorpoise",)
    valid_data_holders = ("ZipArchiveDataHolder",)
    source_col = "observation_date"
    col_to_set = "visit_date"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Sets {AddVisitDateFromObservationDate.col_to_set} "
            f"from {AddVisitDateFromObservationDate.source_col}"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.source_col not in data_holder.data:
            self._log(
                f"Source column {self.source_col} not found", level=adm_logger.DEBUG
            )
            return
        data_holder.data[self.col_to_set] = data_holder.data[self.source_col]
        self._log(
            f"Column {self.col_to_set} set from source column {self.source_col}",
            level=adm_logger.INFO,
        )


##########################################################################################
##########################################################################################
##########################################################################################
class PolarsFixDateFormat(PolarsTransformer):
    dates_to_check = ("sample_date", "visit_date", "analysis_date", "observation_date")

    from_format = "%Y%m%d"
    to_format = "%Y-%m-%d"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mapping = {}

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Changes date format from {PolarsFixDateFormat.from_format} "
            f"to {PolarsFixDateFormat.to_format}"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for col in self.dates_to_check:
            if col not in data_holder.data:
                continue

            boolean = data_holder.data[col].str.find(pattern=r"^\d{8}$").is_not_null()
            if boolean.any():
                data_holder.data = data_holder.data.with_columns(
                    pl.col(col).str.to_date("%Y%m%d").dt.strftime("%Y-%m-%d").alias(col)
                )
                self._log(
                    f"Converting date format from %Y%m%d to %Y-%m-%d in column {col} "
                    f"({boolean.sum()} places)",
                    level=adm_logger.INFO,
                )

            boolean = (
                data_holder.data[col].str.find(pattern=r"^\d{4}-\d{2}-\d{2}$").is_null()
            )
            if not boolean.any():
                continue

            unique_values = set(data_holder.data.filter(boolean)[col])
            self._log(
                f"Could not convert the following dates in column {col}: "
                f"{', '.join(sorted(unique_values))}",
                level=adm_logger.ERROR,
            )


class PolarsFixTimeFormat(Transformer):
    time_cols = ("sample_time", "visit_time", "sample_endtime")

    @staticmethod
    def get_transformer_description() -> str:
        cols_str = ", ".join(PolarsFixTimeFormat.time_cols)
        return f"Reformat time values in columns: {cols_str}"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for col in self.time_cols:
            if col not in data_holder.data:
                continue

            t_boolean = (
                data_holder.data[col].str.find(pattern=r"^\d{2}:\d{2}:\d{2}$").is_null()
            )
            if not t_boolean.any():
                self._log(
                    f"All values in column {col} correct format HH:MM:SS",
                    level=adm_logger.DEBUG,
                )
                continue

            m_boolean = data_holder.data[col].str.find(pattern=r"^\d{2}:\d{2}$").is_null()
            if not m_boolean.any():
                self._log(
                    f"All values in column {col} correct format HH:MM",
                    level=adm_logger.DEBUG,
                )
                continue

            boolean = t_boolean & m_boolean

            unique_values = set(data_holder.data.filter(boolean)[col])
            for value in unique_values:
                if "." in value:
                    h, m = value.split(".")
                    new_value = f"{h.zfill(2)}:{m.zfill(2)}"
                    if self._is_valid_value(new_value):
                        self._set_new_value(data_holder, col, value, new_value)
                    else:
                        self._log(
                            f"Cant handle time format {value} in column {col}",
                            level=adm_logger.ERROR,
                        )
                elif ":" in value:
                    h, m = value.split(":")
                    new_value = f"{h.zfill(2)}:{m.zfill(2)}"
                    if self._is_valid_value(new_value):
                        self._set_new_value(data_holder, col, value, new_value)
                    else:
                        self._log(
                            f"Cant handle time format {value} in column {col}",
                            level=adm_logger.ERROR,
                        )

                elif len(value) == 4:
                    new_value = f"{value[:2]}:{value[2:]}"
                    if self._is_valid_value(new_value):
                        self._set_new_value(data_holder, col, value, new_value)
                    else:
                        self._log(
                            f"Cant handle time format {value} in column {col}",
                            level=adm_logger.ERROR,
                        )

    def _set_new_value(
        self,
        data_holder: DataHolderProtocol,
        col: str,
        current_value: str,
        new_value: str,
    ):
        b = data_holder.data[col] == current_value
        self._log(
            f"Converting date {current_value} to {new_value} in column {col} "
            f"({b.sum()} places)",
            level=adm_logger.INFO,
        )
        data_holder.data = data_holder.data.with_columns(
            pl.when(pl.col(col) == current_value)
            .then(pl.lit(new_value))
            .otherwise(pl.col(col))
            .alias(col)
        )

    def _is_valid_value(self, value: str) -> bool | None:
        for pat in [r"^\d{2}:\d{2}:\d{2}$", r"^\d{2}:\d{2}$"]:
            if re.search(pat, value):
                return True


class PolarsAddVisitDateFromObservationDate(Transformer):
    valid_data_types = ("HarbourPorpoise",)
    valid_data_holders = ("ZipArchiveDataHolder",)
    source_col = "observation_date"
    col_to_set = "visit_date"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Sets {PolarsAddVisitDateFromObservationDate.col_to_set} "
            f"from {PolarsAddVisitDateFromObservationDate.source_col}"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.source_col not in data_holder.data:
            self._log(
                f"Source column {self.source_col} not found", level=adm_logger.DEBUG
            )
            return
        data_holder.data = data_holder.data.with_columns(
            pl.col(self.source_col).alias(self.col_to_set)
        )
        self._log(
            f"Column {self.col_to_set} set from source column {self.source_col}",
            level=adm_logger.DEBUG,
        )


class PolarsAddMonth(PolarsTransformer):
    month_columns = ("sample_month", "visit_month")

    @staticmethod
    def get_transformer_description() -> str:
        return (
            "Adds month column to data. Month is taken from the datetime column "
            "and will overwrite old values"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if "datetime" not in data_holder.data.columns:
            self._log("Missing column: datetime", level=adm_logger.WARNING)
            return
        for col in self.month_columns:
            data_holder.data = data_holder.data.with_columns(
                pl.col("datetime").dt.month().cast(str).str.zfill(2).alias(col)
            )
