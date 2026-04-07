import polars as pl

from sharkadm.validators.base import DataHolderProtocol, Validator


class ValidateWaterDepth(Validator):
    _display_name = "Water depth"
    lower_limit = 0
    upper_limit = 500

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that the water depth is within reasonable ranges (0 to 500 m)."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        self._log_workflow(
            "Checking that the water depth is within reasonable ranges (0 to 500 m).",
        )

        if "water_depth_m" not in data_holder.data.columns:
            self._log_fail(
                "Could not validate water depth because water depth column missing.",
            )
            return
        if (
            "visit_date" not in data_holder.data.columns
            or "reported_station_name" not in data_holder.data.columns
        ):
            self._log_fail("Missing visit date or reported station name columns.")
            return

        unique_rows = (
            data_holder.data.select(
                ["visit_date", "reported_station_name", "water_depth_m", "row_number"]
            )
            .group_by(["visit_date", "reported_station_name", "water_depth_m"])
            .agg(pl.col("row_number").alias("row_numbers"))
        )
        unique_rows = unique_rows.with_columns(
            [
                pl.when(
                    pl.col("water_depth_m").is_null()
                    | (pl.col("water_depth_m").str.strip_chars() == "")
                    | pl.col("water_depth_m").str.contains(r"[^0-9.]")
                )
                .then(
                    pl.format(
                        "{} on {}: Missing or invalid water depth: {}",
                        pl.col("reported_station_name"),
                        pl.col("visit_date"),
                        pl.col("water_depth_m"),
                    )
                )
                .when(
                    pl.col("water_depth_m")
                    .cast(pl.Float64, strict=False)
                    .is_between(self.lower_limit, self.upper_limit, closed="both")
                )
                .then(pl.lit("Water depth is ok"))
                .otherwise(
                    pl.format(
                        "{} on {}: Water depth is outside acceptable ranges:{} > {} > {}",
                        pl.col("reported_station_name"),
                        pl.col("visit_date"),
                        self.lower_limit,
                        pl.col("water_depth_m"),
                        self.upper_limit,
                    )
                )
                .alias("message")
            ]
        )

        if unique_rows.filter(pl.col("message") != "Water depth is ok").height == 0:
            self._log_success("All water depths are ok")
        else:
            for (msg,), df in unique_rows.filter(
                pl.col("message") != "Water depth is ok"
            ).group_by("message"):
                self._log_fail(msg=msg, row_numbers=df["row_numbers"][0])


class ValidateSampleDepth(Validator):
    _display_name = "Sample depth"

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that sample depth is never below water depth."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        self._log_workflow(
            "Checking that sample depth is never below water depth.",
        )

        if "water_depth_m" not in data_holder.data.columns:
            self._log_fail(
                "Could not validate sample depth because water depth column missing.",
            )
            return

        if "sample_depth_m" not in data_holder.data.columns:
            self._log_fail(
                "Could not validate sample depth because sample depth column missing.",
            )
            return

        if (
            "visit_date" not in data_holder.data.columns
            or "reported_station_name" not in data_holder.data.columns
        ):
            self._log_fail("Missing visit date or reported station name columns.")
            return

        unique_rows = data_holder.data.select(
            [
                "visit_date",
                "reported_station_name",
                "water_depth_m",
                "sample_depth_m",
                "row_number",
            ]
        ).unique()
        unique_rows = unique_rows.with_columns(
            [
                pl.when(
                    pl.col("water_depth_m").is_null()
                    | pl.col("sample_depth_m").is_null()
                    | (pl.col("water_depth_m").str.strip_chars() == "")
                    | (pl.col("sample_depth_m").str.strip_chars() == "")
                    | pl.col("water_depth_m").str.contains(r"[^0-9.]")
                    | pl.col("sample_depth_m").str.contains(r"[^0-9.]")
                )
                .then(
                    pl.format(
                        "{} on {}: "
                        "Missing or invalid water depth {}, or sample depth {} ",
                        pl.col("reported_station_name"),
                        pl.col("visit_date"),
                        pl.col("water_depth_m"),
                        pl.col("sample_depth_m"),
                    )
                )
                .when(
                    pl.col("sample_depth_m").cast(pl.Float64, strict=False)
                    < pl.col("water_depth_m").cast(pl.Float64, strict=False)
                )
                .then(pl.lit("Sample depth is ok"))
                .otherwise(
                    pl.format(
                        "{} on {}: "
                        "Sample depth is equal to or larger water depth:"
                        "{} >= {}",
                        pl.col("reported_station_name"),
                        pl.col("visit_date"),
                        pl.col("sample_depth_m"),
                        pl.col("water_depth_m"),
                    )
                )
                .alias("message")
            ]
        )

        if unique_rows.filter(pl.col("message") != "Sample depth is ok").height == 0:
            self._log_success("All sample depths are ok")
        else:
            for (msg,), df in unique_rows.filter(
                pl.col("message") != "Sample depth is ok"
            ).group_by("message"):
                self._log_fail(msg=msg, row_number=df["row_number"][0])


class ValidateSecchiDepth(Validator):
    _display_name = "Secchi depth"

    @staticmethod
    def get_validator_description() -> str:
        return "Checks that secchi depth is never below water depth."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        self._log_workflow(
            "Checking that secchi depth is never below water depth.",
        )

        if "water_depth_m" not in data_holder.data.columns:
            self._log_fail(
                "Could not validate secchi depth because water depth column missing.",
            )
            return

        if (
            "parameter" not in data_holder.data.columns
            or "value" not in data_holder.data.columns
        ):
            self._log_fail(
                "Could not validate secchi depth because secchi depth is missing.",
            )
            return

        error = False
        secchi_value_found = False
        for (value, water_depth), df in data_holder.data.filter(
            data_holder.data["parameter"] == "SECCHI"
        ).group_by(["value", "water_depth_m"]):
            secchi_value_found = True
            visit_info = (
                df.select(["visit_date", "reported_station_name"]).unique().to_dicts()
            )
            try:
                secchi_depth_float = float(value)
                water_depth_float = float(water_depth)
            except (TypeError, ValueError):
                self._log_fail(
                    "Could not validate secchi depth:"
                    " Invalid formats in . "
                    f"secchi_depth={value!r} "
                    f"and/or "
                    f"water_depth={water_depth!r} "
                    f"at visit date and station: {visit_info}",
                    row_numbers=list(df["row_number"]),
                )
                error = True
                continue

            if secchi_depth_float >= water_depth_float:
                self._log_fail(
                    f"Secchi depth below water depth: {value} >= {df['water_depth_m']}"
                    f" at visit date and station: "
                    f"{visit_info}",
                    row_numbers=list(df["row_number"]),
                )
                error = True

        if not secchi_value_found:
            self._log_fail(
                "Could not validate secchi depth because secchi depth is missing.",
            )
            return

        if not error:
            self._log_success("All secchi depths above water depth.")
