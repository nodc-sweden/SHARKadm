from .base import Validator, DataHolderProtocol

from sharkadm import adm_logger


class _ValidateAphiaId(Validator):
    from_aphia_id_col = ""
    to_aphia_id_col = ""

    @staticmethod
    def get_validator_description() -> str:
        return ""

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        if self.from_aphia_id_col not in data_holder.data:
            adm_logger.log_validation(
                f"Could not validate aphia_id. Missing column {self.from_aphia_id_col}",
                level=adm_logger.WARNING,
            )
            return

        if self.to_aphia_id_col not in data_holder.data:
            adm_logger.log_validation(
                f"Could not validate aphia_id. Missing column {self.to_aphia_id_col}",
                level=adm_logger.WARNING,
            )
            return

        if not any(data_holder.data[self.from_aphia_id_col]):
            adm_logger.log_validation(f"No values in {self.from_aphia_id_col}")
            return

        for (reported, translated), df in data_holder.data.groupby(
            [self.from_aphia_id_col, self.to_aphia_id_col]
        ):
            if not (reported and translated):
                continue
            if not reported:
                adm_logger.log_validation(
                    f"Missing {self.from_aphia_id_col} for {self.to_aphia_id_col}: "
                    f"{translated} ({len(df)} places)",
                    level=adm_logger.INFO,
                )
                continue
            if not translated:
                adm_logger.log_validation(
                    f"Missing {self.to_aphia_id_col} for {self.from_aphia_id_col}: "
                    f"{reported}({len(df)} places)",
                    level=adm_logger.INFO,
                )
                continue
            adm_logger.log_validation(
                f"{self.from_aphia_id_col} differs from {self.to_aphia_id_col}: "
                f'"{reported}" ({self.from_aphia_id_col}) <-> '
                f'"{translated}" ({self.to_aphia_id_col}) ({len(df)} places)',
                level=adm_logger.INFO,
            )


class ValidateReportedVsAphiaId(_ValidateAphiaId):
    from_aphia_id_col = "reported_aphia_id"
    to_aphia_id_col = "aphia_id"

    @staticmethod
    def get_validator_description() -> str:
        return (
            f"Checks if {ValidateReportedVsAphiaId.to_aphia_id_col} "
            f"is the same as {ValidateReportedVsAphiaId.from_aphia_id_col}"
        )


class ValidateReportedVsBvolAphiaId(_ValidateAphiaId):
    from_aphia_id_col = "reported_aphia_id"
    to_aphia_id_col = "bvol_aphia_id"

    @staticmethod
    def get_validator_description() -> str:
        return (
            f"Checks if {ValidateReportedVsBvolAphiaId.to_aphia_id_col} "
            f"is the same as {ValidateReportedVsBvolAphiaId.from_aphia_id_col}"
        )


class ValidateAphiaIdVsBvolAphiaId(_ValidateAphiaId):
    from_aphia_id_col = "aphia_id"
    to_aphia_id_col = "bvol_aphia_id"

    @staticmethod
    def get_validator_description() -> str:
        return (
            f"Checks if {ValidateAphiaIdVsBvolAphiaId.to_aphia_id_col} "
            f"is the same as {ValidateAphiaIdVsBvolAphiaId.from_aphia_id_col}"
        )
