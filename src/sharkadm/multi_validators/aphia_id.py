from sharkadm import validators

from .base import MultiValidator


class AphiaIdAfter(MultiValidator):
    _validators = (
        validators.ValidateReportedVsAphiaId,
        validators.ValidateReportedVsBvolAphiaId,
        validators.ValidateAphiaIdVsBvolAphiaId,
    )

    @staticmethod
    def get_validator_description() -> str:
        string_list = ["Performs all validators related to Aphia ID."]
        for trans in AphiaIdAfter._validators:
            string_list.append(f"    {trans.get_validator_description()}")
        return "\n".join(string_list)
