from sharkadm import adm_logger, config
from sharkadm.data import PolarsDataHolder, is_valid_polars_data_holder


class OperationBase:
    valid_data_types: tuple[str, ...] = ()
    invalid_data_types: tuple[str, ...] = ()

    valid_data_holders: tuple[str, ...] = ()
    invalid_data_holders: tuple[str, ...] = ()

    valid_data_structures: tuple[str, ...] = ()
    invalid_data_structures: tuple[str, ...] = ()

    operation_type: str = "operation"

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def _data_holder_has_valid_data_type(self, data_holder: PolarsDataHolder) -> bool:
        if data_holder.data_type_internal == "unknown":
            return True
        if data_holder.data_type_internal in config.get_valid_data_types(
            valid=self.valid_data_types, invalid=self.invalid_data_types
        ):
            return True
        return False

    def _data_holder_is_valid_data_holder(self, data_holder: PolarsDataHolder) -> bool:
        if is_valid_polars_data_holder(
            data_holder,
            valid=self.valid_data_holders,
            invalid=self.invalid_data_holders,
        ):
            return True
        return False

    def _data_holder_has_valid_data_structure(
        self, data_holder: PolarsDataHolder
    ) -> bool:
        if data_holder.data_structure.lower() in config.get_valid_data_structures(
            valid=self.valid_data_structures, invalid=self.invalid_data_structures
        ):
            return True
        return False

    def is_valid_data_holder(self, data_holder: PolarsDataHolder) -> bool:
        if not self._data_holder_has_valid_data_type(data_holder):
            self._log_workflow(
                f"Invalid data_type {data_holder.data_type_internal} "
                f"for {self.operation_type}"
                f" {self.name}",
                level=adm_logger.DEBUG,
            )
            return False
        if not self._data_holder_is_valid_data_holder(data_holder):
            self._log_workflow(
                f"Invalid data_holder {data_holder.__class__.__name__} "
                f"for {self.operation_type}"
                f" {self.name}"
            )
            return False
        if not self._data_holder_has_valid_data_structure(data_holder):
            self._log_workflow(
                f"Invalid data structure {data_holder.data_structure} "
                f"for {self.operation_type} {self.name}",
                level=adm_logger.DEBUG,
            )
            return False
        return True

    def _log_workflow(self, msg: str, **kwargs):
        adm_logger.log_workflow(msg, cls=self.__class__.__name__, **kwargs)
