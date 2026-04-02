# ruff: noqa: I001
import functools
import pathlib
from typing import Type

from sharkadm import utils
from sharkadm.transformers.add_ctd_kust import AddCtdKust
from sharkadm.transformers.add_gsw_parameters import (
    PolarsAddPressure,
    PolarsAddDensityWide,
    PolarsAddDensity,
    PolarsAddOxygenSaturationWide,
    PolarsAddOxygenSaturation,
)
from sharkadm.transformers.add_lmqnt import PolarsAddLmqnt
from sharkadm.transformers.add_uncertainty import (
    PolarsAddUncertainty,
    PolarsAddStandardUncertainty,
)
from sharkadm.transformers.analyse_info import (
    PolarsAddAnalyseInfo,
)
from sharkadm.transformers.aphia_id import (
    PolarsSetReportedAphiaIdFromAphiaId,
    PolarsSetAphiaIdFromReportedAphiaId,
    PolarsSetAphiaIdFromWormsAphiaId,
    PolarsSetAphiaIdFromBvolAphiaId,
)
from sharkadm.transformers.arithmetic import (
    PolarsMultiply,
    PolarsDivide,
)
from sharkadm.transformers.bacteria import SetBacteriaAsReportedScientificName
from sharkadm.transformers.base import PolarsTransformer
from sharkadm.transformers.boolean import (
    PolarsFixYesNo,
    PolarsFixTrueAndFalse,
    PolarsAddDataFilterBooleanColumn,
)
from sharkadm.transformers.bvol import (
    PolarsAddBvolAphiaId,
    PolarsAddBvolRefList,
    PolarsAddBvolScientificNameAndSizeClass,
    PolarsAddBvolScientificNameOriginal,
    PolarsAddBvolCellVolume,
    PolarsAddBvolCarbonVolume,
)
from sharkadm.transformers.calculate import (
    PolarsCalculateAbundance,
    PolarsCalculateBiovolume,
    PolarsCalculateCarbon,
    PolarsFixCalcByDc,
    PolarsOnlyKeepReportedIfCalcByDc,
)
from sharkadm.transformers.columns import (
    PolarsAddColumnViewsColumns,
    PolarsRemoveColumns,
    PolarsClearColumns,
    PolarsSortColumns,
    AddColumnsWithPrefix,
    PolarsAddDEPHqcColumn,
    PolarsAddFloatColumns,
    PolarsAddColumnDiff,
    PolarsAddBooleanLargerThan,
    PolarsAddIntColumns,
    PolarsFixDuplicateColumns,
)
from sharkadm.transformers.coordinates import PolarsSetBoundingBox
from sharkadm.transformers.cruise import PolarsAddCruiseId
from sharkadm.transformers.custom_id import (
    PolarsAddCustomId,
    PolarsAddSharkSampleMd5,
)
from sharkadm.transformers.dataset_name import (
    PolarsAddDatasetFileName,
    PolarsAddDatasetName,
)
from sharkadm.transformers.datatype import (
    PolarsAddDatatype,
    PolarsAddDatatypePlanktonBarcoding,
)
from sharkadm.transformers.date_and_time import (
    PolarsAddDatetime,
    PolarsAddMonth,
    PolarsAddReportedDates,
    PolarsAddSampleDate,
    PolarsAddSampleTime,
    PolarsAddVisitDateFromObservationDate,
    PolarsCreateFakeFullDates,
    PolarsFixDateFormat,
    PolarsFixTimeFormat,
)
from sharkadm.transformers.delivery_note_info import (
    PolarsAddDeliveryNoteInfo,
)
from sharkadm.transformers.depth import (
    AddSampleMinAndMaxDepth,
    ReorderSampleMinAndMaxDepth,
)
from sharkadm.transformers.dyntaxa import (
    PolarsAddDyntaxaId,
    PolarsAddDyntaxaScientificName,
    PolarsAddDyntaxaTranslatedScientificNameDyntaxaId,
    PolarsAddReportedDyntaxaId,
    PolarsAddReportedScientificNameDyntaxaId,
    PolarsAddTaxonRanks,
)
from sharkadm.transformers.fake import FakeAddCTDtagToColumns, FakeAddPressureFromDepth
from sharkadm.transformers.flags import PolarsConvertFlagsToSDN
from sharkadm.transformers.laboratory import (
    PolarsAddEnglishSamplingLaboratory,
    PolarsAddSwedishSamplingLaboratory,
    PolarsAddEnglishAnalyticalLaboratory,
    PolarsAddSwedishAnalyticalLaboratory,
)
from sharkadm.transformers.lims import (
    PolarsMoveLessThanFlagRowFormat,
    PolarsMoveLargerThanFlagRowFormat,
    PolarsRemoveNonDataLines,
    PolarsKeepOnlyJellyfishLines,
)
from sharkadm.transformers.location import (
    PolarsAddLocationHelcomOsparArea,
    PolarsAddLocationMunicipality,
    PolarsAddLocationNation,
    PolarsAddLocationSeaBasin,
    PolarsAddLocationTypeArea,
    PolarsAddLocationSeaAreaCode,
    PolarsAddLocationSeaAreaName,
    PolarsAddLocationTYPNFS06,
    PolarsAddLocationWaterCategory,
    PolarsAddLocationWaterDistrict,
    PolarsAddLocationWB,
    PolarsAddLocationCounty,
    PolarsAddLocationOnLand,
    PolarsAddLocations,
)
from sharkadm.transformers.long_to_wide import LongToWide
from sharkadm.transformers.manual import (
    PolarsManualEpibenthos,
    PolarsManualHarbourPorpoise,
    PolarsManualSealPathology,
)
from sharkadm.transformers.map_header import ArchiveMapper
from sharkadm.transformers.map_parameter_column import (
    PolarsMapperParameterColumn,
)
from sharkadm.transformers.metadata import PolarsAddFromMetadata
from sharkadm.transformers.occurrence_id import AddOccurrenceId
from sharkadm.transformers.orderer import (
    PolarsAddEnglishSampleOrderer,
    PolarsAddSwedishSampleOrderer,
)
from sharkadm.transformers.parameter_column import PolarsAddParameterShortColumn
from sharkadm.transformers.parameter_unit_value import RemoveRowsWithNoParameterValue
from sharkadm.transformers.position import (
    PolarsAddReportedPosition,
    PolarsAddSamplePositionDD,
    PolarsAddSamplePositionDDAsFloat,
    PolarsAddSamplePositionDM,
    PolarsAddSamplePositionSweref99tm,
)
from sharkadm.transformers.profile import (
    PolarsAddMetadataToProfileData,
    PolarsLoadSensorInfoToProfileData,
)
from sharkadm.transformers.project_code import (
    PolarsAddEnglishProjectName,
    PolarsAddSwedishProjectName,
)
from sharkadm.transformers.red_list import AddRedList
from sharkadm.transformers.remove import (
    PolarsRemoveMask,
    PolarsRemoveValueInColumns,
    PolarsKeepMask,
    PolarsRemoveProfiles,
    PolarsRemoveBottomDepthInfoProfiles,
    PolarsRemoveValueInRowsForParameters,
    PolarsReplaceColumnWithMask,
)
from sharkadm.transformers.replace import PolarsReplaceNanWithNone
from sharkadm.transformers.replace_comma_with_dot import PolarsReplaceCommaWithDot
from sharkadm.transformers.reporting_institute import (
    PolarsAddEnglishReportingInstitute,
    PolarsAddSwedishReportingInstitute,
)
from sharkadm.transformers.row import PolarsAddRowNumber
from sharkadm.transformers.sampler_area import AddCalculatedSamplerArea
from sharkadm.transformers.sampling_info import PolarsAddSamplingInfo
from sharkadm.transformers.scientific_name import (
    PolarsSetScientificNameFromDyntaxaScientificName,
    PolarsSetScientificNameFromReportedScientificName,
)
from sharkadm.transformers.serial_number import FormatSerialNumber
from sharkadm.transformers.shark_id import PolarsAddSharkId
from sharkadm.transformers.sort_data import (
    PolarsSortData,
    SortDataPlanktonImaging,
)
from sharkadm.transformers.static_data_holding_center import (
    PolarsAddStaticDataHoldingCenterEnglish,
    PolarsAddStaticDataHoldingCenterSwedish,
)
from sharkadm.transformers.static_internet_access import (
    PolarsAddStaticInternetAccessInfo,
)
from sharkadm.transformers.station import (
    PolarsAddStationInfo,
    PolarsCopyReportedStationNameToStationName,
    PolarsSetStationNameFromReportedStationNameIfMissing,
)
from sharkadm.transformers.status import SetStatusDataHost, SetStatusDeliverer
from sharkadm.transformers.string import PolarsCodesToUppercase
from sharkadm.transformers.strip import PolarsStripAllValues
from sharkadm.transformers.trophic_type import PolarsSetTrophicTypeSMHI
from sharkadm.transformers.visit import (
    PolarsAddPhysicalChemicalKey,
    PolarsAddVisitKey,
)
from sharkadm.transformers.wide_to_long import PolarsWideToLong
from sharkadm.transformers.worms import (
    PolarsAddWormsAphiaId,
    PolarsAddWormsScientificName,
)
from sharkadm.utils.inspect_kwargs import get_kwargs_for_class


@functools.cache
def get_transformer_list() -> list[str]:
    """Returns a sorted list of name of all available transformers"""
    return sorted(utils.get_all_class_children_names(PolarsTransformer))


def get_transformers() -> dict[str, Type[PolarsTransformer]]:
    """Returns a dictionary with transformers"""
    return utils.get_all_class_children(PolarsTransformer)


def get_transformer_object(name: str, **kwargs) -> PolarsTransformer | None:
    """Returns PolarsTransformer object that matches the given transformer names"""
    all_trans = get_transformers()
    tran = all_trans.get(name)
    if not tran:
        return
    args = kwargs.pop("args", [])
    return tran(*args, **kwargs)


def get_transformers_description() -> dict[str, str]:
    """Returns a dictionary with transformer name as key and the description as value"""
    result = dict()
    for name, tran in get_transformers().items():
        if name.startswith("_"):
            continue
        result[name] = tran.get_transformer_description()
    return result


def get_transformers_info() -> dict:
    result = dict()
    for name, tran in get_transformers().items():
        result[name] = dict()
        result[name]["name"] = name
        result[name]["description"] = tran.get_transformer_description()
        result[name]["kwargs"] = get_kwargs_for_class(tran)
    return result


def get_transformers_description_text() -> str:
    info = get_transformers_description()
    line_length = 100
    lines = list()
    lines.append("=" * line_length)
    lines.append("Available transformers:")
    lines.append("-" * line_length)
    for key in sorted(info):
        lines.append(f"{key.ljust(40)}{info[key]}")
    lines.append("=" * line_length)
    return "\n".join(lines)


def print_transformers_description() -> None:
    """Prints all transformers on screen"""
    print(get_transformers_description_text())


def write_transformers_description_to_file(path: str | pathlib.Path) -> None:
    """Prints all transformers on screen"""
    with open(path, "w") as fid:
        fid.write(get_transformers_description_text())


def get_physical_chemical_transformer_objects() -> list[PolarsTransformer]:
    return [
        PolarsAddDEPHqcColumn(),
        PolarsAddCruiseId(),
        PolarsAddVisitKey(),
        PolarsWideToLong(),
    ]
