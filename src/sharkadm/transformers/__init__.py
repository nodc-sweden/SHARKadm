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
from sharkadm.transformers.add_lmqnt import AddLmqnt, PolarsAddLmqnt
from sharkadm.transformers.add_uncertainty import (
    AddUncertainty,
    PolarsAddUncertainty,
    PolarsAddStandardUncertainty,
)
from sharkadm.transformers.analyse_info import (
    AddAnalyseInfo,
    PolarsAddAnalyseInfoOld,
    PolarsAddAnalyseInfo,
)
from sharkadm.transformers.aphia_id import (
    PolarsSetReportedAphiaIdFromAphiaId,
    PolarsSetAphiaIdFromReportedAphiaId,
    PolarsSetAphiaIdFromWormsAphiaId,
    PolarsSetAphiaIdFromBvolAphiaId,
)
from sharkadm.transformers.arithmetic import (
    Divide,
    Multiply,
    PolarsMultiply,
    PolarsDivide,
)
from sharkadm.transformers.bacteria import SetBacteriaAsReportedScientificName
from sharkadm.transformers.base import PolarsTransformer, Transformer
from sharkadm.transformers.boolean import (
    FixYesNo,
    PolarsFixYesNo,
    PolarsFixTrueAndFalse,
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
    AddColumnViewsColumns,
    AddDEPHqcColumn,
    PolarsAddApprovedKeyColumn,
    PolarsAddColumnViewsColumns,
    PolarsRemoveColumns,
    PolarsClearColumns,
    RemoveColumns,
    SortColumns,
    PolarsSortColumns,
    AddColumnsWithPrefix,
    PolarsAddDEPHqcColumn,
    PolarsAddFloatColumns,
    PolarsAddColumnDiff,
    PolarsAddBooleanLargerThan,
    PolarsFixDuplicateColumns,
)
from sharkadm.transformers.coordinates import PolarsSetBoundingBox
from sharkadm.transformers.cruise import AddCruiseId, PolarsAddCruiseId
from sharkadm.transformers.custom_id import (
    AddCustomId,
    AddSharkSampleMd5,
    PolarsAddCustomId,
    PolarsAddSharkSampleMd5,
)
from sharkadm.transformers.dataframe import (
    ConvertFromPandasToPolars,
    ConvertFromPolarsToPandas,
)
from sharkadm.transformers.dataset_name import (
    AddDatasetFileName,
    AddDatasetName,
    PolarsAddDatasetFileName,
    PolarsAddDatasetName,
)
from sharkadm.transformers.datatype import (
    PolarsAddDatatype,
    PolarsAddDatatypePlanktonBarcoding,
)
from sharkadm.transformers.date_and_time import (
    AddDatetime,
    AddMonth,
    AddReportedDates,
    AddSampleDate,
    AddSampleTime,
    AddVisitDateFromObservationDate,
    CreateFakeFullDates,
    FixDateFormat,
    FixTimeFormat,
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
    AddDeliveryNoteInfo,
    AddStatus,
    PolarsAddDeliveryNoteInfo,
)
from sharkadm.transformers.depth import (
    AddSampleMinAndMaxDepth,
    AddSectionStartAndEndDepth,
    ReorderSampleMinAndMaxDepth,
    PolarsAddIOdisDepth,
)
from sharkadm.transformers.dyntaxa import (
    AddDyntaxaId,
    AddDyntaxaScientificName,
    AddDyntaxaTranslatedScientificNameDyntaxaId,
    AddReportedDyntaxaId,
    AddReportedScientificNameDyntaxaId,
    AddTaxonRanks,
    PolarsAddDyntaxaId,
    PolarsAddDyntaxaScientificName,
    PolarsAddDyntaxaTranslatedScientificNameDyntaxaId,
    PolarsAddReportedDyntaxaId,
    PolarsAddReportedScientificNameDyntaxaId,
    PolarsAddTaxonRanks,
)
from sharkadm.transformers.fake import FakeAddCTDtagToColumns, FakeAddPressureFromDepth
from sharkadm.transformers.flags import ConvertFlagsToSDN, PolarsConvertFlagsToSDN
from sharkadm.transformers.laboratory import (
    AddEnglishAnalyticalLaboratory,
    AddEnglishSamplingLaboratory,
    AddSwedishAnalyticalLaboratory,
    AddSwedishSamplingLaboratory,
    PolarsAddEnglishAnalyticalLaboratory,
    PolarsAddEnglishSamplingLaboratory,
    PolarsAddSwedishAnalyticalLaboratory,
    PolarsAddSwedishSamplingLaboratory,
)
from sharkadm.transformers.lims import (
    MoveLessThanFlagColumnFormat,
    MoveLessThanFlagRowFormat,
    MoveLargerThanFlagRowFormat,
    PolarsMoveLessThanFlagRowFormat,
    PolarsMoveLargerThanFlagRowFormat,
    PolarsRemoveNonDataLines,
    RemoveNonDataLines,
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
    PolarsAddLocationR,
    PolarsAddLocationRA,
    PolarsAddLocationRB,
    PolarsAddLocationRC,
    PolarsAddLocationRG,
    PolarsAddLocationRH,
    PolarsAddLocationRO,
    PolarsAddLocationOnLand,
    PolarsAddLocations,
)
from sharkadm.transformers.long_to_wide import LongToWide
from sharkadm.transformers.manual import (
    ManualHarbourPorpoise,
    ManualSealPathology,
    PolarsManualEpibenthos,
    PolarsManualHarbourPorpoise,
    PolarsManualSealPathology,
)
from sharkadm.transformers.map_header import ArchiveMapper
from sharkadm.transformers.map_parameter_column import (
    MapperParameterColumn,
    PolarsMapperParameterColumn,
)
from sharkadm.transformers.metadata import PolarsAddFromMetadata
from sharkadm.transformers.occurrence_id import AddOccurrenceId
from sharkadm.transformers.orderer import (
    AddEnglishSampleOrderer,
    AddSwedishSampleOrderer,
    PolarsAddEnglishSampleOrderer,
    PolarsAddSwedishSampleOrderer,
)
from sharkadm.transformers.parameter_column import PolarsAddParameterShortColumn
from sharkadm.transformers.parameter_unit_value import RemoveRowsWithNoParameterValue
from sharkadm.transformers.position import (
    AddSamplePositionDD,
    AddSamplePositionDM,
    AddSamplePositionSweref99tm,
    PolarsAddReportedPosition,
    PolarsAddSamplePositionDD,
    PolarsAddSamplePositionDDAsFloat,
    PolarsAddSamplePositionDM,
    PolarsAddSamplePositionSweref99tm,
)
from sharkadm.transformers.profile import PolarsAddMetadataToProfileData
from sharkadm.transformers.project_code import (
    AddEnglishProjectName,
    AddSwedishProjectName,
    PolarsAddEnglishProjectName,
    PolarsAddSwedishProjectName,
)
from sharkadm.transformers.qc.columns import AddColumnsForAutomaticQC
from sharkadm.transformers.red_list import AddRedList
from sharkadm.transformers.remove import (
    PolarsKeepMask,
    PolarsRemoveMask,
    PolarsRemoveProfiles,
    PolarsRemoveBottomDepthInfoProfiles,
    PolarsRemoveValueInColumns,
    PolarsRemoveValueInRowsForParameters,
    PolarsReplaceColumnWithMask,
    RemoveDeepestDepthAtEachVisit,
    RemoveInterval,
    RemoveReportedValueIfNotCalculated,
    RemoveRowsAtDepthRestriction,
    RemoveRowsForParameters,
    RemoveValuesInColumns,
    SetMaxLengthOfValuesInColumns,
)
from sharkadm.transformers.replace import (
    ReplaceNanWithEmptyString,
    PolarsReplaceNanWithNone,
)
from sharkadm.transformers.replace_comma_with_dot import (
    PolarsReplaceCommaWithDot,
    ReplaceCommaWithDot,
)
from sharkadm.transformers.reporting_institute import (
    AddEnglishReportingInstitute,
    AddSwedishReportingInstitute,
    PolarsAddEnglishReportingInstitute,
    PolarsAddSwedishReportingInstitute,
)
from sharkadm.transformers.row import AddRowNumber, PolarsAddRowNumber
from sharkadm.transformers.sampler_area import AddCalculatedSamplerArea
from sharkadm.transformers.sampling_info import AddSamplingInfo, PolarsAddSamplingInfo
from sharkadm.transformers.scientific_name import (
    SetScientificNameFromDyntaxaScientificName,
    SetScientificNameFromReportedScientificName,
    PolarsSetScientificNameFromDyntaxaScientificName,
    PolarsSetScientificNameFromReportedScientificName,
)
from sharkadm.transformers.serial_number import FormatSerialNumber
from sharkadm.transformers.shark_id import AddSharkId, PolarsAddSharkId
from sharkadm.transformers.sort_data import (
    SortData,
    PolarsSortData,
    SortDataPlanktonImaging,
)
from sharkadm.transformers.static_data_holding_center import (
    AddStaticDataHoldingCenterEnglish,
    AddStaticDataHoldingCenterSwedish,
    PolarsAddStaticDataHoldingCenterEnglish,
    PolarsAddStaticDataHoldingCenterSwedish,
)
from sharkadm.transformers.static_internet_access import (
    AddStaticInternetAccessInfo,
    PolarsAddStaticInternetAccessInfo,
)
from sharkadm.transformers.station import (
    AddStationInfo,
    PolarsAddStationInfo,
    CopyReportedStationNameToStationName,
    PolarsCopyReportedStationNameToStationName,
    PolarsSetStationNameFromReportedStationNameIfMissing,
)
from sharkadm.transformers.status import SetStatusDataHost, SetStatusDeliverer
from sharkadm.transformers.string import PolarsCodesToUppercase
from sharkadm.transformers.strip import StripAllValues, StripAllValuesPolars
from sharkadm.transformers.trophic_type import PolarsSetTrophicTypeSMHI
from sharkadm.transformers.visit import (
    AddVisitKey,
    PolarsAddPhysicalChemicalKey,
    PolarsAddVisitKey,
)
from sharkadm.transformers.wide_to_long import PolarsWideToLong, WideToLong
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
