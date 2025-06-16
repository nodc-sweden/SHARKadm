# ruff: noqa: F401
import functools
import pathlib
from typing import Type

from sharkadm import utils
from sharkadm.transformers.add_lmqnt import AddLmqnt
from sharkadm.transformers.add_uncertainty import AddUncertainty
from sharkadm.transformers.analyse_info import AddAnalyseInfo, PolarsAddAnalyseInfo
from sharkadm.transformers.arithmetic import Divide, Multiply
from sharkadm.transformers.bacteria import SetBacteriaAsReportedScientificName
from sharkadm.transformers.base import PolarsTransformer, Transformer
from sharkadm.transformers.boolean import FixYesNo
from sharkadm.transformers.bvol import (
    PolarsAddBvolAphiaId,
    PolarsAddBvolRefList,
    PolarsAddBvolScientificNameAndSizeClass,
    # AddBvolAphiaId,
    # AddBvolRefList,
    # AddBvolScientificNameAndSizeClass,
    # AddBvolScientificNameOriginal,
    PolarsAddBvolScientificNameOriginal,
)
from sharkadm.transformers.calculate import (
    # CalculateAbundance,
    # CalculateBiovolume,
    # CalculateCarbon,
    PolarsCalculateAbundance,
)
from sharkadm.transformers.columns import (
    AddColumnViewsColumns,
    AddDEPHqcColumn,
    PolarsAddApprovedKeyColumn,
    PolarsAddColumnViewsColumns,
    PolarsRemoveColumns,
    RemoveColumns,
    SortColumns,
)
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
    AddDatatype,
    AddDatatypePlanktonBarcoding,
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
    PolarsMoveLessThanFlagRowFormat,
    PolarsRemoveNonDataLines,
    RemoveNonDataLines,
)
from sharkadm.transformers.location import (
    AddLocationCounty,
    AddLocationHelcomOsparArea,
    AddLocationMunicipality,
    AddLocationNation,
    AddLocationSeaBasin,
    AddLocationTypeArea,
    AddLocationTYPNFS06,
    AddLocationWaterCategory,
    AddLocationWaterDistrict,
    AddLocationWB,
    PolarsAddLocationCounty,
    PolarsAddLocationR,
    PolarsAddLocationRA,
    PolarsAddLocationRB,
    PolarsAddLocationRC,
    PolarsAddLocationRG,
    PolarsAddLocationRH,
    PolarsAddLocationRO,
    PolarsAddLocationTypeArea,
    PolarsAddLocationWB,
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
from sharkadm.transformers.occurrence_id import AddOccurrenceId
from sharkadm.transformers.orderer import (
    AddEnglishSampleOrderer,
    AddSwedishSampleOrderer,
    PolarsAddEnglishSampleOrderer,
    PolarsAddSwedishSampleOrderer,
)
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
from sharkadm.transformers.replace import ReplaceNanWithEmptyString
from sharkadm.transformers.replace_comma_with_dot import (
    PolarsReplaceCommaWithDot,
    ReplaceCommaWithDot,
    ReplaceCommaWithDotPolars,
)
from sharkadm.transformers.reporting_institute import (
    AddEnglishReportingInstitute,
    AddSwedishReportingInstitute,
    PolarsAddEnglishReportingInstitute,
    PolarsAddSwedishReportingInstitute,
)
from sharkadm.transformers.row import AddRowNumber, PolarsAddRowNumber
from sharkadm.transformers.sampler_area import AddCalculatedSamplerArea
from sharkadm.transformers.sampling_info import AddSamplingInfo
from sharkadm.transformers.scientific_name import (
    SetScientificNameFromDyntaxaScientificName,
    SetScientificNameFromReportedScientificName,
)
from sharkadm.transformers.shark_id import AddSharkId
from sharkadm.transformers.sort_data import SortData, SortDataPlanktonImaging
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
    CopyReportedStationNameToStationName,
)
from sharkadm.transformers.status import SetStatusDataHost, SetStatusDeliverer
from sharkadm.transformers.strip import StripAllValues, StripAllValuesPolars
from sharkadm.transformers.visit import (
    AddVisitKey,
    PolarsAddPhysicalChemicalKey,
    PolarsAddVisitKey,
)
from sharkadm.transformers.wide_to_long import PolarsWideToLong, WideToLong
from sharkadm.transformers.worms import (
    AddReportedAphiaId,
    AddWormsAphiaId,
    AddWormsScientificName,
    PolarsAddReportedAphiaId,
    PolarsAddWormsAphiaId,
    PolarsAddWormsScientificName,
    PolarsSetAphiaIdFromReportedAphiaId,
    SetAphiaIdFromReportedAphiaId,
)
from sharkadm.utils.inspect_kwargs import get_kwargs_for_class


@functools.cache
def get_transformer_list() -> list[str]:
    """Returns a sorted list of name of all available transformers"""
    return sorted(utils.get_all_class_children_names(Transformer))


def get_transformers() -> dict[str, Type[Transformer]]:
    """Returns a dictionary with transformers"""
    return utils.get_all_class_children(Transformer)


def get_transformer_object(name: str, **kwargs) -> Transformer | None:
    """Returns Transformer object that matches the given transformer names"""
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


def get_physical_chemical_transformer_objects() -> list[Transformer]:
    return [
        AddDEPHqcColumn(),
        # AddSamplePosition(),
        # ChangeDateFormat(),
        AddColumnsForAutomaticQC(),
        AddCruiseId(),
        AddVisitKey(),
        # AddStatus(),
        WideToLong(),
    ]
