import logging
import xml.etree.ElementTree as ET

import pandas as pd

from .base import DataFile

logger = logging.getLogger(__name__)


class old_XmlDataFile(DataFile):
    @staticmethod
    def extract_data_from_element(element, data):
        for el in element:
            if not el.text.strip():
                old_XmlDataFile.extract_data_from_element(el, data)
            else:
                data[el.tag] = el.text

    def _load_file(self) -> None:
        tree = ET.parse(self._path)
        root = tree.getroot()

        rows = []
        und_data = {}
        for element in root.find("undersokning"):
            if element.tag == "lokal":
                line_data = und_data.copy()
                old_XmlDataFile.extract_data_from_element(element, line_data)
                rows.append(line_data)
                # s = pd.Series(line_data)
            else:
                und_data[element.tag] = element.text

        self._data = pd.DataFrame.from_dict(rows, orient="columns")


class XmlMartransDataFile(DataFile):
    def _load_file(self) -> None:
        all_tags = set()
        lines = []

        tree = ET.parse(self._path)
        root = tree.getroot()

        meta_data = {}
        undersokning_data = {}
        lokal_data = {}
        tillfalle_data = {}
        hydrografi_data = {}
        transekt_data = {}
        avsnitt_data = {}
        avsnittart_data = {}
        avsnittsubstrat_data = {}
        prov_data = {}
        provart_data = {}
        provsubstrat_data = {}
        transekttaxaminmax = {}

        all_tags.update([el.tag for el in root if not el.text.strip()])
        meta_data = dict((el.tag, el.text.strip()) for el in root if el.text.strip())
        for undersokning in root.findall("undersokning"):
            all_tags.update(
                [f"undersokning.{el.tag}" for el in undersokning if not el.text.strip()]
            )
            undersokning_data = dict(
                (f"undersokning.{el.tag}", el.text.strip())
                for el in undersokning
                if el.text.strip()
            )

            for lokal in undersokning.findall("lokal"):
                all_tags.update(
                    [f"lokal.{el.tag}" for el in lokal if not el.text.strip()]
                )
                lokal_data = dict(
                    (f"lokal.{el.tag}", el.text.strip())
                    for el in lokal
                    if el.text.strip()
                )

                for tillfalle in lokal.findall("tillfalle"):
                    all_tags.update(
                        [f"tillfalle.{el.tag}" for el in tillfalle if not el.text.strip()]
                    )
                    tillfalle_data = dict(
                        (f"tillfalle.{el.tag}", el.text.strip())
                        for el in tillfalle
                        if el.text.strip()
                    )
                    # print([el.tag for el in tillfalle if not el.text.strip()])

                    for hydrografi in tillfalle.findall("hydrografi"):
                        all_tags.update(
                            [
                                f"hydrografi.{el.tag}"
                                for el in hydrografi
                                if not el.text.strip()
                            ]
                        )
                        hydrografi_data = dict(
                            (f"hydrografi.{el.tag}", el.text.strip())
                            for el in hydrografi
                            if el.text.strip()
                        )

                    for transekt in tillfalle.findall("transekt"):
                        all_tags.update(
                            [
                                f"transekt.{el.tag}"
                                for el in transekt
                                if not el.text.strip()
                            ]
                        )
                        transekt_data = dict(
                            (f"transekt.{el.tag}", el.text.strip())
                            for el in transekt
                            if el.text.strip()
                        )

                        for avsnitt in transekt.findall("avsnitt"):
                            all_tags.update(
                                [
                                    f"avsnitt.{el.tag}"
                                    for el in avsnitt
                                    if not el.text.strip()
                                ]
                            )
                            avsnitt_data = dict(
                                (f"avsnitt.{el.tag}", el.text.strip())
                                for el in avsnitt
                                if el.text.strip()
                            )
                            # print(
                            #     [
                            #         f'avsnitt.{el.tag}' for el in avsnitt
                            #         if not el.text.strip()
                            #     ]
                            # )

                            for avsnittart in avsnitt.findall("avsnittart"):
                                all_tags.update(
                                    [
                                        f"avsnittart.{el.tag}"
                                        for el in avsnittart
                                        if not el.text.strip()
                                    ]
                                )
                                avsnittart_data = dict(
                                    (f"avsnittart.{el.tag}", el.text.strip())
                                    for el in avsnittart
                                    if el.text.strip()
                                )
                                # print(
                                #     [
                                #         f'avsnitt.{el.tag}'
                                #         for el in avsnitt if not el.text.strip()
                                #     ]
                                # )
                                data = dict()
                                data.update(meta_data)
                                data.update(undersokning_data)
                                data.update(lokal_data)
                                data.update(tillfalle_data)
                                data.update(hydrografi_data)
                                data.update(transekt_data)
                                data.update(avsnitt_data)
                                data.update(avsnittart_data)
                                lines.append(data)

                            for avsnittsubstrat in avsnitt.findall("avsnittsubstrat"):
                                all_tags.update(
                                    [
                                        f"avsnittsubstrat.{el.tag}"
                                        for el in avsnittsubstrat
                                        if not el.text.strip()
                                    ]
                                )
                                avsnittsubstrat_data = dict(
                                    (f"avsnittsubstrat.{el.tag}", el.text.strip())
                                    for el in avsnittsubstrat
                                    if el.text.strip()
                                )
                                # print(
                                #     [
                                #         f'avsnitt.{el.tag}'
                                #         for el in avsnitt if not el.text.strip()
                                #     ]
                                # )
                                data = dict()
                                data.update(meta_data)
                                data.update(undersokning_data)
                                data.update(lokal_data)
                                data.update(tillfalle_data)
                                data.update(hydrografi_data)
                                data.update(transekt_data)
                                data.update(avsnitt_data)
                                data.update(avsnittsubstrat_data)
                                lines.append(data)

                        for transekttaxaminmax in transekt.findall("transekttaxaminmax"):
                            all_tags.update(
                                [
                                    f"transekttaxaminmax.{el.tag}"
                                    for el in transekttaxaminmax
                                    if not el.text.strip()
                                ]
                            )
                            transekttaxaminmax_data = dict(
                                (f"transekttaxaminmax.{el.tag}", el.text.strip())
                                for el in transekttaxaminmax
                                if el.text.strip()
                            )
                            # print(
                            #     [
                            #         f'transekttaxaminmax.{el.tag}'
                            #         for el in transekttaxaminmax if not el.text.strip()
                            #     ]
                            # )
                            data = dict()
                            data.update(meta_data)
                            data.update(undersokning_data)
                            data.update(lokal_data)
                            data.update(tillfalle_data)
                            data.update(hydrografi_data)
                            data.update(transekt_data)
                            data.update(transekttaxaminmax_data)
                            lines.append(data)

                        for prov in transekt.findall("prov"):
                            all_tags.update(
                                [f"prov.{el.tag}" for el in prov if not el.text.strip()]
                            )
                            prov_data = dict(
                                (f"prov.{el.tag}", el.text.strip())
                                for el in prov
                                if el.text.strip()
                            )
                            # print(
                            #     [
                            #         f'prov.{el.tag}' for el in prov
                            #         if not el.text.strip()
                            #     ]
                            # )

                            for provsubstrat in prov.findall("provsubstrat"):
                                all_tags.update(
                                    [
                                        f"provsubstrat.{el.tag}"
                                        for el in provsubstrat
                                        if not el.text.strip()
                                    ]
                                )
                                provsubstrat_data = dict(
                                    (f"provsubstrat.{el.tag}", el.text.strip())
                                    for el in provsubstrat
                                    if el.text.strip()
                                )
                                # print(
                                #     [
                                #         f'avsnitt.{el.tag}' for el in avsnitt
                                #         if not el.text.strip()
                                #     ]
                                # )
                                data = dict()
                                data.update(meta_data)
                                data.update(undersokning_data)
                                data.update(lokal_data)
                                data.update(tillfalle_data)
                                data.update(hydrografi_data)
                                data.update(transekt_data)
                                data.update(prov_data)
                                data.update(provsubstrat_data)
                                lines.append(data)

                            for provart in prov.findall("provart"):
                                all_tags.update(
                                    [
                                        f"provart.{el.tag}"
                                        for el in provart
                                        if not el.text.strip()
                                    ]
                                )
                                provart_data = dict(
                                    (f"provart.{el.tag}", el.text.strip())
                                    for el in provart
                                    if el.text.strip()
                                )
                                # print(
                                #     [
                                #         f'avsnitt.{el.tag}' for el in avsnitt
                                #         if not el.text.strip()
                                #     ]
                                # )
                                data = dict()
                                data.update(meta_data)
                                data.update(undersokning_data)
                                data.update(lokal_data)
                                data.update(tillfalle_data)
                                data.update(hydrografi_data)
                                data.update(transekt_data)
                                data.update(prov_data)
                                data.update(provart_data)
                                lines.append(data)

        self._data = pd.DataFrame.from_records(lines)
        self._data.fillna("", inplace=True)

        # self._fix_reported_scientific_name()
        # self._fix_dyntaxa()
        # self._fix_variable_comment()
        # self._fix_cover()

        self._concatenate(
            "reported_scientific_name",
            [
                "avsnittart.taxon_namn",
                "provart.taxon_namn",
                "taxon.taxon_namn",
                "transekttaxaminmax.taxon_namn",
            ],
        )
        self._concatenate(
            "dyntaxa_id",
            [
                "transekttaxaminmax.transekttaxaminmax_taxonID",
                "avsnittart.taxonID",
                "provart.provart_taxonID",
                "provart.taxonID",
            ],
        )
        self._concatenate(
            "variable_comment",
            [
                "provart.provart_kommentar",
                "transekttaxaminmax.transekttaxaminmax_kommentar",
                "avsnitt.avsnittart_kommentar",
                "bitmarken.bitmarken_beskrivning",
                "nyrekrytering.nyrekrytering_beskrivning",
                "avsnittart.kommentar",
                "provart.kommentar",
                "avsnittart.nyrekrytering_nyrekrytering",
                "avsnittart.bitmarken_bitmarken",
                "transekttaxaminmax.kommentar",
            ],
        )
        self._concatenate(
            "variable.COPY_VARIABLE.Cover.%",
            [
                "provartstorlek.provartstorlek_abundans",
                "provart.provart_abundans",
                "avsnittart.avsnittart_antal",
                "avsnittart.antal",
                "provart.abundans",
            ],
        )
        self._concatenate(
            "taxon_photo",
            ["avsnittart.foto", "provart.provart_foto", "provart.foto", "avsnitt.foto"],
        )
        self._concatenate(
            "epibiont",
            [
                "transekttaxaminmax.transekttaxaminmax_epibiotisk",
                "provart.provart_epibiotisk",
                "avsnittart.epibiotisk",
                "provart.epibiotisk",
                "transekttaxaminmax.epibiotisk",
            ],
        )
        self._concatenate(
            "species_flag_code",
            [
                "transekttaxaminmax.transekttaxaminmax_sflag",
                "provart.provart_sflag",
                "avsnittart.avsnittart_sflag",
                "avsnittart.sflag",
                "provart.sflag",
                "transekttaxaminmax.sflag",
            ],
        )
        self._concatenate(
            "sample_depth_m", ["prov.prov_djup", "hydrografi.djup", "prov.djup"]
        )
        self._concatenate(
            "COPY_VARIABLE.Cover.%",
            [
                "avsnittart.tackningsgrad",
                "provart.provart_tackningsgrad",
                "provart.tackningsgrad",
            ],
        )
        self._concatenate(
            "CREATE_VARIABLE.Total cover of all species.%",
            ["avsnitt.totalcover", "prov.totalcover"],
        )
        self._concatenate(
            "section_distance_start_m", ["avsnitt.startavstand", "prov.avstand"]
        )
        self._concatenate(
            "section_distance_end_m", ["avsnitt.slutavstand", "prov.avsnitt_slutavstand"]
        )
        self._concatenate("taxanomist", ["transekt.inventerare", "prov.sorterare"])

    def _concatenate(self, result_col: str, columns: list[str]) -> None:
        cols = [col for col in columns if col in self._data.columns]
        self._data[result_col] = self._data[cols].astype(str).apply("".join, axis=1)
        self._data.drop(cols, axis="columns", inplace=True)

    # def _fix_reported_scientific_name(self):
    #     cols = [
    #         'avsnittart.taxon_namn', 'provart.taxon_namn', 'taxon.taxon_namn',
    #         'transekttaxaminmax.taxon_namn'
    #     ]
    #     cols = [col for col in cols if col in self._data.columns]
    #     self._data['reported_scientific_name'] = self._data[cols].apply(''.join, axis=1)
    #     self._data.drop(cols, axis='columns', inplace=True)
    #
    # def _fix_dyntaxa(self):
    #     cols = [
    #         'transekttaxaminmax.transekttaxaminmax_taxonID', 'avsnittart.taxonID',
    #         'provart.provart_taxonID', 'provart.taxonID'
    #     ]
    #     cols = [col for col in cols if col in self._data.columns]
    #     self._data['dyntaxa_id'] = self._data[cols].apply(''.join, axis=1)
    #     self._data.drop(cols, axis='columns', inplace=True)
    #
    # def _fix_variable_comment(self):
    #     cols = [
    #         'provart.provart_kommentar',
    #         'transekttaxaminmax.transekttaxaminmax_kommentar',
    #         'avsnitt.avsnittart_kommentar',
    #         'bitmarken.bitmarken_beskrivning',
    #         'nyrekrytering.nyrekrytering_beskrivning',
    #         'avsnittart.kommentar',
    #         'provart.kommentar',
    #         'avsnittart.nyrekrytering_nyrekrytering',
    #         'avsnittart.bitmarken_bitmarken',
    #         'transekttaxaminmax.kommentar	segmenttaxonkommentar',
    #         'kommentar',
    #         'COMNT_VAR'
    #     ]
    #     cols = [col for col in cols if col in self._data.columns]
    #     self._data['variable_comment'] = self._data[cols].apply(''.join, axis=1)
    #     self._data.drop(cols, axis='columns', inplace=True)
    #
    # def _fix_cover(self):
    #     cols = [
    #         'provartstorlek.provartstorlek_abundans',
    #         'provart.provart_abundans',
    #         'avsnittart.avsnittart_antal',
    #         'avsnittart.antal',
    #         'provart.abundans'
    #     ]
    #     cols = [col for col in cols if col in self._data.columns]
    #     self._data['variable.COPY_VARIABLE.Cover.%'] = self._data[cols].apply(
    #         ''.join, axis=1
    #     )
    #     self._data.drop(cols, axis='columns', inplace=True)
    #
    # def _fix_taxon_photo(self):
    #     cols = [
    #         'avsnittart.foto', 'provart.provart_foto', 'provart.foto', 'avsnitt.foto'
    #     ]
    #     cols = [col for col in cols if col in self._data.columns]
    #     self._data['taxon_photo'] = self._data[cols].apply(''.join, axis=1)
    #     self._data.drop(cols, axis='columns', inplace=True)
