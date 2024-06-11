Denna server innehåller marin miljöövervakningsdata som levererats till datavärden SMHI.

Filerna är ordnade efter Övervakningsår/Datatyp/Dataleverantör (enligt RLABO-koder; se SMHIs kodlista)/Versionsnummer enligt datum. 

Detta datapaket innehåller klorofyll data.

Undersökningstyp: Vattenprover samlas antingen in med en vertikalt hängande slang som försluts och ger ett blandat prov från olika djup eller med en flaska från fasta djup. Vattnet filtreras. Klorofyllhalten bestäms utifrån fluorescensmätningar.

Datapaket är komprimerade i zip-format.

Zip-filen innehåller:

- Filen "README.txt": Beskriver på engelska innehållet i zip-filen samt användarvillkor för data.

- Filen "README_sv.txt": Beskriver på svenska innehållet i zip-filen samt användarvillkor för data.

- Filen "shark_metadata.txt": Skapad av datavärden med en allmän beskrivning av levererade data.

- Mappen "processed_data": Innehåller en eller flera redigerade datafiler (kan vara semikolonseparerat, tabbavgränsade eller xml filer), som är korrigerad (t.ex. koder, arter och stationsnamn) och anpassad till SHARKs datahanteringssystem, en delivery_note.txt där dataleverantören visar vad dataleveransen innehåller, en analyse_info.txt där information om analyserna presenteras, en sampling_info.txt där information om provtagningen presenteras och ibland en change_log.txt fil som dokumenterar förändringar gjorda av datavärden under leveranskontroll och databearbetning.

- Mappen "received_data": Innehåller en eller flera levererade datafiler från dataleverantören (kan vara semikolonseparerat, tabbavgränsade, excel-filer eller xml filer), samt medföljande filer från leverantören (i olika format som t.ex. pdf) som anses vara relevant tillsammans med levererade data.

- Mappen "shark_generated": Innehåller shark_data.txt som visar bearbetad data med översätta kolumnkoder, en shark_column_data.txt med data organiserad i en tabell med en rad per prov, en shark_metadata_auto.txt med detaljerad information om data (t.ex. minimum och maximum latitud samt enheter) och en shark_translate.txt som innehåller översättningar för koder använda i data. 


Data är fria att användas enligt den datapolicy/användarvillkor som beskrivs i länken "Villkor för nedladdning" längst ned på sidan för SHARKweb (https://sharkweb.smhi.se/).

Kontakta shark@smhi.se om du har frågor eller kommentarer.

På hemsidan https://www.smhi.se/klimatdata/oceanografi/havsmiljodata finns artlistor för olika år och en vägledning för datarapportering där man bland annat hittar kodlistor som beskriver de koder som används inom datavärdskapet.