# FBI Return A master file fixtures

Public data from the Crime Data Explorer downloads page (Master File Downloads,
Return A). The page serves each file through
`https://cde.ucr.cjis.gov/LATEST/s3/signedurl?key=master_files/reta/reta-<year>.zip`;
the layout guide is `master_files/reta/reta-help.zip`.

- `Ret A Rec Descrip.pdf`, `Ret A negative entries.pdf`: the contents of
  `reta-help.zip`, unchanged (record layout with field positions; overpunch table).
- `reta-2023-sample.txt`: 47 whole 7,385-character records copied verbatim from
  `reta-2023.txt` (6.25 MB zipped, 195.7 MB unzipped, 26,490 records), latin-1, LF.
  Every agency in Wisconsin counties 013 (Dane), 053 (Rock) and 005 (Brown);
  Travis County, Texas, NIBRS-converted agencies TX22700, TX22701 (three county
  slices) and TX22702; NJ00408, the only 2023 record with an overpunched
  negative; WI01102 (three county slices); several months_reported = 00 records.

County codes (positions 54-56, also ORI characters 3-5) are the state's counties
in alphabetical order, not FIPS: Dane 013, Rock 053, Brown 005, Travis 227. In
both states FIPS = 2 x code - 1, but that holds only where FIPS codes are the
odd numbers in alphabetical order, so a load needs a crosswalk table.
