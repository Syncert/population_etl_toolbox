# SOI county migration rows, 2011-2012 layout

`countyinflow1112.csv` is the header and the first eleven rows of
<https://www.irs.gov/pub/irs-soi/countyinflow1112.csv>, copied verbatim on
2026-11-05: Alabama's five state summary rows (subject county `000`) and
Autauga County's file totals and non-migrants. The files from 2011-2012 to
2017-2018 share this header with the files from 2018-2019; `n2` counts
exemptions. Kept in its own folder so the DAG pipeline stub, which serves
fixtures by file name, keeps answering 2011-2012 with an empty file.
