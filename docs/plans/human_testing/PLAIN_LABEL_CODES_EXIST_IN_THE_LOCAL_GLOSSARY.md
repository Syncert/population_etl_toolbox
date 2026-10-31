# Check: every plain-label code names a metric the warehouse publishes

## What this checks

That each metric code in `docs/semantics/metric_definitions.json` is a code
`gold_glossary.dim_metric` on the internal stack actually holds, so no
definition sits on a code nothing serves, and that the new label fields come
back from the live routes.

## Why it was not automated

The unit tier proves every code the place pages ask for has an entry, and
that every response naming a metric carries its label. Whether those codes
exist is a fact about the populated warehouse, which a cloud session cannot
reach. On 2026-10-26, 141 of 144 codes existed; the codes added on
2026-10-31 (EIA gasoline, BLS regional gasoline and food-at-home CPI, BEA
price parities) have not been checked.

- Filed by: [`docs/plans/needs_review/API_PLAIN_LABELS_PLAN.md`](../needs_review/API_PLAIN_LABELS_PLAN.md)
- Catalog row: **API-170** in [`docs/reference/TESTING_CONTRACT.md`](../../reference/TESTING_CONTRACT.md)

## You need

- The internal stack up (`make deploy-up`), with the warehouse populated
- Python 3.11 with the repository installed, and `DATABASE_URL` pointing at
  the warehouse

**Time:** about 2 minutes. **Touches:** read-only queries.

## Steps

1. List the registry codes the glossary does not hold:

   ```bash
   python - <<'PY'
   import os, sqlalchemy as sa
   from data_ingestion_toolbox.semantics.definitions import registry
   codes = sorted(registry())
   engine = sa.create_engine(os.environ["DATABASE_URL"])
   with engine.connect() as db:
       held = {r[0] for r in db.execute(sa.text(
           "SELECT metric_code FROM gold_glossary.dim_metric "
           "WHERE metric_code = ANY(:c)"), {"c": codes})}
   missing = [c for c in codes if c not in held]
   print(f"{len(held)} of {len(codes)} codes held")
   print("\n".join(missing))
   PY
   ```

   Expected: every code held except
   `CENSUS_BPS:units:1_unit:annual`, `CENSUS_BPS:units:5_plus_units:annual`
   (annual permits not loaded locally) and `BLS_QCEW:employment:92:5`
   (public administration has no private ownership). Any other missing code
   is a defect: either the entry names the wrong code or the page asks for a
   metric that is not loaded.

2. Ask the live API for a labelled comparison and a distribution:

   ```bash
   curl -s "http://localhost:8000/api/v1/comparison?metric_code_a=CENSUS_ACS:acs5:B19013_001&metric_code_b=CENSUS_ACS:acs5:B25064_001&geo_level=STATE&limit=1" | python -m json.tool | grep metric_label
   curl -s "http://localhost:8000/api/v1/distribution/bins?metric_code=CENSUS_ACS:acs5:B19013_001&geo_level=COUNTY" | python -m json.tool | grep metric_label
   ```

   Expected: `metric_label_a` "Median household income" with status
   `draft`, and the same label on the bins.

## Record the result

Note the date, the counts from step 1 and the step 2 output in the plan's
evidence, then move this file to `completed/`.
