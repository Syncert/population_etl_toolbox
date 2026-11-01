r"""Check every by-age rate scheme's lines against the Bureau's own labels.

Reads ``gold_census.dim_acs_variable`` (the labels the Census API published
for each loaded variable) and prints every disagreement
``semantics.age_bands.rate_label_problems`` finds: a numerator line that does
not name what the scheme counts, a line of the wrong sex, or a line whose age
falls outside its group. Read-only. On OFFICE_LAB:

    cd H:\population_etl_toolbox
    $env:ANALYTICS_DB_HOST="localhost"; $env:ANALYTICS_DB_PORT="5435"
    python scripts/check_age_rate_labels.py --env-file infra/docker/stack.env

Every release the warehouse holds labels for is checked (``--releases``
narrows it, e.g. ``acs5:2013,acs5:2023``), each group as that release labels
it. Exit status 0 when every scheme agrees with the labels of every checked
release, 1 otherwise.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_ingestion_toolbox.semantics.age_bands import (  # noqa: E402
    load_rate_registry,
    rate_label_problems,
)

RELEASES_SQL = """
SELECT DISTINCT dataset_code, vintage_year
FROM gold_census.dim_acs_variable
WHERE dataset_code = ANY(%(datasets)s)
  AND regexp_replace(variable_code, 'E$', '') = ANY(%(codes)s)
ORDER BY dataset_code, vintage_year
"""

LABELS_SQL = """
SELECT DISTINCT ON (variable)
       regexp_replace(variable_code, 'E$', '') AS variable, variable_label
FROM gold_census.dim_acs_variable
WHERE dataset_code = %(dataset)s AND vintage_year = %(vintage)s
  AND value_role = 'ESTIMATE'
  AND regexp_replace(variable_code, 'E$', '') = ANY(%(codes)s)
ORDER BY variable
"""


def _env(path: str | None) -> None:
    if not path:
        return
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        key, sep, value = line.partition("=")
        if sep and not key.startswith("#"):
            os.environ.setdefault(key.strip(), value.strip())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--env-file")
    parser.add_argument("--releases", default="")
    args = parser.parse_args()
    _env(args.env_file)

    import psycopg2

    connection = psycopg2.connect(
        host=os.environ.get("ANALYTICS_DB_HOST", "localhost"),
        port=os.environ.get("ANALYTICS_DB_PORT", "5435"),
        dbname=os.environ["ANALYTICS_DB_NAME"],
        user=os.environ["ANALYTICS_DB_USER"],
        password=os.environ["ANALYTICS_DB_PASSWORD"],
    )
    failed = False
    with connection, connection.cursor() as cursor:
        for scheme in load_rate_registry().values():
            if args.releases:
                releases = [r.split(":") for r in args.releases.split(",")]
            else:
                cursor.execute(
                    RELEASES_SQL,
                    {
                        "datasets": list(scheme.datasets),
                        "codes": list(scheme.variables()),
                    },
                )
                releases = [(d, str(v)) for d, v in cursor.fetchall()]
            for dataset, vintage in releases:
                release = f"{dataset}:{vintage}"
                cursor.execute(
                    LABELS_SQL,
                    {
                        "dataset": dataset,
                        "vintage": int(vintage),
                        "codes": list(scheme.variables()),
                    },
                )
                labels = dict(cursor.fetchall())
                if not labels:
                    print(f"{scheme.rate_id} {release}: table not loaded, skipped")
                    continue
                problems = rate_label_problems(scheme, labels, int(vintage))
                print(
                    f"{scheme.rate_id} {release}: {len(labels)} labels, "
                    f"{len(problems)} problems"
                )
                for problem in problems:
                    print(f"  {problem}")
                failed = failed or bool(problems)
    connection.close()
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
