// The API-derived correlation, presented with everything that qualifies it.
//
// Two rules shape this component, and both are about what a reader takes away
// from a single number:
//
// - **The association-not-causation sentence is first, and is not
//   collapsible.** It is a product rule, not a courtesy
//   (`docs/product/TOP_20_DATA_PRODUCT_USE_CASES.md`: "Cross-source
//   association must never be presented as causation"), and a caveat behind a
//   disclosure control is a caveat most readers never open. The API sends it
//   first in `caveats`; this renders it first and in full.
// - **Every coefficient is labelled API-derived wherever it appears.** No
//   source publishes a correlation. The label rides each reading and the
//   export, so a figure lifted from this panel into a document carries the
//   fact that this API computed it.
//
// A null coefficient shows its reason rather than an em-dash. The API already
// sent the reason — too few pairs, a constant side — and a blank where a
// number should be teaches a reader that the screen is broken.

import {
  CORRELATION_IS_ACROSS_GEOGRAPHIES,
  formatCoefficient,
} from "../lib/workbench";
import type { CorrelationReading } from "../lib/workbench";

export default function CorrelationPanel({
  readings,
  caveats,
  year,
  periodA,
  periodB,
  testId = "workbench-correlation",
}: {
  readings: CorrelationReading[];
  /** The API's own caveats, in the API's order: causation first. */
  caveats: string[];
  /** The same-year pin the answer was read under, or `null`. */
  year?: number | null;
  periodA?: string | null;
  periodB?: string | null;
  testId?: string;
}) {
  if (readings.length === 0) {
    return null;
  }
  return (
    <section className="card" data-testid={testId} aria-label="Correlation">
      <h3>Correlation</h3>

      {caveats.length > 0 ? (
        <p className="notice" data-testid="workbench-correlation-causation">
          <strong>{caveats[0]}</strong>
        </p>
      ) : null}

      <dl className="correlation-readings">
        {readings.map((reading) => (
          <div key={reading.label} data-testid="correlation-reading">
            <dt>
              {reading.label}
              {reading.derived ? (
                <span className="pill warn" data-testid="correlation-derived">
                  API-derived
                </span>
              ) : null}
            </dt>
            <dd>{reading.value}</dd>
          </div>
        ))}
      </dl>

      <p className="subtle" data-testid="workbench-correlation-scope">
        {CORRELATION_IS_ACROSS_GEOGRAPHIES}
      </p>

      <p className="subtle" data-testid="workbench-correlation-periods">
        {year
          ? `Both sides were reduced within ${year}, so the pairs are same-year at the cost of the coverage reported above.`
          : "Each side reduced to its own newest published value, so a pair can combine two periods. Pin a year to ask for a same-year answer."}
        {periodA || periodB
          ? ` Periods: ${periodA || "several"} against ${periodB || "several"}.`
          : ""}
      </p>

      {caveats.length > 1 ? (
        <ul className="subtle" data-testid="workbench-correlation-caveats">
          {caveats.slice(1).map((caveat) => (
            <li key={caveat}>{caveat}</li>
          ))}
        </ul>
      ) : null}
    </section>
  );
}

export { formatCoefficient };
