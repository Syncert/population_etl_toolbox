import { ExternalLink } from "lucide-react";
import { formatDate } from "../lib/format";

/**
 * What the API says about the source behind a view.
 *
 * Every field here is the API's or the catalog's. This panel used to end
 * with a fixed link to Census ACS estimate guidance -- on every source, so a
 * reader looking at BLS unemployment, FRED, CDC, USDA NASS or FBI UCR was
 * pointed at the Census Bureau's guidance for a survey they were not
 * reading. `/catalog/sources` publishes `reference_url` per source, and no
 * file under `apps/web` read it.
 */
export default function SourceNote({
  source,
  sourceName,
  referenceUrl,
  dataset,
  metric,
  geography,
  period,
  updatedAt,
  transformation = "Raw value",
  caveats,
}) {
  return (
    <section className="source-note" aria-label="Source and methodology">
      <div className="section-kicker">Source and methodology</div>
      <dl className="source-grid">
        <div><dt>Source</dt><dd>{sourceName || source || "Not reported"}</dd></div>
        <div><dt>Dataset</dt><dd>{dataset || "Not reported"}</dd></div>
        <div><dt>Metric</dt><dd>{metric || "Not selected"}</dd></div>
        <div><dt>Geography</dt><dd>{geography || "Not selected"}</dd></div>
        <div><dt>Coverage</dt><dd>{period || "Latest available"}</dd></div>
        <div><dt>Transformation</dt><dd>{transformation}</dd></div>
        <div><dt>Catalog updated</dt><dd>{updatedAt ? formatDate(updatedAt) : "Not reported"}</dd></div>
      </dl>
      {caveats ? <p className="method-note">{caveats}</p> : null}
      {/* Rendered only where the source publishes one. A source with no
          reference gets no link, rather than someone else's. */}
      {referenceUrl ? (
        <a className="text-link" href={referenceUrl} target="_blank" rel="noreferrer">
          {sourceName || source || "Source"} reference{" "}
          <ExternalLink aria-hidden="true" size={14} />
        </a>
      ) : null}
    </section>
  );
}
