import { ExternalLink } from "lucide-react";

export default function SourceNote({
  source,
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
        <div><dt>Source</dt><dd>{source || "Not reported"}</dd></div>
        <div><dt>Dataset</dt><dd>{dataset || "Not reported"}</dd></div>
        <div><dt>Metric</dt><dd>{metric || "Not selected"}</dd></div>
        <div><dt>Geography</dt><dd>{geography || "Not selected"}</dd></div>
        <div><dt>Coverage</dt><dd>{period || "Latest available"}</dd></div>
        <div><dt>Transformation</dt><dd>{transformation}</dd></div>
        <div><dt>Catalog updated</dt><dd>{updatedAt ? new Date(updatedAt).toLocaleDateString() : "Not reported"}</dd></div>
      </dl>
      {caveats ? <p className="method-note">{caveats}</p> : null}
      <a className="text-link" href="https://www.census.gov/programs-surveys/acs/guidance/estimates.html" target="_blank" rel="noreferrer">
        ACS estimate guidance <ExternalLink aria-hidden="true" size={14} />
      </a>
    </section>
  );
}
