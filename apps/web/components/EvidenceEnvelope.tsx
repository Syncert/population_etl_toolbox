"use client";

// The one presentation of an analytical block's reproducibility envelope.
//
// The composer and the reading surface show the same block to different
// people — the author while assembling, the reader afterwards — and the
// envelope is the whole reason the second can trust the first. Two renderings
// of it would be two places for a field to quietly stop being shown, so both
// screens present it through here.

import Link from "next/link";
import StatusPill from "./StatusPill";
import { blockLiveStatus, blockReopenHref, reductionLabel } from "../lib/evidencePackets";
import type { PacketBlock } from "../lib/evidencePackets";

export interface EvidenceEnvelopeProps {
  block: PacketBlock;
  /** Hidden when the surface cannot reopen the analysis, e.g. in print. */
  showReopen?: boolean;
}

export default function EvidenceEnvelope({ block, showReopen = true }: EvidenceEnvelopeProps) {
  const envelope = block.envelope;
  if (!envelope) {
    return null;
  }
  const status = blockLiveStatus(envelope);
  return (
    <div data-testid={`envelope-${block.id}`}>
      <StatusPill
        state={status.state}
        label="Basis"
        message={status.label}
        testId={`live-${block.id}`}
      />
      <p className="subtle">{status.detail}</p>
      <dl className="source-grid">
        <div>
          <dt>Measures</dt>
          <dd>{envelope.metricCodes.join(", ") || "Not recorded"}</dd>
        </div>
        <div>
          <dt>Sources</dt>
          <dd>{envelope.sourceCodes.join(", ") || "Not recorded"}</dd>
        </div>
        <div>
          <dt>Geography</dt>
          <dd>{envelope.geoId || envelope.geoLevel || "Not recorded"}</dd>
        </div>
        <div>
          <dt>Period</dt>
          <dd>{envelope.period || "Not recorded"}</dd>
        </div>
        <div>
          <dt>Units</dt>
          <dd>{envelope.units || "Not recorded"}</dd>
        </div>
        <div>
          <dt>Publication</dt>
          <dd>
            {envelope.scope}
            {envelope.release ? ` · ${envelope.release}` : ""}
            {/* The reduction belongs beside the publication it narrows: the
                period above describes one value per geography only when the
                block was read that way (WEB-071). */}
            {reductionLabel(envelope) ? ` · ${reductionLabel(envelope)}` : ""}
          </dd>
        </div>
        <div>
          <dt>Transformation</dt>
          <dd>{envelope.transformation}</dd>
        </div>
        <div>
          <dt>Request</dt>
          <dd>
            <code>{envelope.apiQuery || "Not recorded"}</code>
          </dd>
        </div>
      </dl>
      {envelope.caveats.length > 0 ? (
        <ul data-testid={`caveats-${block.id}`}>
          {envelope.caveats.map((caveat) => (
            <li key={caveat}>{caveat}</li>
          ))}
        </ul>
      ) : null}
      {showReopen ? (
        <Link
          className="text-link no-print"
          href={blockReopenHref(block)}
          data-testid={`reopen-${block.id}`}
        >
          Reopen this analysis
        </Link>
      ) : null}
    </div>
  );
}
