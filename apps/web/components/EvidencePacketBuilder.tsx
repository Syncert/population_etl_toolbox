"use client";

// The evidence packet composer.
//
// Composition is where analytical context is most easily lost: a chart
// lifted out of the explorer and dropped into a document usually keeps its
// shape and loses which measure it was, for where, over what period, at what
// publication, and with what caveats. So an analytical block here can only
// be filled from a saved view that already recorded its envelope, every
// block shows that envelope inline, and the packet refuses to call itself
// complete while any analytical block is missing context.

import { useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { Download, FileText, Plus, Printer, Save, Trash2 } from "lucide-react";
import StatusPill from "./StatusPill";
import { BUILDER_DRAFT_KEY, readSavedCharts } from "../lib/savedCharts";
import {
  blockLiveStatus,
  blockReopenHref,
  envelopeFromSavedChart,
  grantNeedsTemplate,
  isAnalyticalBlock,
  packetExport,
  packetIsComplete,
  packetIssues,
} from "../lib/evidencePackets";
import type { EvidencePacket, PacketBlock } from "../lib/evidencePackets";

function newId(prefix: string): string {
  return `${prefix}:${Math.random().toString(36).slice(2, 10)}`;
}

export default function EvidencePacketBuilder() {
  const [packet, setPacket] = useState<EvidencePacket>(() => grantNeedsTemplate());
  const [savedCharts, setSavedCharts] = useState<Record<string, unknown>[]>([]);
  const [targetBlockId, setTargetBlockId] = useState("");
  const [saveState, setSaveState] = useState("Draft stored in this browser");
  const [preview, setPreview] = useState(false);

  useEffect(() => {
    setSavedCharts(readSavedCharts());
    try {
      const draft = JSON.parse(window.localStorage.getItem(BUILDER_DRAFT_KEY) || "null");
      if (draft?.version === 1 && Array.isArray(draft.blocks)) {
        setPacket(draft as EvidencePacket);
      }
    } catch {
      // Keep the template; a malformed draft is not silently merged.
    }
  }, []);

  const issues = useMemo(() => packetIssues(packet), [packet]);
  const complete = useMemo(() => packetIsComplete(packet), [packet]);
  const issueByBlock = useMemo(
    () => new Map(issues.map((issue) => [issue.blockId, issue])),
    [issues],
  );

  const updateBlock = useCallback((id: string, patch: Partial<PacketBlock>) => {
    setPacket((current) => ({
      ...current,
      blocks: current.blocks.map((block) => (block.id === id ? { ...block, ...patch } : block)),
      updatedAt: new Date().toISOString(),
    }));
  }, []);

  function addBlock(type: PacketBlock["type"], title: string) {
    setPacket((current) => ({
      ...current,
      blocks: [...current.blocks, { id: newId(type), type, title, content: "" }],
      updatedAt: new Date().toISOString(),
    }));
  }

  function removeBlock(id: string) {
    setPacket((current) => ({
      ...current,
      blocks: current.blocks.filter((block) => block.id !== id),
      updatedAt: new Date().toISOString(),
    }));
  }

  // A saved view brings its recorded envelope with it. A view that never
  // captured a field leaves it empty, so the packet reports the gap rather
  // than the composer inventing a value to fill it.
  function attachSavedView(chart: Record<string, unknown>) {
    const blockId =
      targetBlockId ||
      packet.blocks.find((block) => isAnalyticalBlock(block) && !block.envelope)?.id ||
      "";
    const envelope = envelopeFromSavedChart(chart);
    const document = chart.metricCodeB
      ? {
          kind: "comparison" as const,
          metric_code_a: String(chart.metricCode || ""),
          metric_code_b: String(chart.metricCodeB || ""),
          filters: { geo_level: String(chart.geoLevel || "") },
        }
      : {
          kind: "observations" as const,
          metric_code: String(chart.metricCode || ""),
          filters: {
            geo_level: String(chart.geoLevel || ""),
            geo_id: String(chart.geoId || ""),
          },
        };

    if (blockId) {
      updateBlock(blockId, {
        envelope,
        document,
        content: String(chart.title || ""),
      });
      return;
    }
    setPacket((current) => ({
      ...current,
      blocks: [
        ...current.blocks,
        {
          id: newId("analysis"),
          type: "analysis",
          title: String(chart.title || "Saved view"),
          content: String(chart.title || ""),
          envelope,
          document,
        },
      ],
      updatedAt: new Date().toISOString(),
    }));
  }

  function persist() {
    window.localStorage.setItem(BUILDER_DRAFT_KEY, JSON.stringify(packet));
    setSaveState(`Saved ${new Date().toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })}`);
  }

  function exportCsv() {
    const { headings, rows, filename } = packetExport(packet);
    const escape = (value: unknown) => `"${String(value ?? "").replaceAll('"', '""')}"`;
    const content = [headings, ...rows].map((row) => row.map(escape).join(",")).join("\n");
    const blob = new Blob([content], { type: "text/csv;charset=utf-8" });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = filename;
    link.click();
    URL.revokeObjectURL(link.href);
  }

  return (
    <main
      className="page-shell"
      data-testid="evidence-packet"
      data-block-count={packet.blocks.length}
      data-issue-count={issues.length}
      data-complete={complete ? "true" : "false"}
      data-preview={preview ? "true" : "false"}
    >
      <header className="page-heading no-print">
        <div className="section-kicker">Evidence packet</div>
        <h1>Needs assessment composer</h1>
        <p>
          Analytical blocks are filled from saved views, which bring their own reproducibility
          envelope. A block that would present values without the context needed to read them is
          reported below rather than rendered as finished evidence.
        </p>
      </header>

      <section className="status-row no-print">
        <StatusPill
          state={complete ? "ok" : "warn"}
          label="Packet"
          message={
            complete
              ? `${packet.blocks.length} blocks, every analytical block has its envelope`
              : `${issues.length} analytical block${issues.length === 1 ? "" : "s"} missing context`
          }
          testId="packet-status"
        />
      </section>

      <section className="profile-controls no-print">
        <label>
          Title
          <input
            value={packet.title}
            onChange={(event) =>
              setPacket((current) => ({ ...current, title: event.target.value }))
            }
            data-testid="packet-title"
          />
        </label>
        <button className="button secondary" type="button" onClick={() => addBlock("text", "Narrative")} data-testid="add-text">
          <FileText size={15} /> Add narrative
        </button>
        <button className="button secondary" type="button" onClick={() => addBlock("caveat", "Caveat")} data-testid="add-caveat">
          <Plus size={15} /> Add caveat
        </button>
        <button className="button secondary" type="button" onClick={persist} data-testid="packet-save">
          <Save size={15} /> Save draft
        </button>
        <button className="button secondary" type="button" onClick={exportCsv} data-testid="packet-export">
          <Download size={15} /> Export evidence
        </button>
        <button
          className="button secondary"
          type="button"
          onClick={() => setPreview((value) => !value)}
          data-testid="packet-preview"
        >
          {preview ? "Back to editing" : "Preview"}
        </button>
        <button className="button primary" type="button" onClick={() => window.print()} data-testid="packet-print">
          <Printer size={15} /> Print
        </button>
        <span className="subtle">{saveState}</span>
      </section>

      {issues.length > 0 ? (
        <section className="coverage-note partial no-print" data-testid="packet-issues">
          <strong>These blocks cannot be read as evidence yet:</strong>
          <ul>
            {issues.map((issue) => (
              <li key={issue.blockId} data-testid={`issue-${issue.blockId}`}>
                <strong>{issue.title}</strong>: {issue.reason} (missing {issue.missing.join(", ")})
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      <section className="builder-shell">
        {!preview ? (
          <aside className="builder-library no-print" data-testid="packet-library">
            <div className="library-heading">Saved views</div>
            <p className="subtle">
              Only a saved view can fill an analytical block, because only a saved view carries
              the envelope the block needs.
            </p>
            <label>
              Fill block
              <select
                value={targetBlockId}
                onChange={(event) => setTargetBlockId(event.target.value)}
                data-testid="packet-target"
              >
                <option value="">First empty analytical block</option>
                {packet.blocks.filter(isAnalyticalBlock).map((block) => (
                  <option value={block.id} key={block.id}>
                    {block.title}
                  </option>
                ))}
              </select>
            </label>
            {savedCharts.length === 0 ? (
              <div className="empty-state compact" data-testid="packet-library-empty">
                Save a view in the Explorer or the comparison workspace to make it available here.
              </div>
            ) : (
              savedCharts.map((chart) => (
                <button
                  className="library-button"
                  type="button"
                  key={String(chart.id)}
                  onClick={() => attachSavedView(chart)}
                  data-testid={`packet-attach-${String(chart.id)}`}
                >
                  <span>
                    <strong>{String(chart.title || chart.id)}</strong>
                    <small>{String(chart.metricCode || "")}</small>
                  </span>
                  <Plus size={15} />
                </button>
              ))
            )}
          </aside>
        ) : null}

        <section className="builder-workspace">
          <article className="packet-document" data-testid="packet-document">
            <h2>{packet.title}</h2>
            <p className="subtle">{packet.purpose}</p>

            {packet.blocks.map((block) => {
              const issue = issueByBlock.get(block.id);
              const status = blockLiveStatus(block.envelope);
              return (
                <article
                  className="builder-block"
                  key={block.id}
                  data-testid={`block-${block.id}`}
                  data-block-type={block.type}
                  data-has-envelope={block.envelope ? "true" : "false"}
                >
                  <div className="panel-heading">
                    <div>
                      <div className="section-kicker">{block.type}</div>
                      <h3>{block.title}</h3>
                    </div>
                    {!preview ? (
                      <button
                        className="icon-button"
                        type="button"
                        aria-label={`Remove ${block.title}`}
                        onClick={() => removeBlock(block.id)}
                        data-testid={`remove-${block.id}`}
                      >
                        <Trash2 size={16} />
                      </button>
                    ) : null}
                  </div>

                  {preview ? (
                    <p>{block.content}</p>
                  ) : (
                    <textarea
                      aria-label={`${block.title} content`}
                      value={block.content || ""}
                      onChange={(event) => updateBlock(block.id, { content: event.target.value })}
                      data-testid={`content-${block.id}`}
                    />
                  )}

                  {isAnalyticalBlock(block) ? (
                    block.envelope ? (
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
                            <dd>{block.envelope.metricCodes.join(", ") || "Not recorded"}</dd>
                          </div>
                          <div>
                            <dt>Sources</dt>
                            <dd>{block.envelope.sourceCodes.join(", ") || "Not recorded"}</dd>
                          </div>
                          <div>
                            <dt>Geography</dt>
                            <dd>
                              {block.envelope.geoId || block.envelope.geoLevel || "Not recorded"}
                            </dd>
                          </div>
                          <div>
                            <dt>Period</dt>
                            <dd>{block.envelope.period || "Not recorded"}</dd>
                          </div>
                          <div>
                            <dt>Publication</dt>
                            <dd>
                              {block.envelope.scope}
                              {block.envelope.release ? ` · ${block.envelope.release}` : ""}
                            </dd>
                          </div>
                          <div>
                            <dt>Transformation</dt>
                            <dd>{block.envelope.transformation}</dd>
                          </div>
                          <div>
                            <dt>Request</dt>
                            <dd>
                              <code>{block.envelope.apiQuery || "Not recorded"}</code>
                            </dd>
                          </div>
                        </dl>
                        {block.envelope.caveats.length > 0 ? (
                          <ul data-testid={`caveats-${block.id}`}>
                            {block.envelope.caveats.map((caveat) => (
                              <li key={caveat}>{caveat}</li>
                            ))}
                          </ul>
                        ) : null}
                        <Link
                          className="text-link no-print"
                          href={blockReopenHref(block)}
                          data-testid={`reopen-${block.id}`}
                        >
                          Reopen this analysis
                        </Link>
                      </div>
                    ) : (
                      <p className="coverage-note partial" data-testid={`empty-${block.id}`}>
                        {issue?.reason || "this block presents no analysis yet"}. Attach a saved
                        view to fill it.
                      </p>
                    )
                  ) : null}
                </article>
              );
            })}
          </article>
        </section>
      </section>
    </main>
  );
}
