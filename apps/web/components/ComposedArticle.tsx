"use client";

// The reading surface for a composed evidence packet.
//
// This route used to be a hand-written article: one hard-coded ACS measure
// for one hard-coded county, with a headline percentage this client computed
// itself from two observations. That number was the problem. It was an
// analytical value the warehouse never published, rendered in the same
// typeface as the published series beside it, and derived by arithmetic that
// read a suppressed value as a real one. A reader had no way to tell which
// of the two the page was showing them.
//
// So nothing here computes an analytical value. The page presents blocks the
// composer produced, each with the reproducibility envelope it recorded, and
// reports every block that lacks one instead of rendering it as finished
// evidence. What a reader sees is exactly what somebody composed, and the
// context needed to read it travels with it.

import { useEffect, useState } from "react";
import Link from "next/link";
import { Download, Printer } from "lucide-react";
import StatusPill from "./StatusPill";
import EvidenceEnvelope from "./EvidenceEnvelope";
import { BUILDER_DRAFT_KEY } from "../lib/savedCharts";
import {
  isAnalyticalBlock,
  packetExport,
  packetIsComplete,
  packetIssues,
  readComposedPacket,
} from "../lib/evidencePackets";
import type { ComposedPacketRead } from "../lib/evidencePackets";

const LOADING: ComposedPacketRead = {
  state: "empty",
  packet: null,
  reason: "reading the composition stored in this browser",
  unsupported: [],
};

export default function ComposedArticle() {
  const [read, setRead] = useState<ComposedPacketRead>(LOADING);
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    let raw: string | null = null;
    try {
      raw = window.localStorage.getItem(BUILDER_DRAFT_KEY);
    } catch {
      // Storage throws outright in a private window rather than answering
      // null. That is an unreadable composition, not an absent one.
      setRead({
        state: "unreadable",
        packet: null,
        reason: "this browser does not allow reading stored compositions, so none can be shown",
        unsupported: [],
      });
      setLoaded(true);
      return;
    }
    setRead(readComposedPacket(raw));
    setLoaded(true);
  }, []);

  const packet = read.packet;
  const issues = packet ? packetIssues(packet) : [];
  const complete = packetIsComplete(packet);
  const issueByBlock = new Map(issues.map((issue) => [issue.blockId, issue]));

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

  if (!loaded) {
    return (
      <main className="article-shell" data-testid="composed-article" data-state="loading">
        <h1>Article</h1>
        <StatusPill state="loading" label="Article" message={read.reason} testId="article-status" />
      </main>
    );
  }

  if (!packet || packet.blocks.length === 0) {
    // Three distinct absences, and the reader is told which one this is: a
    // composition that was never made, one this build cannot parse, and one
    // that exists but carries no blocks. Only the first means "start here".
    const state = read.state === "unreadable" ? "bad" : "idle";
    const reason =
      packet && packet.blocks.length === 0
        ? "the stored composition carries no blocks yet"
        : read.reason;
    return (
      <main className="article-shell" data-testid="composed-article" data-state={read.state}>
        <header className="article-header">
          <div className="section-kicker">Articles</div>
          <h1>Composed evidence</h1>
          <p className="article-deck">
            An article here is a composed evidence packet: narrative beside analytical blocks that
            each carry the measure, source, geography, period, publication, and request needed to
            read them. Nothing on this page is written by hand and nothing on it is computed here.
          </p>
        </header>
        <StatusPill state={state} label="Article" message={reason} testId="article-status" />
        <div className="article-copy">
          <p data-testid="article-empty">{reason}.</p>
          <p>
            <Link className="button primary" href="/builder">
              Compose one in the builder
            </Link>
          </p>
        </div>
      </main>
    );
  }

  return (
    <main
      className="article-shell"
      data-testid="composed-article"
      data-state="ready"
      data-block-count={packet.blocks.length}
      data-issue-count={issues.length}
      data-complete={complete ? "true" : "false"}
    >
      <article>
        <header className="article-header">
          <div className="section-kicker">Composed evidence</div>
          <h1>{packet.title || "Untitled composition"}</h1>
          {packet.purpose ? <p className="article-deck">{packet.purpose}</p> : null}
          <div className="article-byline">
            <span>Economic Data Studio</span>
            <span data-testid="article-updated">
              {packet.updatedAt ? `Composed ${packet.updatedAt}` : "Composition time not recorded"}
            </span>
          </div>
        </header>

        <section className="status-row no-print">
          <StatusPill
            state={complete ? "ok" : "warn"}
            label="Article"
            message={
              complete
                ? `${packet.blocks.length} blocks, every analytical block has its envelope`
                : `${issues.length} analytical block${issues.length === 1 ? "" : "s"} missing context`
            }
            testId="article-status"
          />
          <button
            className="button secondary"
            type="button"
            onClick={exportCsv}
            data-testid="article-export"
          >
            <Download size={15} /> Export evidence
          </button>
          <button
            className="button secondary"
            type="button"
            onClick={() => window.print()}
            data-testid="article-print"
          >
            <Printer size={15} /> Print
          </button>
        </section>

        {read.unsupported.length > 0 ? (
          <section className="coverage-note partial" data-testid="article-unsupported">
            <strong>This composition carries blocks this build cannot present:</strong>
            <ul>
              {read.unsupported.map((title) => (
                <li key={title}>{title}</li>
              ))}
            </ul>
          </section>
        ) : null}

        {issues.length > 0 ? (
          <section className="coverage-note partial" data-testid="article-issues">
            <strong>These blocks are not presented as evidence:</strong>
            <ul>
              {issues.map((issue) => (
                <li key={issue.blockId} data-testid={`article-issue-${issue.blockId}`}>
                  <strong>{issue.title}</strong>: {issue.reason} (missing {issue.missing.join(", ")})
                </li>
              ))}
            </ul>
          </section>
        ) : null}

        {packet.blocks.map((block) => {
          const issue = issueByBlock.get(block.id);
          return (
            <section
              className={
                isAnalyticalBlock(block) ? "embedded-analysis" : "article-copy"
              }
              key={block.id}
              data-testid={`article-block-${block.id}`}
              data-block-type={block.type}
              data-has-envelope={block.envelope ? "true" : "false"}
            >
              <div className="panel-heading">
                <div>
                  <div className="section-kicker">{block.type}</div>
                  <h2>{block.title}</h2>
                </div>
              </div>
              {block.content ? <p>{block.content}</p> : null}
              {isAnalyticalBlock(block) ? (
                <>
                  {/* Whatever the block did record is still shown, with each
                      gap reading "Not recorded" — a reader deciding whether
                      to trust it needs to see how far short it falls, not
                      just that it falls short. */}
                  {block.envelope ? <EvidenceEnvelope block={block} /> : null}
                  {issue ? (
                    <p className="coverage-note partial" data-testid={`article-empty-${block.id}`}>
                      {issue.reason} (missing {issue.missing.join(", ")}). It is shown as composed
                      and is not presented as evidence.
                    </p>
                  ) : null}
                </>
              ) : null}
            </section>
          );
        })}
      </article>
    </main>
  );
}
