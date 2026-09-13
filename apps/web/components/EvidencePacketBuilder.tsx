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
//
// Where a packet is saved is one decision, made in one place
// (`saveDestination`), and it is stated on the control before the click and
// on the outcome after it: the account whenever a token is held, this browser
// otherwise. A refused account save is reported, never quietly redirected to
// the browser store -- telling someone their work is safe somewhere they did
// not choose and cannot see from their account is worse than telling them it
// was not saved.

import { useCallback, useEffect, useMemo, useState } from "react";
import { Download, FileText, FolderOpen, Plus, Printer, Save, Trash2 } from "lucide-react";
import StatusPill from "./StatusPill";
import EvidenceEnvelope from "./EvidenceEnvelope";
import { BUILDER_DRAFT_KEY, readSavedCharts } from "../lib/savedCharts";
import {
  ApiError,
  createEvidencePacket,
  getEvidencePacket,
  fetchCollectionPages,
  updateEvidencePacket,
} from "../lib/api/client";
import type { EvidencePacketSummary, PacketValidation } from "../lib/api/types";
import { useStoredToken } from "../lib/apiToken";
import {
  LIBRARY_PAGE_LIMIT,
  LIBRARY_PAGE_SIZE,
  describeLibraryLoad,
  describeSaveFailure,
  describeSaveSuccess,
  saveDestination,
} from "../lib/savedAnalysis";
import type { SaveOutcome } from "../lib/savedAnalysis";
import {
  documentToPacket,
  envelopeFromSavedChart,
  grantNeedsTemplate,
  isAnalyticalBlock,
  mergeBlockStates,
  packetExport,
  packetIsComplete,
  packetIssues,
  packetToDocument,
} from "../lib/evidencePackets";
import type { EvidencePacket, PacketBlock } from "../lib/evidencePackets";

function newId(prefix: string): string {
  return `${prefix}:${Math.random().toString(36).slice(2, 10)}`;
}

/** The account record the composer is editing, when it opened one. */
interface AccountPacket {
  packetId: number;
  version: number;
  name: string;
}

export default function EvidencePacketBuilder() {
  const { token, resolved: tokenResolved } = useStoredToken();
  const [packet, setPacket] = useState<EvidencePacket>(() => grantNeedsTemplate());
  const [savedCharts, setSavedCharts] = useState<Record<string, unknown>[]>([]);
  const [targetBlockId, setTargetBlockId] = useState("");
  const [saveState, setSaveState] = useState("Draft stored in this browser");
  const [saveOutcome, setSaveOutcome] = useState<SaveOutcome | null>(null);
  const [saving, setSaving] = useState(false);
  const [preview, setPreview] = useState(false);
  const [account, setAccount] = useState<AccountPacket | null>(null);
  const [accountPackets, setAccountPackets] = useState<EvidencePacketSummary[]>([]);
  const [accountStatus, setAccountStatus] = useState({ state: "idle", message: "not signed in" });
  const [apiValidation, setApiValidation] = useState<PacketValidation | null>(null);

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

  const destination = saveDestination(token);

  const refreshAccount = useCallback(async (activeToken: string) => {
    if (!activeToken) {
      setAccountPackets([]);
      setAccountStatus({ state: "idle", message: "not signed in" });
      return;
    }
    setAccountStatus({ state: "loading", message: "loading your packets" });
    try {
      const pages = await fetchCollectionPages<EvidencePacketSummary>(
        "/evidence-packets",
        {
          token: activeToken,
          pageSize: LIBRARY_PAGE_SIZE,
          maxPages: LIBRARY_PAGE_LIMIT,
        },
      );
      setAccountPackets(pages.items);
      setAccountStatus({
        state: pages.complete ? "ok" : "bad",
        message: describeLibraryLoad(
          pages.items.length,
          pages.total,
          pages.complete,
          "packet in your account",
          "packets in your account",
        ),
      });
    } catch (error) {
      setAccountPackets([]);
      // A 401 is identical for a missing, malformed, unknown, or revoked
      // token by design; every other refusal keeps its classification.
      setAccountStatus({
        state:
          error instanceof ApiError ? (error.status === 401 ? "unauthorized" : error.kind) : "bad",
        message:
          error instanceof ApiError && error.status === 401
            ? "the token was not accepted"
            : (error as { message?: string })?.message || "could not load your packets",
      });
    }
  }, []);

  useEffect(() => {
    if (tokenResolved) {
      refreshAccount(token);
    }
  }, [refreshAccount, token, tokenResolved]);

  const issues = useMemo(() => packetIssues(packet), [packet]);
  const complete = useMemo(() => packetIsComplete(packet), [packet]);
  const issueByBlock = useMemo(
    () => new Map(issues.map((issue) => [issue.blockId, issue])),
    [issues],
  );
  // Only the API can see staleness; a block it reports and the client does
  // not is a measure retired since the block was composed.
  const staleBlocks = useMemo(
    () => mergeBlockStates(packet, apiValidation).filter((state) => state.state === "stale"),
    [packet, apiValidation],
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
  // than the composer inventing a value to fill it. The query's scope and
  // release are the envelope's, so the two can never disagree -- the API
  // refuses a block whose envelope records a publication its query does not
  // ask for.
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
          scope: envelope.scope,
          release: envelope.release || null,
          filters: { geo_level: String(chart.geoLevel || "") },
        }
      : {
          kind: "observations" as const,
          metric_code: String(chart.metricCode || ""),
          scope: envelope.scope,
          release: envelope.release || null,
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

  function persistLocally() {
    window.localStorage.setItem(BUILDER_DRAFT_KEY, JSON.stringify(packet));
    setSaveState(`Saved ${new Date().toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })}`);
  }

  async function persist() {
    if (destination !== "account") {
      persistLocally();
      setSaveOutcome(describeSaveSuccess("browser", packet.title || "packet"));
      window.setTimeout(() => setSaveOutcome(null), 4000);
      return;
    }
    // Signed in, the packet is stored as the composer produced it: each
    // analytical block's query and recorded envelope, never an observation
    // value. The API refuses a contradiction and reports incompleteness;
    // both come back here as the API said them.
    const name = packet.title.trim() || "Untitled packet";
    setSaving(true);
    setSaveOutcome({ state: "loading", message: "Saving to your account", destination: null });
    try {
      const record = account
        ? await updateEvidencePacket(token, account.packetId, {
            name,
            document: packetToDocument(packet),
            expected_version: account.version,
          })
        : await createEvidencePacket(token, { name, document: packetToDocument(packet) });
      setAccount({ packetId: record.packet_id, version: record.version, name: record.name });
      setApiValidation(record.validation);
      setSaveOutcome(describeSaveSuccess("account", record.name));
      setSaveState(`In your account as “${record.name}”, version ${record.version}`);
      refreshAccount(token);
    } catch (error) {
      // A 409 is either a name clash or a version the caller never read. The
      // API's own detail names which; it is surfaced, never merged over.
      if (error instanceof ApiError && error.status === 409 && account) {
        setSaveOutcome({
          state: "conflict",
          message: `not saved: ${error.message}. Reopen the packet to see the current version.`,
          destination: null,
        });
      } else {
        setSaveOutcome(describeSaveFailure(error));
      }
    } finally {
      setSaving(false);
    }
    window.setTimeout(() => setSaveOutcome(null), 6000);
  }

  async function openAccountPacket(packetId: number) {
    if (!token) return;
    setAccountStatus({ state: "loading", message: "opening packet" });
    try {
      const record = await getEvidencePacket(token, packetId);
      setPacket(documentToPacket(record.document, record.updated_at));
      setAccount({ packetId: record.packet_id, version: record.version, name: record.name });
      setApiValidation(record.validation);
      setAccountStatus({
        state: "ok",
        message: `opened “${record.name}”, version ${record.version}`,
      });
    } catch (error) {
      setAccountStatus({
        state:
          error instanceof ApiError ? (error.status === 401 ? "unauthorized" : error.kind) : "bad",
        message: (error as { message?: string })?.message || "could not open the packet",
      });
    }
  }

  function startNewPacket() {
    setPacket(grantNeedsTemplate());
    setAccount(null);
    setApiValidation(null);
    setSaveState("New packet, not yet saved");
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
      data-account-packet={account ? String(account.packetId) : ""}
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

      <section className="status-row no-print" role="status">
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
        <StatusPill
          state={tokenResolved ? accountStatus.state : "loading"}
          label="Account"
          message={tokenResolved ? accountStatus.message : "checking"}
          testId="packet-account-status"
        />
        {saveOutcome ? (
          <StatusPill
            state={saveOutcome.state}
            label="Save"
            message={saveOutcome.message}
            testId="packet-save-toast"
          />
        ) : null}
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
        <button
          className="button secondary"
          type="button"
          onClick={persist}
          disabled={saving || !tokenResolved}
          data-testid="packet-save"
          data-destination={destination}
          title={
            destination === "account"
              ? account
                ? `Updates “${account.name}” in your account`
                : "Saves to your account"
              : "Saves in this browser only; sign in on Saved analyses to keep it"
          }
        >
          <Save size={15} />{" "}
          {destination === "account"
            ? account
              ? "Save to account (update)"
              : "Save to account"
            : "Save draft in browser"}
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
        <span className="subtle" data-testid="packet-save-state">{saveState}</span>
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

      {staleBlocks.length > 0 ? (
        <section className="coverage-note partial no-print" data-testid="packet-stale">
          <strong>The API reports these blocks can no longer be replayed as composed:</strong>
          <ul>
            {staleBlocks.map((state) => (
              <li key={state.blockId} data-testid={`stale-${state.blockId}`}>
                <strong>{state.title}</strong>: {state.reason}
              </li>
            ))}
          </ul>
          <p className="subtle">
            Shown exactly as stored — the API reports the mismatch rather than rewriting your
            packet, so you decide what to change.
          </p>
        </section>
      ) : null}

      <section className="builder-shell">
        {!preview ? (
          <aside className="builder-library no-print" data-testid="packet-library">
            {token ? (
              <div data-testid="packet-account-library">
                <div className="library-heading">Your packets</div>
                <p className="subtle">
                  Stored in your account. Opening one replaces the composer&apos;s contents;
                  nothing about a packet is written to the address bar.
                </p>
                <button
                  className="button secondary"
                  type="button"
                  onClick={startNewPacket}
                  data-testid="packet-new"
                >
                  <Plus size={15} /> New packet
                </button>
                {accountPackets.length === 0 ? (
                  <div className="empty-state compact" data-testid="packet-account-empty">
                    No packets in your account yet.
                  </div>
                ) : (
                  accountPackets.map((row) => (
                    <button
                      className="library-button"
                      type="button"
                      key={row.packet_id}
                      onClick={() => openAccountPacket(row.packet_id)}
                      data-testid={`packet-open-${row.packet_id}`}
                    >
                      <span>
                        <strong>{row.name}</strong>
                        <small>
                          v{row.version} · {row.block_count} blocks, {row.analytical_block_count}{" "}
                          analytical
                        </small>
                      </span>
                      <FolderOpen size={15} />
                    </button>
                  ))
                )}
              </div>
            ) : null}

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
                      <EvidenceEnvelope block={block} />
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
