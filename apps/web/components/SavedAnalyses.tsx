"use client";

// The saved-analysis screen: the user's own configurations, over the
// authenticated API contract.
//
// Everything on this screen is private content. The bearer token is held in
// component state and, at the user's choice, `sessionStorage` — never in a
// URL, never in `localStorage` alongside public data, and never logged. The
// screen writes nothing about a configuration into the address bar, so
// history, referrers, and shared links stay free of user content.

import { useCallback, useEffect, useRef, useState } from "react";
import Link from "next/link";
import { RefreshCw, Trash2, Upload } from "lucide-react";
import StatusPill from "./StatusPill";
import {
  ApiError,
  apiErrorMessage,
  createSavedAnalysis,
  deleteSavedAnalysis,
  getSavedAnalysis,
  listSavedAnalyses,
  updateSavedAnalysis,
} from "../lib/api/client";
import { createRequestTracker } from "../lib/api/requestState";
import type { SavedAnalysisConfiguration, SavedAnalysisSummary } from "../lib/api/types";
import { readSavedCharts } from "../lib/savedCharts";
import {
  describeConflict,
  describeDocument,
  planLocalMigration,
  reopenHref,
  sortConfigurations,
  validationState,
} from "../lib/savedAnalysis";
import type { MigrationPlan } from "../lib/savedAnalysis";

const TOKEN_SESSION_KEY = "economic-data-studio:api-token";

interface RequestStatus {
  state: string;
  message: string;
}

export default function SavedAnalyses() {
  const listTracker = useRef(createRequestTracker()).current;
  const detailTracker = useRef(createRequestTracker()).current;

  const [token, setToken] = useState("");
  const [tokenDraft, setTokenDraft] = useState("");
  const [remember, setRemember] = useState(false);
  const [items, setItems] = useState<SavedAnalysisSummary[]>([]);
  const [listStatus, setListStatus] = useState<RequestStatus>({
    state: "idle",
    message: "not signed in",
  });
  const [selected, setSelected] = useState<SavedAnalysisConfiguration | null>(null);
  const [detailStatus, setDetailStatus] = useState<RequestStatus>({
    state: "idle",
    message: "select a saved analysis",
  });
  const [renameDraft, setRenameDraft] = useState("");
  const [conflict, setConflict] = useState("");
  const [migration, setMigration] = useState<MigrationPlan | null>(null);
  const [migrationStatus, setMigrationStatus] = useState("");

  // A token the user asked this browser to remember lives in sessionStorage,
  // which is cleared when the tab closes and is never shared across origins.
  useEffect(() => {
    try {
      const stored = window.sessionStorage.getItem(TOKEN_SESSION_KEY);
      if (stored) {
        setToken(stored);
        setRemember(true);
      }
    } catch {
      // Storage unavailable: the user signs in for this page view only.
    }
  }, []);

  const refresh = useCallback(
    async (activeToken: string) => {
      if (!activeToken) {
        setItems([]);
        setListStatus({ state: "idle", message: "not signed in" });
        return;
      }
      const request = listTracker.begin();
      setListStatus({ state: "loading", message: "loading your analyses" });
      try {
        const payload = await listSavedAnalyses(activeToken, { limit: "200" });
        if (!request.isCurrent()) {
          return;
        }
        const rows = sortConfigurations(payload.items);
        setItems(rows);
        setListStatus({
          state: "ok",
          message: `${rows.length} of ${payload.total ?? rows.length} saved analyses`,
        });
      } catch (error) {
        if (!request.isCurrent()) {
          return;
        }
        setItems([]);
        // A 401 is identical for a missing, malformed, unknown, or revoked
        // token by design; the screen says so rather than guessing which.
        setListStatus({
          state: error instanceof ApiError && error.status === 401 ? "unauthorized" : "bad",
          message:
            error instanceof ApiError && error.status === 401
              ? "the token was not accepted"
              : apiErrorMessage(error),
        });
      }
    },
    [listTracker],
  );

  useEffect(() => {
    refresh(token);
  }, [refresh, token]);

  async function openConfiguration(configurationId: number) {
    if (!token) {
      return;
    }
    const request = detailTracker.begin();
    setConflict("");
    setDetailStatus({ state: "loading", message: "loading configuration" });
    try {
      const payload = await getSavedAnalysis(token, configurationId);
      if (!request.isCurrent()) {
        return;
      }
      setSelected(payload);
      setRenameDraft(payload.name);
      setDetailStatus({ state: "ok", message: `version ${payload.version}` });
    } catch (error) {
      if (request.isCurrent()) {
        setSelected(null);
        // Another owner's id and one that never existed answer identically.
        setDetailStatus({ state: "bad", message: apiErrorMessage(error) });
      }
    }
  }

  async function saveRename() {
    if (!token || !selected) {
      return;
    }
    setConflict("");
    setDetailStatus({ state: "loading", message: "saving" });
    try {
      const payload = await updateSavedAnalysis(token, selected.configuration_id, {
        name: renameDraft,
        // The document is sent back unchanged: this screen renames, it does
        // not rewrite a user's analysis intent.
        document: selected.document,
        expected_version: selected.version,
      });
      setSelected(payload);
      setRenameDraft(payload.name);
      setDetailStatus({ state: "ok", message: `version ${payload.version}` });
      await refresh(token);
    } catch (error) {
      const status = error instanceof ApiError ? error.status : null;
      const detail = error instanceof ApiError ? error.detail : null;
      const state = describeConflict(status, detail, selected.version);
      if (state.conflicted) {
        // Refused, not merged: overwriting a version this client never read
        // would discard a change made elsewhere.
        setConflict(state.message);
        setDetailStatus({ state: "conflict", message: "version conflict" });
        return;
      }
      setDetailStatus({ state: "bad", message: apiErrorMessage(error) });
    }
  }

  async function removeConfiguration(configurationId: number) {
    if (!token) {
      return;
    }
    setDetailStatus({ state: "loading", message: "deleting" });
    try {
      await deleteSavedAnalysis(token, configurationId);
      setSelected(null);
      setDetailStatus({ state: "idle", message: "deleted" });
      await refresh(token);
    } catch (error) {
      setDetailStatus({ state: "bad", message: apiErrorMessage(error) });
    }
  }

  async function duplicateConfiguration() {
    if (!token || !selected) {
      return;
    }
    setDetailStatus({ state: "loading", message: "duplicating" });
    try {
      const payload = await createSavedAnalysis(token, {
        name: `${selected.name} (copy)`,
        document: selected.document,
      });
      setSelected(payload);
      setRenameDraft(payload.name);
      setDetailStatus({ state: "ok", message: `version ${payload.version}` });
      await refresh(token);
    } catch (error) {
      setDetailStatus({ state: "bad", message: apiErrorMessage(error) });
    }
  }

  function planMigration() {
    setMigration(planLocalMigration(readSavedCharts()));
    setMigrationStatus("");
  }

  async function runMigration() {
    if (!token || !migration) {
      return;
    }
    let imported = 0;
    let failed = 0;
    for (const candidate of migration.candidates) {
      try {
        await createSavedAnalysis(token, {
          name: candidate.name,
          document: candidate.document,
        });
        imported += 1;
      } catch {
        // A document the API refuses stays local and is counted; the local
        // store is never cleared, so nothing is lost by importing.
        failed += 1;
      }
    }
    setMigrationStatus(
      `${imported} imported, ${failed} refused by the API, ${migration.skipped.length} not describable as a configuration. Your local copies were kept.`,
    );
    await refresh(token);
  }

  const validation = validationState(selected?.validation);

  return (
    <main
      className="page-shell"
      data-testid="saved-analyses"
      data-signed-in={token ? "true" : "false"}
      data-count={items.length}
      data-selected={selected?.configuration_id ?? ""}
    >
      <header className="page-heading">
        <div className="section-kicker">Your account</div>
        <h1>Saved analyses</h1>
        <p>
          Saved analyses are stored by the API against your account. They record what to ask
          for, not the answers, so reopening one replays it against the current publication
          rather than showing a frozen copy.
        </p>
      </header>

      <section className="profile-controls">
        <label>
          API token
          <input
            type="password"
            value={tokenDraft}
            onChange={(event) => setTokenDraft(event.target.value)}
            placeholder="operator-provisioned bearer token"
            data-testid="token-input"
            autoComplete="off"
          />
        </label>
        <label>
          <input
            type="checkbox"
            checked={remember}
            onChange={(event) => setRemember(event.target.checked)}
            data-testid="token-remember"
          />{" "}
          Keep for this tab only
        </label>
        <button
          className="button primary"
          type="button"
          data-testid="token-submit"
          onClick={() => {
            setToken(tokenDraft);
            try {
              if (remember && tokenDraft) {
                window.sessionStorage.setItem(TOKEN_SESSION_KEY, tokenDraft);
              } else {
                window.sessionStorage.removeItem(TOKEN_SESSION_KEY);
              }
            } catch {
              // Storage unavailable; the token stays in memory for this view.
            }
          }}
        >
          Sign in
        </button>
        <button
          className="button secondary"
          type="button"
          data-testid="token-clear"
          onClick={() => {
            setToken("");
            setTokenDraft("");
            setItems([]);
            setSelected(null);
            try {
              window.sessionStorage.removeItem(TOKEN_SESSION_KEY);
            } catch {
              // Nothing to clear.
            }
          }}
        >
          Sign out
        </button>
        <button className="button secondary" type="button" onClick={() => refresh(token)}>
          <RefreshCw size={15} /> Refresh
        </button>
      </section>
      <p className="subtle" data-testid="privacy-note">
        Your token is sent only as an authorization header and is never placed in a link, a
        shared URL, or the address bar. Saved analyses are served <code>private, no-store</code>{" "}
        and are never publicly cached. Nothing on this page is written to the address bar.
      </p>

      <section className="status-row">
        <StatusPill
          state={listStatus.state}
          label="Account"
          message={listStatus.message}
          testId="saved-list-status"
        />
        <StatusPill
          state={detailStatus.state}
          label="Configuration"
          message={detailStatus.message}
          testId="saved-detail-status"
        />
      </section>

      <section className="analysis-panel">
        <div className="panel-heading">
          <div>
            <div className="section-kicker">Your analyses</div>
            <h2>Stored configurations</h2>
          </div>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Kind</th>
                <th>Version</th>
                <th>Updated</th>
                <th>Open</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item) => (
                <tr key={item.configuration_id} data-testid={`saved-row-${item.configuration_id}`}>
                  <td>{item.name}</td>
                  <td>{String(item.kind ?? "-")}</td>
                  <td>{item.version}</td>
                  <td>{item.updated_at}</td>
                  <td>
                    <button
                      className="button secondary"
                      type="button"
                      onClick={() => openConfiguration(item.configuration_id)}
                      data-testid={`saved-open-${item.configuration_id}`}
                    >
                      Open
                    </button>
                  </td>
                </tr>
              ))}
              {items.length === 0 ? (
                <tr>
                  <td colSpan={5} className="subtle">
                    {token
                      ? "No saved analyses for this account."
                      : "Sign in with your API token to see your saved analyses."}
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>

      {selected ? (
        <section className="analysis-panel" data-testid="saved-detail">
          <div className="panel-heading">
            <div>
              <div className="section-kicker">Configuration {selected.configuration_id}</div>
              <h2>{selected.name}</h2>
              <p className="subtle" data-testid="saved-document">
                {describeDocument(selected.document)}
              </p>
            </div>
            <StatusPill
              state={validation.state}
              label="Validity"
              message={validation.message}
              testId="saved-validation"
            />
          </div>

          {!selected.validation?.valid ? (
            <p className="coverage-note partial" data-testid="stale-note">
              This configuration no longer matches live capabilities. It is shown exactly as you
              saved it — the API reports the mismatch rather than rewriting your content, so
              you decide what to change.
            </p>
          ) : null}

          {conflict ? (
            <p className="coverage-note partial" data-testid="conflict-note">
              {conflict}
            </p>
          ) : null}

          <div className="profile-controls">
            <label>
              Name
              <input
                value={renameDraft}
                onChange={(event) => setRenameDraft(event.target.value)}
                data-testid="rename-input"
              />
            </label>
            <button
              className="button primary"
              type="button"
              onClick={saveRename}
              data-testid="rename-save"
            >
              Save name
            </button>
            <button
              className="button secondary"
              type="button"
              onClick={duplicateConfiguration}
              data-testid="duplicate"
            >
              Duplicate
            </button>
            <button
              className="button secondary"
              type="button"
              onClick={() => removeConfiguration(selected.configuration_id)}
              data-testid="delete"
            >
              <Trash2 size={15} /> Delete permanently
            </button>
            <Link
              className="button secondary"
              href={reopenHref(selected.document)}
              data-testid="reopen"
            >
              Reopen in the explorer
            </Link>
          </div>
          <p className="subtle">
            Deletion is immediate and permanent. Reopening replays this configuration against
            the current publication.
          </p>
        </section>
      ) : null}

      <section className="analysis-panel" data-testid="migration-panel">
        <div className="panel-heading">
          <div>
            <div className="section-kicker">Browser-local charts</div>
            <h2>Import saved views from this browser</h2>
            <p className="subtle">
              Views saved in this browser before accounts existed can be imported. Your local
              copies are kept either way.
            </p>
          </div>
        </div>
        <div className="profile-controls">
          <button
            className="button secondary"
            type="button"
            onClick={planMigration}
            data-testid="migration-plan"
          >
            <Upload size={15} /> Check this browser
          </button>
          {migration && migration.candidates.length > 0 ? (
            <button
              className="button primary"
              type="button"
              onClick={runMigration}
              disabled={!token}
              data-testid="migration-run"
            >
              Import {migration.candidates.length}
            </button>
          ) : null}
        </div>
        {migration ? (
          <div data-testid="migration-summary">
            <p className="subtle">
              {migration.candidates.length} can be imported as configurations;{" "}
              {migration.skipped.length} cannot.
            </p>
            {migration.skipped.length > 0 ? (
              <ul data-testid="migration-skipped">
                {migration.skipped.map((entry) => (
                  <li key={entry.localId}>
                    <strong>{entry.name}</strong>: {entry.reason}
                  </li>
                ))}
              </ul>
            ) : null}
            {migrationStatus ? (
              <p className="coverage-note partial" data-testid="migration-result">
                {migrationStatus}
              </p>
            ) : null}
          </div>
        ) : null}
      </section>
    </main>
  );
}
