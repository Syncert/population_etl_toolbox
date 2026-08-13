"use client";

import { useEffect, useMemo, useState } from "react";
import { BarChart3, FileText, Plus, Save, Trash2 } from "lucide-react";
import { BUILDER_DRAFT_KEY, readSavedCharts } from "../../lib/savedCharts";

const starterBlocks = [{ id: "intro", type: "text", content: "Explain what changed, where it changed, and why it matters." }];

export default function BuilderPage() {
  const [title, setTitle] = useState("Untitled analysis");
  const [blocks, setBlocks] = useState(starterBlocks);
  const [savedCharts, setSavedCharts] = useState([]);
  const [saveState, setSaveState] = useState("Draft stored in this browser");

  useEffect(() => {
    setSavedCharts(readSavedCharts());
    try {
      const draft = JSON.parse(window.localStorage.getItem(BUILDER_DRAFT_KEY) || "null");
      if (draft) { setTitle(draft.title || "Untitled analysis"); setBlocks(Array.isArray(draft.blocks) ? draft.blocks : starterBlocks); }
    } catch { /* Keep starter draft. */ }
  }, []);

  const draft = useMemo(() => ({ version: 1, title, blocks, updatedAt: new Date().toISOString() }), [title, blocks]);
  function persist() { window.localStorage.setItem(BUILDER_DRAFT_KEY, JSON.stringify(draft)); setSaveState(`Saved ${new Date().toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })}`); }
  function addText() { setBlocks((items) => [...items, { id: crypto.randomUUID(), type: "text", content: "Add analytical context..." }]); }
  function addChart(chart) { setBlocks((items) => [...items, { id: crypto.randomUUID(), type: "chart", chart }]); }
  function removeBlock(id) { setBlocks((items) => items.filter((item) => item.id !== id)); }
  function updateText(id, content) { setBlocks((items) => items.map((item) => item.id === id ? { ...item, content } : item)); }

  return (
    <main className="builder-shell">
      <aside className="builder-library"><div className="section-kicker">Block library</div><h1>Page Builder</h1><p>Compose a source-visible analytical page from reusable blocks.</p><button className="library-button" type="button" onClick={addText}><FileText size={17} /><span><strong>Text block</strong><small>Narrative and interpretation</small></span><Plus size={15} /></button><div className="library-heading">Saved charts</div>{savedCharts.length === 0 ? <div className="empty-state compact">Save a view in the Explorer to make it available here.</div> : savedCharts.map((chart) => <button className="library-button" type="button" onClick={() => addChart(chart)} key={chart.id}><BarChart3 size={17} /><span><strong>{chart.title}</strong><small>{chart.metricCode}</small></span><Plus size={15} /></button>)}</aside>
      <section className="builder-workspace"><header className="builder-toolbar"><div><span>Draft page</span><small>{saveState}</small></div><button className="button primary" type="button" onClick={persist}><Save size={15} /> Save draft</button></header><div className="builder-canvas"><input className="builder-title" aria-label="Page title" value={title} onChange={(event) => setTitle(event.target.value)} />{blocks.map((block) => <article className="builder-block" key={block.id}><button className="icon-button block-delete" type="button" title="Remove block" aria-label="Remove block" onClick={() => removeBlock(block.id)}><Trash2 size={16} /></button>{block.type === "text" ? <textarea aria-label="Narrative text" value={block.content} onChange={(event) => updateText(block.id, event.target.value)} /> : <div className="chart-block-preview"><div className="section-kicker">Saved analytical view</div><h2>{block.chart.title}</h2><p>{block.chart.metricName}</p><dl><div><dt>Metric</dt><dd>{block.chart.metricCode}</dd></div><div><dt>Geography</dt><dd>{block.chart.geoLevel}</dd></div><div><dt>Source</dt><dd>{block.chart.source}</dd></div></dl></div>}</article>)}<button className="add-block-button" type="button" onClick={addText}><Plus size={16} /> Add narrative block</button></div></section>
    </main>
  );
}
