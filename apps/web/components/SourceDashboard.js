"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import {
  Bell,
  ChartNoAxesCombined,
  ChevronDown,
  CircleHelp,
  Columns3,
  Database,
  Download,
  FileChartColumn,
  Gauge,
  LayoutDashboard,
  Map,
  Menu,
  Moon,
  MoreVertical,
  RefreshCw,
  Search,
  Settings,
  Share2,
  SlidersHorizontal,
  Sparkles,
  Sun,
} from "lucide-react";
import { getSourceLatestObservations, searchMetrics } from "../lib/api/client";

const SOURCE_META = {
  bls: {
    sourceCode: "BLS",
    title: "BLS Labor Market Monitor",
    agency: "U.S. Bureau of Labor Statistics",
    preferredMetric: "BLS:LAU:UNEMP_RATE",
    catalogQuery: "unemployment rate",
    geoLevel: "STATE",
    segment: "bls",
    theme: "dark",
    asOf: "May 2025",
  },
  census: {
    sourceCode: "CENSUS_ACS",
    title: "Census Population & Demographics",
    agency: "U.S. Census Bureau",
    preferredMetric: "ACS:acs5:B01003_001",
    catalogQuery: "total population",
    geoLevel: "COUNTY",
    segment: "census",
    theme: "light",
    asOf: "2023",
  },
  fred: {
    sourceCode: "FRED",
    title: "FRED Economic Indicators",
    agency: "Federal Reserve Economic Data",
    preferredMetric: "FRED:CPIAUCSL",
    catalogQuery: "consumer price index",
    geoLevel: "NATIONAL",
    segment: "fred",
    theme: "light",
    asOf: "May 30, 2025",
  },
};

const NAV_ITEMS = {
  bls: [
    [LayoutDashboard, "Overview"],
    [Map, "Map"],
    [ChartNoAxesCombined, "Charts"],
    [Columns3, "Compare"],
    [Search, "Data Explorer"],
    [FileChartColumn, "Saved Views"],
  ],
  census: [
    [LayoutDashboard, "Overview"],
    [Map, "Map"],
    [ChartNoAxesCombined, "Charts"],
    [Columns3, "Compare"],
    [Search, "Data Explorer"],
    [FileChartColumn, "Saved Views"],
    [FileChartColumn, "Reports"],
  ],
  fred: [
    [LayoutDashboard, "Overview"],
    [Search, "Explore"],
    [ChartNoAxesCombined, "Charts"],
    [Columns3, "Compare"],
    [Menu, "Series List"],
    [FileChartColumn, "Saved Views"],
  ],
};

const BLS_LINE = [6.2, 6.1, 7.4, 7.1, 8.3, 13.9, 13.6, 11.5, 10.2, 9.1, 8, 7, 6.4, 5.8, 5.4, 5.1, 4.9, 16.1, 7.2, 5.8, 5.2, 5.1, 5.3, 5.8, 6.1];
const CENSUS_LINE = [19.7, 22.4, 25.1, 27.8, 30.5, 32.7, 35.1];
const FRED_LINE = [72, 76, 81, 89, 102, 132, 159, 191, 223, 258, 315, 308, 336, 331, 347, 362];
const FRED_BARS = [0.29, 0.63, 0.38, 0.34, -0.04, 0.38, 0.68, 0.41, 0.18, 0.33, 0.59, 0.19, 0.12, 0.03, -0.04];

function formatValue(value, digits = 1) {
  const number = Number(value);
  if (!Number.isFinite(number)) return null;
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: digits }).format(number);
}

function dateLabel(item, fallback) {
  const value = item?.period || item?.observation_date || item?.as_of_date;
  if (!value) return fallback;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return new Intl.DateTimeFormat("en-US", { month: "short", year: "numeric" }).format(date);
}

function useSourceObservations(sourceKey) {
  const meta = SOURCE_META[sourceKey];
  const [state, setState] = useState({ status: "loading", items: [], metric: null, message: "Connecting" });
  const [reloadKey, setReloadKey] = useState(0);

  useEffect(() => {
    const controller = new AbortController();

    async function load() {
      setState((current) => ({ ...current, status: "loading", message: "Connecting" }));
      try {
        const catalog = await searchMetrics(
          {
            source_code: meta.sourceCode,
            q: meta.catalogQuery,
            active_only: "true",
            limit: "25",
          },
          { signal: controller.signal },
        );
        const metrics = Array.isArray(catalog.items) ? catalog.items : [];
        const metric =
          metrics.find((item) => item.metric_code === meta.preferredMetric) ||
          metrics.find((item) => String(item.metric_display_name || "").toLowerCase().includes(meta.catalogQuery.split(" ")[0])) ||
          metrics[0];
        const metricCode = metric?.metric_code || meta.preferredMetric;
        const payload = await getSourceLatestObservations(
          meta.segment,
          {
            metric_code: metricCode,
            geo_level: meta.geoLevel,
            limit: sourceKey === "fred" ? "50" : "500",
          },
          { signal: controller.signal },
        );
        const items = Array.isArray(payload.items) ? payload.items : [];
        if (items.length === 0) throw new Error("no observations returned");
        setState({ status: "live", items, metric, message: `${items.length.toLocaleString()} live records` });
      } catch (error) {
        if (error?.name !== "AbortError") {
          setState({ status: "preview", items: [], metric: null, message: "Preview data" });
        }
      }
    }

    load();
    return () => controller.abort();
  }, [meta, reloadKey, sourceKey]);

  return { ...state, reload: () => setReloadKey((value) => value + 1) };
}

function Sidebar({ sourceKey }) {
  const [open, setOpen] = useState(false);
  return (
    <aside className={`source-sidebar ${open ? "open" : ""}`}>
      <button className="mobile-menu-button" type="button" onClick={() => setOpen((value) => !value)} aria-label="Toggle navigation">
        <Menu size={18} />
      </button>
      <Link className="source-wordmark" href="/">
        <span className="source-wordmark-icon"><Sparkles size={17} /></span>
        <span>DataPulse</span>
      </Link>
      <nav className="source-nav" aria-label={`${sourceKey.toUpperCase()} dashboard`}>
        {NAV_ITEMS[sourceKey].map(([Icon, label], index) => (
          <button className={index === 0 ? "active" : ""} type="button" key={label}>
            <Icon size={15} />
            <span>{label}</span>
          </button>
        ))}
      </nav>
      <div className="source-sidebar-bottom">
        <button type="button"><Bell size={15} /><span>Alerts</span></button>
        <button type="button"><Settings size={15} /><span>Settings</span></button>
        <div className="theme-mode">{sourceKey === "census" ? <Sun size={15} /> : <Moon size={15} />}<span>{sourceKey === "census" ? "Light" : "Dark"}</span><ChevronDown size={14} /></div>
        <div className="dashboard-user"><span>N</span><strong>Nicholas</strong></div>
      </div>
    </aside>
  );
}

function DashboardHeader({ meta, sourceState }) {
  function exportCsv() {
    const rows = sourceState.items;
    const headings = ["source", "metric_code", "geo_id", "geo_name", "observation_date", "value", "units"];
    const escape = (value) => `"${String(value ?? "").replaceAll('"', '""')}"`;
    const content = [
      headings,
      ...rows.map((item) => headings.map((heading) => {
        if (heading === "source") return item.source || item.source_code;
        if (heading === "geo_name") return item.geo_name || item.county_name || item.state_name;
        return item[heading];
      })),
    ].map((row) => row.map(escape).join(",")).join("\n");
    const blob = new Blob([content], { type: "text/csv;charset=utf-8" });
    const href = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = href;
    link.download = `${meta.sourceCode.toLowerCase()}-dashboard.csv`;
    link.click();
    URL.revokeObjectURL(href);
  }

  return (
    <header className="source-dashboard-header">
      <div>
        <h1>{meta.title}</h1>
        <p>{meta.agency}</p>
      </div>
      <div className="dashboard-header-actions">
        <span className={`data-state ${sourceState.status}`} title={sourceState.message}>
          <i /> {sourceState.status === "live" ? "Live API" : sourceState.status === "loading" ? "Connecting" : "Preview"}
        </span>
        <small>Data as of {sourceState.items[0] ? dateLabel(sourceState.items[0], meta.asOf) : meta.asOf}</small>
        <button type="button" onClick={exportCsv} disabled={sourceState.items.length === 0}><Download size={14} /> Export</button>
        <button className="icon-only" type="button" onClick={sourceState.reload} aria-label="Refresh data"><RefreshCw size={14} /></button>
        <button className="icon-only" type="button" aria-label="More options"><MoreVertical size={15} /></button>
      </div>
    </header>
  );
}

function Segment({ items, selected = 0 }) {
  const [active, setActive] = useState(selected);
  return (
    <div className="segment">
      {items.map((item, index) => <button className={index === active ? "active" : ""} type="button" onClick={() => setActive(index)} key={item}>{item}</button>)}
    </div>
  );
}

function SelectControl({ label, value, options = [] }) {
  const [selected, setSelected] = useState(value);
  return (
    <label className="dash-control">
      <span>{label}</span>
      <select value={selected} onChange={(event) => setSelected(event.target.value)}>
        <option>{value}</option>
        {options.filter((option) => option !== value).map((option) => <option key={option}>{option}</option>)}
      </select>
    </label>
  );
}

function FilterBar({ sourceKey }) {
  if (sourceKey === "bls") {
    return (
      <section className="dashboard-filters bls-filters">
        <div className="dash-control"><span>Geography</span><Segment items={["National", "State", "County"]} /></div>
        <SelectControl label="Geography selector" value="United States" options={["California", "Texas", "Wisconsin"]} />
        <SelectControl label="Measure category" value="Employment & Unemployment" options={["Labor Force", "Earnings"]} />
        <SelectControl label="Measure" value="Unemployment Rate" options={["Payroll Employment", "Participation Rate"]} />
        <SelectControl label="Time range" value="Custom" options={["1 Year", "5 Years", "All"]} />
      </section>
    );
  }
  if (sourceKey === "census") {
    return (
      <section className="dashboard-filters">
        <div className="dash-control"><span>Geography</span><Segment items={["National", "State", "County"]} /></div>
        <SelectControl label="Geography selector" value="Texas" options={["United States", "California", "Florida"]} />
        <SelectControl label="Measure category" value="Population" options={["Age", "Race & Ethnicity", "Housing"]} />
        <SelectControl label="Measure" value="Total Population" options={["Population Density", "Median Age"]} />
        <SelectControl label="Time / Vintage" value="2023 (ACS 1-Year)" options={["2022 (ACS 1-Year)", "2023 (ACS 5-Year)"]} />
      </section>
    );
  }
  return (
    <section className="dashboard-filters fred-filters">
      <div className="dash-control"><span>Geography</span><Segment items={["National", "Region", "State"]} /></div>
      <SelectControl label="Indicator category" value="Prices" options={["Labor Market", "Interest Rates", "Growth"]} />
      <SelectControl label="Indicator" value="CPI – All Items (Consumer Price Index)" options={["Core CPI", "PCE Price Index"]} />
      <SelectControl label="Frequency" value="Monthly" options={["Quarterly", "Annual"]} />
      <SelectControl label="Time range" value="Custom" options={["1 Year", "10 Years", "All"]} />
      <button className="compare-button" type="button"><Share2 size={13} /> Add to Compare</button>
    </section>
  );
}

function Kpi({ label, value, detail, delta, positive = true }) {
  return (
    <div className="dashboard-kpi">
      <span>{label}</span>
      <strong>{value}</strong>
      {detail ? <small>{detail}</small> : null}
      {delta ? <em className={positive ? "positive" : ""}>{delta}</em> : null}
    </div>
  );
}

function Panel({ title, subtitle, className = "", children }) {
  return (
    <article className={`dashboard-panel ${className}`}>
      <div className="panel-heading">
        <div><h2>{title}</h2>{subtitle ? <p>{subtitle}</p> : null}</div>
      </div>
      {children}
    </article>
  );
}

function LineChart({ values, color = "#1764d9", labels = [], area = false, recessions = false }) {
  const width = 620;
  const height = 225;
  const pad = 24;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const points = values.map((value, index) => {
    const x = pad + (index / Math.max(values.length - 1, 1)) * (width - pad * 2);
    const y = height - pad - ((value - min) / span) * (height - pad * 2);
    return [x, y];
  });
  const path = points.map(([x, y], index) => `${index ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
  const areaPath = `${path} L${points.at(-1)[0]},${height - pad} L${points[0][0]},${height - pad} Z`;
  return (
    <svg className="dashboard-chart" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Time-series line chart">
      {[0, 1, 2, 3, 4].map((line) => <line className="dash-gridline" x1={pad} x2={width - pad} y1={pad + line * 44} y2={pad + line * 44} key={line} />)}
      {recessions ? [84, 165, 218, 280, 430, 492].map((x) => <rect className="recession" x={x} y={pad} width="13" height={height - pad * 2} key={x} />) : null}
      {area ? <path d={areaPath} fill={`${color}18`} /> : null}
      <path className="dash-line" d={path} stroke={color} />
      {points.filter((_, index) => values.length < 10 || index === values.length - 1).map(([x, y], index) => <circle cx={x} cy={y} r="3.3" fill={color} key={index} />)}
      {labels.map((label, index) => <text className="axis-label" x={pad + index * ((width - pad * 2) / Math.max(labels.length - 1, 1))} y={height - 5} textAnchor={index === 0 ? "start" : index === labels.length - 1 ? "end" : "middle"} key={label}>{label}</text>)}
    </svg>
  );
}

function BarChart({ values }) {
  const max = Math.max(...values.map(Math.abs));
  const zero = 142;
  return (
    <svg className="dashboard-chart bar-chart" viewBox="0 0 600 220" role="img" aria-label="Recent monthly change bar chart">
      {[40, 90, 142, 190].map((y) => <line className="dash-gridline" x1="28" x2="580" y1={y} y2={y} key={y} />)}
      {values.map((value, index) => {
        const height = Math.abs(value / max) * 78;
        const x = 48 + index * 34;
        return <rect x={x} y={value >= 0 ? zero - height : zero} width="18" height={height} fill="#4d76bd" key={index} />;
      })}
      <text className="axis-label" x="34" y="212">Jun ’24</text>
      <text className="axis-label" x="558" y="212" textAnchor="end">May ’25</text>
    </svg>
  );
}

function UsMap() {
  const cells = [
    [50, 58, 42, 42, 2], [93, 64, 42, 45, 2], [136, 69, 42, 46, 1], [179, 74, 44, 43, 1], [224, 77, 45, 42, 1], [270, 79, 46, 42, 1], [317, 82, 44, 42, 0],
    [65, 105, 43, 49, 2], [109, 111, 43, 45, 3], [153, 116, 44, 46, 2], [198, 119, 45, 45, 1], [244, 120, 46, 46, 1], [291, 124, 47, 47, 0], [339, 120, 44, 44, 0],
    [85, 153, 46, 48, 3], [132, 158, 44, 47, 2], [177, 163, 45, 48, 1], [223, 166, 45, 46, 1], [269, 169, 46, 44, 1], [316, 165, 45, 43, 0], [362, 151, 38, 42, 1],
    [150, 205, 55, 44, 3], [206, 210, 50, 45, 2], [257, 211, 49, 44, 1], [307, 208, 47, 42, 0], [355, 196, 45, 42, 1], [401, 177, 30, 34, 0],
  ];
  const palette = ["#a7c8fb", "#7da8ef", "#4e83dc", "#1761bd"];
  return (
    <div className="map-visual us-map">
      <svg viewBox="0 0 500 290" role="img" aria-label="United States unemployment choropleth">
        <path className="map-outline" d="M33 39 L89 35 119 53 181 58 215 68 300 69 333 85 367 72 401 87 449 65 471 91 455 123 423 136 407 167 432 185 403 214 374 217 346 251 314 245 286 221 245 235 210 255 176 248 148 218 120 211 92 184 62 170 70 138 48 113Z" />
        {cells.map(([x, y, w, h, level], index) => <rect x={x} y={y} width={w} height={h} rx="2" fill={palette[level]} stroke="#d6e5ff" strokeWidth="1" key={index} />)}
        <path className="map-watermark" d="M30 40 L95 36 145 60 220 69 310 70 356 88 401 85 452 67 470 92 445 125 416 139 405 171 430 186 396 214 368 218 344 249 309 243 284 221 245 235 210 253 177 248 146 218 118 209 89 182 60 168 69 137 47 112Z" />
        <g transform="translate(35 222)"><path d="M0 7 L54 2 79 20 62 41 25 38Z" fill="#4e83dc" stroke="#a7c8fb" /><path d="M98 21 l18 -13 18 9 -5 16 -24 2z" fill="#7da8ef" /></g>
      </svg>
      <div className="map-key"><span>Unemployment Rate</span>{[["7.0 and above", "#1761bd"], ["5.0 to 6.9", "#4e83dc"], ["3.5 to 4.9", "#7da8ef"], ["2.5 to 3.4", "#a7c8fb"], ["Below 2.5", "#d8e7ff"]].map(([label, color]) => <i key={label}><b style={{ background: color }} />{label}</i>)}</div>
    </div>
  );
}

function TexasMap() {
  const blocks = [];
  for (let row = 0; row < 9; row += 1) {
    for (let col = 0; col < 12; col += 1) {
      const shade = (row * 7 + col * 3) % 5;
      blocks.push(<rect x={40 + col * 32} y={16 + row * 25} width="30" height="23" fill={["#e4f1c6", "#c8e4b6", "#91cdb2", "#4ba69c", "#197896"][shade]} stroke="#f5f8ea" key={`${row}-${col}`} />);
    }
  }
  return (
    <div className="map-visual texas-map">
      <svg viewBox="0 0 470 270" role="img" aria-label="Texas county population choropleth">
        <defs><clipPath id="texas-shape"><path d="M38 17 L224 20 227 69 278 69 300 94 341 102 371 123 424 138 411 174 386 189 374 231 347 250 315 220 286 201 259 171 231 162 205 146 176 145 145 117 113 105 89 78 39 77Z" /></clipPath></defs>
        <g clipPath="url(#texas-shape)">{blocks}</g>
        <path className="texas-outline" d="M38 17 L224 20 227 69 278 69 300 94 341 102 371 123 424 138 411 174 386 189 374 231 347 250 315 220 286 201 259 171 231 162 205 146 176 145 145 117 113 105 89 78 39 77Z" />
      </svg>
      <div className="map-key light"><span>Population</span>{[["500,000+", "#197896"], ["100,000–499,999", "#4ba69c"], ["50,000–99,999", "#91cdb2"], ["10,000–49,999", "#c8e4b6"], ["Below 10,000", "#e4f1c6"]].map(([label, color]) => <i key={label}><b style={{ background: color }} />{label}</i>)}</div>
      <div className="map-zoom"><button type="button">+</button><button type="button">−</button></div>
    </div>
  );
}

function RankedList({ heading, items }) {
  return (
    <div className="rank-list">
      <strong>{heading}</strong>
      {items.map(([name, value], index) => <div key={name}><span><b>{index + 1}</b>{name}</span><em>{value}</em></div>)}
    </div>
  );
}

function BlsDashboard({ sourceState }) {
  const liveValue = formatValue(sourceState.items[0]?.value);
  const latest = liveValue ? `${liveValue}%` : "3.7%";
  return (
    <>
      <section className="kpi-strip">
        <Kpi label="Unemployment Rate" value={latest} detail={dateLabel(sourceState.items[0], "May 2025")} delta="↓ 0.2 pp vs Apr 2025" positive />
        <Kpi label="Total Nonfarm Payrolls" value="159.3M" detail="May 2025" delta="↑ 139K vs Apr 2025" positive />
        <Kpi label="Labor Force Participation" value="62.4%" detail="May 2025" delta="↑ 0.1 pp vs Apr 2025" positive />
        <Kpi label="Employment–Population Ratio" value="59.9%" detail="May 2025" delta="↑ 0.1 pp vs Apr 2025" positive />
      </section>
      <section className="dashboard-grid bls-grid">
        <Panel title="Unemployment Rate by State" subtitle="May 2025" className="bls-map-panel"><UsMap /></Panel>
        <Panel title="Unemployment Rate Over Time" subtitle="United States"><LineChart values={BLS_LINE} color="#45a6ff" labels={["2005", "2010", "2015", "2020", "2025"]} area /></Panel>
        <Panel title="Top / Bottom States by Unemployment Rate" subtitle="May 2025" className="rank-panel">
          <div className="two-ranks">
            <RankedList heading="Highest" items={[["Nevada", "5.8%"], ["California", "5.6%"], ["New Mexico", "5.1%"], ["Michigan", "4.9%"], ["Alaska", "4.8%"]]} />
            <RankedList heading="Lowest" items={[["South Dakota", "1.6%"], ["Vermont", "1.7%"], ["North Dakota", "1.7%"], ["Nebraska", "1.8%"], ["Utah", "1.9%"]]} />
          </div>
        </Panel>
        <Panel title="Measure Description" className="description-panel">
          <p>The unemployment rate is the number of unemployed persons as a percent of the labor force.</p>
          <p>Source: U.S. Bureau of Labor Statistics (BLS)</p>
          <a href="https://www.bls.gov/cps/definitions.htm#ur" target="_blank" rel="noreferrer">View Full Definition ↗</a>
        </Panel>
      </section>
    </>
  );
}

function CensusDashboard({ sourceState }) {
  const live = formatValue(sourceState.items[0]?.value, 0);
  const counties = sourceState.items.slice(0, 5).map((item) => [
    item.geo_name || item.county_name || item.geo_id,
    formatValue(item.value, 0) || "—",
  ]);
  const tableRows = counties.length ? counties : [["Harris County", "4,781,609"], ["Dallas County", "2,600,840"], ["Tarrant County", "2,110,640"], ["Bexar County", "2,059,530"], ["Travis County", "1,326,436"]];
  return (
    <>
      <section className="kpi-strip census-kpis">
        <Kpi label="Total Population" value={live || "30,503,301"} />
        <Kpi label="Population Change (Since 2020)" value="+2,302,878" delta="+8.2%" />
        <Kpi label="Population Density" value="116.1" detail="People per sq mi" />
        <Kpi label="Median Age" value="35.0" detail="Years" />
      </section>
      <section className="dashboard-grid census-grid">
        <Panel title="Population by County" subtitle="Texas  |  2023"><TexasMap /></Panel>
        <Panel title="Population Over Time" subtitle="Texas"><LineChart values={CENSUS_LINE} color="#1764d9" labels={["2000", "2005", "2010", "2015", "2020", "2023"]} /></Panel>
        <Panel title="Largest Counties" className="county-table-panel">
          <table className="dashboard-table"><thead><tr><th>#</th><th>County</th><th>Population</th><th>% of State</th></tr></thead><tbody>{tableRows.map(([name, value], index) => <tr key={name}><td>{index + 1}</td><td>{name}</td><td>{value}</td><td>{["15.7%", "8.5%", "6.9%", "6.8%", "4.4%"][index]}</td></tr>)}</tbody></table>
        </Panel>
        <Panel title="Demographic Highlights" subtitle="Texas  |  2023" className="demographic-panel">
          <div className="donut" aria-label="Demographic distribution donut chart" />
          <div className="donut-legend">{[["White", "39.6%", "#5aa582"], ["Hispanic or Latino", "40.2%", "#2872a8"], ["Black or African American", "12.4%", "#174d8a"], ["Asian", "5.6%", "#d8d98c"], ["Other / Two+ Races", "2.2%", "#94a8c6"]].map(([name, value, color]) => <div key={name}><i style={{ background: color }} /><span>{name}</span><b>{value}</b></div>)}</div>
        </Panel>
      </section>
    </>
  );
}

function FredDashboard({ sourceState }) {
  const live = formatValue(sourceState.items[0]?.value, 2);
  return (
    <>
      <section className="kpi-strip fred-kpis">
        <Kpi label="Latest Value" value={live || "315.17"} detail={dateLabel(sourceState.items[0], "May 2025")} />
        <Kpi label="Change from Prior Month" value="+0.13%" detail="Apr 2025" />
        <Kpi label="Change from Prior Year" value="+2.44%" detail="May 2024" />
        <Kpi label="Series Start" value="Jan 1947" />
        <Kpi label="Unit" value="Index 1982–84=100" />
        <Kpi label="Frequency" value="Monthly" />
      </section>
      <section className="dashboard-grid fred-grid">
        <Panel title="CPI – All Items Over Time" subtitle="Index 1982–84=100"><LineChart values={FRED_LINE} color="#075bd8" labels={["1950", "1960", "1970", "1980", "1990", "2000", "2010", "2020", "2025"]} recessions /></Panel>
        <Panel title="Recent History" subtitle="Last 12 Months"><BarChart values={FRED_BARS} /></Panel>
        <Panel title="Related Indicators" className="related-panel">
          <table className="dashboard-table"><thead><tr><th>Series</th><th></th><th>Latest Value</th><th>YoY Change</th></tr></thead><tbody>
            {[["Core CPI (All Items Less Food & Energy)", "May 2025", "3.02%", "+2.85%"], ["PCE Price Index", "Apr 2025", "122.41", "+2.18%"], ["Personal Consumption Expenditures", "Apr 2025", "19,781.6B", "+3.65%"], ["Unemployment Rate", "May 2025", "4.02%", "+0.12 pp"], ["Federal Funds Effective Rate", "May 2025", "4.33%", "0.00 pp"]].map((row) => <tr key={row[0]}>{row.map((value, index) => <td className={index === 3 ? "positive-cell" : ""} key={index}>{value}</td>)}</tr>)}
          </tbody></table>
        </Panel>
      </section>
      <div className="fred-source-note">Source: Federal Reserve Bank of St. Louis (FRED) <span /> Data may be revised.</div>
    </>
  );
}

export default function SourceDashboard({ sourceKey }) {
  const meta = SOURCE_META[sourceKey] || SOURCE_META.census;
  const sourceState = useSourceObservations(sourceKey);
  const content = useMemo(() => {
    if (sourceKey === "bls") return <BlsDashboard sourceState={sourceState} />;
    if (sourceKey === "fred") return <FredDashboard sourceState={sourceState} />;
    return <CensusDashboard sourceState={sourceState} />;
  }, [sourceKey, sourceState]);

  return (
    <main className={`source-dashboard theme-${meta.theme} source-${sourceKey}`}>
      <Sidebar sourceKey={sourceKey} />
      <section className="source-dashboard-main">
        <p className="demo-banner" role="note" data-testid="demo-banner">
          <strong>Demonstration layout.</strong> Except for the first KPI and export rows
          when the status chip reads &ldquo;Live API&rdquo;, the values on this page —
          trend charts, secondary KPIs, ranked lists, related-indicator tables,
          demographic breakdowns, stylized maps, and filter options — are illustrative
          examples, not published data. For live, source-backed analysis use the{" "}
          <Link href="/explore">Explorer</Link>.
        </p>
        <DashboardHeader meta={meta} sourceState={sourceState} />
        <FilterBar sourceKey={sourceKey} />
        {content}
      </section>
    </main>
  );
}
