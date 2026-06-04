import { Metric } from "@/lib/types";

export function MetricControlPanel({ metrics }: { metrics: Metric[] }) {
  return (
    <aside className="rounded-lg border p-4 bg-white space-y-3">
      <h2 className="text-lg font-semibold">Controls</h2>
      <div>
        <label className="block text-sm font-medium mb-1">Metric</label>
        <select className="w-full rounded border p-2" defaultValue={metrics[0]?.metric_id ?? "population"}>
          {(metrics.length ? metrics : [{ metric_id: "population", display_name: "Total Population" } as Metric]).map((metric) => (
            <option key={metric.metric_id} value={metric.metric_id}>
              {metric.display_name}
            </option>
          ))}
        </select>
      </div>
      <div>
        <label className="block text-sm font-medium mb-1">Geo Level</label>
        <select className="w-full rounded border p-2" defaultValue="county">
          <option value="county">County</option>
          <option value="state">State</option>
          <option value="national">National</option>
        </select>
      </div>
    </aside>
  );
}
