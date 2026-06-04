import { LatestObservationCollection } from "@/lib/types";

export function ObservationSidePanel({ latest }: { latest: LatestObservationCollection | null }) {
  return (
    <section className="rounded-lg border p-4 bg-white min-h-80">
      <h2 className="text-lg font-semibold mb-2">Observation Side Panel</h2>
      <p className="text-sm text-slate-600 mb-3">
        Latest loaded records: {latest?.count ?? 0}
      </p>
      <div className="text-xs text-slate-600 max-h-56 overflow-auto">
        {latest?.observations?.slice(0, 10).map((row) => (
          <div key={`${row.geo_id}-${row.period}`} className="py-1 border-b">
            {row.geo_id}: {row.value}
          </div>
        ))}
      </div>
    </section>
  );
}
