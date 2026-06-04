import { AppHeader } from "@/components/layout/AppHeader";
import { MapLegend } from "@/components/ui/MapLegend";
import { TimeSeriesChart } from "@/features/comparison/TimeSeriesChart";
import { GeoMap } from "@/features/map/GeoMap";
import { MetricControlPanel } from "@/features/metric-catalog/MetricControlPanel";
import { ObservationSidePanel } from "@/features/observations/ObservationSidePanel";
import { apiClient } from "@/lib/api";

export default async function Home() {
  const [health, metrics, latest] = await Promise.all([
    apiClient.getHealth(),
    apiClient.getMetrics(),
    apiClient.getLatestPopulationByCounty(),
  ]);

  return (
    <main className="min-h-screen bg-slate-100">
      <AppHeader apiStatus={health?.status ?? "unavailable"} />
      <div className="p-6 grid grid-cols-1 lg:grid-cols-4 gap-4">
        <div className="lg:col-span-1">
          <MetricControlPanel metrics={metrics ?? []} />
          <MapLegend />
        </div>
        <div className="lg:col-span-2">
          <GeoMap />
          <TimeSeriesChart />
        </div>
        <div className="lg:col-span-1">
          <ObservationSidePanel latest={latest} />
        </div>
      </div>
    </main>
  );
}
