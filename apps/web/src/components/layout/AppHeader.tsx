export function AppHeader({ apiStatus }: { apiStatus: string }) {
  return (
    <header className="border-b bg-white px-6 py-4 flex items-center justify-between">
      <h1 className="text-xl font-bold">Population Geospatial Analytics (First Pass)</h1>
      <span className="text-sm px-2 py-1 rounded border">API: {apiStatus}</span>
    </header>
  );
}
