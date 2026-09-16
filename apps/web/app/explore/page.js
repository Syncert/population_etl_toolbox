import SourceExplorerPage from "../../components/SourceExplorerPage";
import { explorerTitle } from "../../lib/routeTitles";

// A server wrapper, so this route can name itself. The component it renders
// keeps `"use client"`: nothing about the screen moved to the server, and the
// wrapper awaits nothing but `searchParams`, which Next already has.
export async function generateMetadata({ searchParams }) {
  return { title: explorerTitle(await searchParams) };
}

export default function ExplorePage() {
  return <SourceExplorerPage sourceKey="census" />;
}
