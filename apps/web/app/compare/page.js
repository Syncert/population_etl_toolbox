import ComparisonWorkspace from "../../components/ComparisonWorkspace";
import { comparisonTitle } from "../../lib/routeTitles";

export async function generateMetadata({ searchParams }) {
  return { title: comparisonTitle(await searchParams) };
}

export default function ComparePage() {
  return <ComparisonWorkspace />;
}
