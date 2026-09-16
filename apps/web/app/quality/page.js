import DataQualityExplorer from "../../components/DataQualityExplorer";
import { STATIC_ROUTE_TITLES } from "../../lib/routeTitles";

// A server wrapper, so this route can name itself. Its address carries
// no state, so the title is fixed.
export const metadata = { title: STATIC_ROUTE_TITLES["/quality"] };

export default function QualityPage() {
  return <DataQualityExplorer />;
}
