import PublicDataPage from "../../components/PublicDataPage";
import { STATIC_ROUTE_TITLES } from "../../lib/routeTitles";

export const metadata = {
  title: STATIC_ROUTE_TITLES["/data"],
  description: "When each source last refreshed, what it covers, and the rules every number on this site follows.",
};

export default function DataPage() {
  return <PublicDataPage />;
}
