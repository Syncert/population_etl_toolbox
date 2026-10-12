import PlacePage from "../../components/PlacePage";
import { STATIC_ROUTE_TITLES } from "../../lib/routeTitles";

export const metadata = {
  title: STATIC_ROUTE_TITLES["/us"],
  description: "The nation's place page: people, work, housing, health, safety, land, and change, as published.",
};

export default function NationPage() {
  return <PlacePage />;
}
