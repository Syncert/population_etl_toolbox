import WorkbenchPage from "../../components/WorkbenchPage";
import { workbenchTitle } from "../../lib/routeTitles";

export async function generateMetadata({ searchParams }) {
  return { title: workbenchTitle(await searchParams) };
}

export default function Workbench() {
  return <WorkbenchPage />;
}
