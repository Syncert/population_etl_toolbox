import UseCaseDirectory from "../../components/UseCaseDirectory";
import { useCaseGroups } from "../../lib/useCasePages";

export const dynamic = "force-dynamic";
export const metadata = { title: "Analytics use cases" };

export default function UseCasesPage() {
  const groups = useCaseGroups.map((group) => ({ ...group, pages: group.pages.map(({ id, href, title, rank, question, audience, products, group }) => ({ id, href, title, rank, question, audience, products, group })) }));
  return <UseCaseDirectory useCaseGroups={groups} />;
}
