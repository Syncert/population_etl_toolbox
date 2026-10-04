import { notFound } from "next/navigation";

import ProfileProduct from "../../../components/ProfileProduct";
import { findUseCasePage, useCasePages } from "../../../lib/useCasePages";
import UseCaseIntro from "../../../components/UseCaseIntro";
import DataQualityExplorer from "../../../components/DataQualityExplorer";

export const dynamic = "force-dynamic";

export async function generateMetadata({ params }) {
  const entry = findUseCasePage((await params).id);
  if (!entry) notFound();
  return { title: entry.title, description: entry.summary };
}

export default async function UseCasePage({ params }) {
  const entry = findUseCasePage((await params).id);
  if (!entry) notFound();
  if (entry.tool === "quality") return <main className="page-shell use-case-page use-case-publishing"><UseCaseIntro entry={entry} /><DataQualityExplorer embedded /></main>;
  const related = useCasePages.filter((page) => page.group === entry.group && page.id !== entry.id).map(({ id, title, href, rank }) => ({ id, title, href, rank }));
  return <ProfileProduct fixedTemplateId={entry.id} useCase={entry} relatedUseCases={related} />;
}
