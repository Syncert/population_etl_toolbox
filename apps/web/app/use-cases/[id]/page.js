import { notFound } from "next/navigation";

import ProfileProduct from "../../../components/ProfileProduct";
import { findUseCasePage } from "../../../lib/useCasePages";

export const dynamic = "force-dynamic";

export async function generateMetadata({ params }) {
  const entry = findUseCasePage((await params).id);
  if (!entry) notFound();
  return { title: entry.title, description: entry.summary };
}

export default async function UseCasePage({ params }) {
  const entry = findUseCasePage((await params).id);
  if (!entry) notFound();
  return <ProfileProduct fixedTemplateId={entry.id} />;
}
