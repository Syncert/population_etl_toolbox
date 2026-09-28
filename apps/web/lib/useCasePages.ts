import { PRODUCT_TEMPLATES } from "./productTemplates";

/** Reviewed catalog-backed products that can answer a reader's place question. */
export const useCasePages = PRODUCT_TEMPLATES.map((template) => ({
  ...template,
  href: `/use-cases/${template.id}`,
}));

export function findUseCasePage(id: string) {
  return useCasePages.find((entry) => entry.id === id) || null;
}
