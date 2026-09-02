// Catalog view-model helpers shared by catalog surfaces.

import type { SourceSummary } from "./api/types";

export interface SourceFilterOption {
  value: string;
  label: string;
}

// Builds the source filter from the API's published source list so the
// catalog never carries a closed client-side source enumeration.
export function sourceFilterOptions(
  sourceItems: SourceSummary[] | null | undefined,
): SourceFilterOption[] {
  return [
    { value: "", label: "All sources" },
    ...(Array.isArray(sourceItems) ? sourceItems : []).map((item) => ({
      value: item.source_code,
      label: item.source_name || item.source_code,
    })),
  ];
}
