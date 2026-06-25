const CKAN = "https://data.gov.uk/api/action";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type InsolvencyDataset = {
  id: string;
  title: string;
  notes: string;
  updated: string | null;
  organisation: string;
};

/**
 * Insolvency Service datasets via data.gov.uk — market exit signals.
 * Supplements gazette.ts which covers live Gazette notices.
 */
export async function fetchInsolvencyDatasets(): Promise<InsolvencyDataset[]> {
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({
      q: "insolvency service company liquidation",
      rows: "20",
      sort: "metadata_modified desc",
    });
    const res = await fetch(`${CKAN}/package_search?${params}`, { signal: ac.signal });
    if (!res.ok) return [];
    const data = await res.json() as { result?: { results?: Record<string, unknown>[] } };
    return (data?.result?.results ?? []).map(r => ({
      id: String(r["id"] ?? ""),
      title: String(r["title"] ?? ""),
      notes: String(r["notes"] ?? "").slice(0, 200),
      updated: (r["metadata_modified"] as string | undefined) ?? null,
      organisation: String((r["organization"] as Record<string, unknown> | undefined)?.["title"] ?? "Insolvency Service"),
    }));
  } catch { return []; }
}
