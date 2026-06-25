const CKAN = "https://data.gov.uk/api/action";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type NisraDataset = {
  id: string;
  title: string;
  notes: string;
  resourceCount: number;
  updated: string | null;
};

/** NISRA — Northern Ireland Statistics and Research Agency datasets. No auth needed. */
export async function fetchNisraDatasets(): Promise<NisraDataset[]> {
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({
      q: "NISRA Northern Ireland statistics research",
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
      resourceCount: Number(r["num_resources"] ?? 0),
      updated: (r["metadata_modified"] as string | undefined) ?? null,
    }));
  } catch { return []; }
}
