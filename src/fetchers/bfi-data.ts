const CKAN = "https://data.gov.uk/api/action";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type BfiDataset = {
  id: string;
  title: string;
  notes: string;
  resourceCount: number;
  updated: string | null;
};

/** BFI British Film Institute datasets — film, TV, creative industry signals. No auth needed. */
export async function fetchBfiDatasets(): Promise<BfiDataset[]> {
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({
      q: "BFI British Film Institute film industry television",
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
