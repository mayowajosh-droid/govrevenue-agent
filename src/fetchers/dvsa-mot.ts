const CKAN = "https://data.gov.uk/api/action";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type DvsaDataset = {
  id: string;
  title: string;
  description: string;
  resourceCount: number;
  updated: string | null;
};

/** DVSA MOT test and results datasets — vehicle age/condition signals. No auth needed. */
export async function fetchDvsaMotDatasets(): Promise<DvsaDataset[]> {
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({ q: "DVSA MOT test results vehicle", rows: "10" });
    const res = await fetch(`${CKAN}/package_search?${params}`, { signal: ac.signal });
    if (!res.ok) return [];
    const data = await res.json() as { result?: { results?: Record<string, unknown>[] } };
    return (data?.result?.results ?? []).map(r => ({
      id: String(r["id"] ?? ""),
      title: String(r["title"] ?? ""),
      description: String(r["notes"] ?? "").slice(0, 200),
      resourceCount: Number(r["num_resources"] ?? 0),
      updated: (r["metadata_modified"] as string | undefined) ?? null,
    }));
  } catch { return []; }
}
