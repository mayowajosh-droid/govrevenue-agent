const CKAN = "https://data.gov.uk/api/action";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type StatsWalesDataset = {
  id: string;
  title: string;
  description: string;
  updated: string | null;
  tags: string[];
};

/** Welsh Government statistical datasets via data.gov.uk CKAN. No auth needed. */
export async function fetchStatsWalesDatasets(topic?: string): Promise<StatsWalesDataset[]> {
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({
      q: topic ?? "Wales Welsh government statistics economy housing health",
      fq: "organization:welsh-government OR organization:stats-wales",
      rows: "20",
      sort: "metadata_modified desc",
    });
    const res = await fetch(`${CKAN}/package_search?${params}`, { signal: ac.signal });
    if (!res.ok) return [];
    const data = await res.json() as { result?: { results?: Record<string, unknown>[] } };
    const items = data?.result?.results ?? [];
    if (items.length === 0) {
      // Fallback: broader Welsh stats search
      const ac2 = makeAbort();
      const p2 = new URLSearchParams({
        q: topic ?? "Wales Welsh statistics data",
        rows: "20",
        sort: "metadata_modified desc",
      });
      const r2 = await fetch(`${CKAN}/package_search?${p2}`, { signal: ac2.signal });
      if (!r2.ok) return [];
      const d2 = await r2.json() as { result?: { results?: Record<string, unknown>[] } };
      return (d2?.result?.results ?? []).map(r => ({
        id: String(r["id"] ?? ""),
        title: String(r["title"] ?? ""),
        description: String(r["notes"] ?? "").slice(0, 200),
        updated: (r["metadata_modified"] as string | undefined) ?? null,
        tags: Array.isArray(r["tags"]) ? (r["tags"] as { name?: string }[]).map(t => t.name ?? "") : [],
      }));
    }
    return items.map(r => ({
      id: String(r["id"] ?? ""),
      title: String(r["title"] ?? ""),
      description: String(r["notes"] ?? "").slice(0, 200),
      updated: (r["metadata_modified"] as string | undefined) ?? null,
      tags: Array.isArray(r["tags"]) ? (r["tags"] as { name?: string }[]).map(t => t.name ?? "") : [],
    }));
  } catch { return []; }
}
