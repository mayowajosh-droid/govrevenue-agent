const CKAN_BASE = "https://data.gov.uk/api/action";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type NrwDataset = {
  id: string;
  title: string;
  notes: string | null;
  url: string;
  tags: string[];
  lastModified: string;
  resources: { format: string; url: string; name: string }[];
};

export async function searchNrwDatasets(query = ""): Promise<NrwDataset[]> {
  try {
    const ac = makeAbort();
    const q = `natural resources wales ${query}`.trim();
    const res = await fetch(`${CKAN_BASE}/package_search?q=${encodeURIComponent(q)}&rows=20`, {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data = await res.json() as {
      result?: { results?: {
        id?: string; title?: string; notes?: string; url?: string;
        metadata_modified?: string;
        tags?: { name?: string }[];
        resources?: { format?: string; url?: string; name?: string }[];
      }[] };
    };
    return (data.result?.results ?? []).map(d => ({
      id: String(d.id || ""),
      title: String(d.title || ""),
      notes: d.notes ?? null,
      url: d.url ?? `https://data.gov.uk/dataset/${d.id}`,
      tags: (d.tags ?? []).map(t => t.name ?? "").filter(Boolean),
      lastModified: String(d.metadata_modified || ""),
      resources: (d.resources ?? []).map(r => ({
        format: String(r.format || ""),
        url: String(r.url || ""),
        name: String(r.name || ""),
      })),
    }));
  } catch { return []; }
}

export async function fetchWaterQualityDatasets(): Promise<NrwDataset[]> {
  return searchNrwDatasets("water quality");
}

export async function fetchForestryDatasets(): Promise<NrwDataset[]> {
  return searchNrwDatasets("forestry woodland");
}
