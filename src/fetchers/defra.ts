const CKAN_BASE = "https://data.gov.uk/api/action";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type DefraDataset = {
  id: string;
  title: string;
  notes: string | null;
  organization: string | null;
  url: string;
  tags: string[];
  lastModified: string;
  resources: { format: string; url: string; name: string }[];
};

async function searchDataGovUk(query: string, orgName?: string): Promise<DefraDataset[]> {
  try {
    const ac = makeAbort();
    const q = orgName ? `${query} organization:${orgName}` : query;
    const url = `${CKAN_BASE}/package_search?q=${encodeURIComponent(q)}&rows=20`;
    const res = await fetch(url, { headers: { Accept: "application/json" }, signal: ac.signal });
    if (!res.ok) return [];
    const data = await res.json() as {
      result?: { results?: {
        id?: string; title?: string; notes?: string;
        organization?: { title?: string };
        url?: string; metadata_modified?: string;
        tags?: { name?: string }[];
        resources?: { format?: string; url?: string; name?: string }[];
      }[] };
    };
    return (data.result?.results ?? []).map(d => ({
      id: String(d.id || ""),
      title: String(d.title || ""),
      notes: d.notes ?? null,
      organization: d.organization?.title ?? null,
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

export async function searchDefraDatasets(query = ""): Promise<DefraDataset[]> {
  return searchDataGovUk(`defra ${query}`.trim());
}

export async function fetchEnvironmentalPermits(): Promise<DefraDataset[]> {
  return searchDataGovUk("environmental permits licences", "environment-agency");
}

export async function fetchWasteDatasets(): Promise<DefraDataset[]> {
  return searchDataGovUk("waste recycling defra");
}

export async function fetchAirQualityDatasets(): Promise<DefraDataset[]> {
  return searchDataGovUk("air quality pollution defra");
}
