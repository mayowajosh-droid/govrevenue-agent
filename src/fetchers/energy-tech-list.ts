const CKAN_BASE = "https://data.gov.uk/api/action";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type EtlProduct = {
  id: string;
  name: string;
  category: string;
  organization: string | null;
  description: string | null;
  url: string;
  lastModified: string;
};

export async function searchEtlProducts(query = ""): Promise<EtlProduct[]> {
  try {
    const ac = makeAbort();
    const q = `energy technology list ${query}`.trim();
    const res = await fetch(`${CKAN_BASE}/package_search?q=${encodeURIComponent(q)}&rows=20`, {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data = await res.json() as {
      result?: { results?: {
        id?: string; title?: string; notes?: string;
        organization?: { title?: string };
        url?: string; metadata_modified?: string;
        groups?: { display_name?: string }[];
      }[] };
    };
    return (data.result?.results ?? []).map(d => ({
      id: String(d.id || ""),
      name: String(d.title || ""),
      category: d.groups?.[0]?.display_name ?? "Energy Technology",
      organization: d.organization?.title ?? null,
      description: d.notes ?? null,
      url: d.url ?? `https://data.gov.uk/dataset/${d.id}`,
      lastModified: String(d.metadata_modified || ""),
    }));
  } catch { return []; }
}

export async function fetchEnergyEfficiencyData(): Promise<EtlProduct[]> {
  return searchEtlProducts("energy efficiency");
}

export async function fetchRenewableEnergyData(): Promise<EtlProduct[]> {
  return searchEtlProducts("renewable solar wind");
}
