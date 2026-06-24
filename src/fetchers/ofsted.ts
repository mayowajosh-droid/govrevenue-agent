// Ofsted — inspection data via data.gov.uk CKAN (no key required)
// Ofsted publish downloadable datasets; we use the CKAN search API to locate them

const CKAN_URL = "https://data.gov.uk/api/action/package_search";
const TIMEOUT_MS = 15_000;

export type OfstedInspectionDataset = {
  id: string;
  title: string;
  description: string | null;
  publishedDate: string | null;
  url: string | null;
  format: string | null;
};

export async function fetchOfstedInspectionDatasets(): Promise<OfstedInspectionDataset[]> {
  try {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
    let data: any;
    try {
      const url = new URL(CKAN_URL);
      url.searchParams.set("q", "ofsted inspection school provider");
      url.searchParams.set("fq", "organization:ofsted");
      url.searchParams.set("rows", "40");
      url.searchParams.set("sort", "metadata_modified desc");
      const resp = await fetch(url.toString(), {
        headers: { "User-Agent": "AtlasRevenue/1.0" },
        signal: ctrl.signal,
      });
      if (!resp.ok) return [];
      data = await resp.json();
    } finally {
      clearTimeout(timer);
    }

    const results: any[] = data?.result?.results ?? [];
    return results.slice(0, 40).map((r: any) => {
      const resource = r.resources?.[0];
      return {
        id: r.id ?? "",
        title: r.title ?? "Untitled",
        description: r.notes ? String(r.notes).slice(0, 300) : null,
        publishedDate: r.metadata_modified ?? r.metadata_created ?? null,
        url: resource?.url ?? null,
        format: resource?.format ?? null,
      };
    }).filter((d: OfstedInspectionDataset) => d.title !== "Untitled");
  } catch {
    return [];
  }
}

export async function fetchOfstedRecentInspections(): Promise<OfstedInspectionDataset[]> {
  // Search for recently published Ofsted reports (30 days)
  try {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
    let data: any;
    try {
      const since = new Date(Date.now() - 30 * 24 * 3_600_000).toISOString().slice(0, 10);
      const url = new URL(CKAN_URL);
      url.searchParams.set("q", "ofsted inspection report outcome");
      url.searchParams.set("fq", `metadata_modified:[${since}T00:00:00Z TO *]`);
      url.searchParams.set("rows", "20");
      url.searchParams.set("sort", "metadata_modified desc");
      const resp = await fetch(url.toString(), {
        headers: { "User-Agent": "AtlasRevenue/1.0" },
        signal: ctrl.signal,
      });
      if (!resp.ok) return [];
      data = await resp.json();
    } finally {
      clearTimeout(timer);
    }

    const results: any[] = data?.result?.results ?? [];
    return results.map((r: any) => ({
      id: r.id ?? "",
      title: r.title ?? "Untitled",
      description: r.notes ? String(r.notes).slice(0, 300) : null,
      publishedDate: r.metadata_modified ?? null,
      url: r.resources?.[0]?.url ?? null,
      format: r.resources?.[0]?.format ?? null,
    })).filter((d: OfstedInspectionDataset) => d.title !== "Untitled");
  } catch {
    return [];
  }
}
