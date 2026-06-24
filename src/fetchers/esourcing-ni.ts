// eSourcing NI — Northern Ireland public procurement
// Falls back to data.gov.uk CKAN search filtered to Northern Ireland when direct feed unavailable

const CKAN_URL = "https://data.gov.uk/api/action/package_search";
const TIMEOUT_MS = 15_000;

export type EsourcingNiNotice = {
  title: string;
  buyer: string | null;
  description: string | null;
  publishedDate: string | null;
  url: string | null;
  region: "Northern Ireland";
};

export async function fetchNiProcurementNotices(): Promise<EsourcingNiNotice[]> {
  try {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
    let data: any;
    try {
      const url = new URL(CKAN_URL);
      url.searchParams.set("q", "Northern Ireland procurement contract tender");
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

    const results = data?.result?.results ?? [];
    return (results as any[]).slice(0, 40).map((r: any) => ({
      title: r.title ?? "Untitled",
      buyer: r.organization?.title ?? null,
      description: r.notes ? String(r.notes).slice(0, 300) : null,
      publishedDate: r.metadata_modified ?? r.metadata_created ?? null,
      url: r.url ?? null,
      region: "Northern Ireland" as const,
    })).filter((n: EsourcingNiNotice) => n.title !== "Untitled");
  } catch {
    return [];
  }
}
