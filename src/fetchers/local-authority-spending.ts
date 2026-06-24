const BASE = "https://data.gov.uk/api/action/package_search";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type LocalAuthoritySpend = {
  authority: string;
  datasetUrl: string;
  lastUpdated: string;
  description: string | null;
  resources: { format: string; url: string; name: string }[];
};

function mapPackage(pkg: Record<string, unknown>): LocalAuthoritySpend {
  const org = pkg.organization as Record<string, unknown> | undefined;
  const resources = Array.isArray(pkg.resources)
    ? (pkg.resources as Record<string, unknown>[]).map((r) => ({
        format: String(r.format ?? ""),
        url: String(r.url ?? ""),
        name: String(r.name ?? r.description ?? ""),
      }))
    : [];

  return {
    authority: org ? String(org.title ?? org.name ?? "") : String(pkg.organization ?? ""),
    datasetUrl: pkg.name
      ? `https://data.gov.uk/dataset/${encodeURIComponent(String(pkg.name))}`
      : String(pkg.url ?? ""),
    lastUpdated: String(pkg.metadata_modified ?? pkg.metadata_created ?? ""),
    description: pkg.notes ? String(pkg.notes) : null,
    resources,
  };
}

/**
 * Search data.gov.uk for local authority spending-over-£500 datasets.
 */
export async function searchLocalAuthoritySpending(
  authorityName?: string
): Promise<LocalAuthoritySpend[]> {
  try {
    const ac = makeAbort();
    const q = `spending over 500 ${authorityName ?? ""}`.trim();
    const url = `${BASE}?q=${encodeURIComponent(q)}&rows=20`;
    const res = await fetch(url, {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data: unknown = await res.json();
    if (!data || typeof data !== "object") return [];

    const result = (data as Record<string, unknown>).result;
    if (!result || typeof result !== "object") return [];

    const results = (result as Record<string, unknown>).results;
    if (!Array.isArray(results)) return [];

    return results.map((pkg: Record<string, unknown>) => mapPackage(pkg));
  } catch {
    return [];
  }
}
