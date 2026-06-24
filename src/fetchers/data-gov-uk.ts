const BASE = "https://data.gov.uk/api/action";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type DataGovDataset = {
  id: string;
  name: string;
  title: string;
  notes: string | null;
  organization: string | null;
  url: string;
  metadata_created: string;
  metadata_modified: string;
  tags: string[];
  resources: {
    format: string;
    url: string;
    name: string;
    description: string | null;
  }[];
};

function mapDataset(pkg: Record<string, unknown>): DataGovDataset {
  const org = pkg.organization as Record<string, unknown> | undefined;
  const rawTags = Array.isArray(pkg.tags)
    ? (pkg.tags as Record<string, unknown>[]).map((t) =>
        String(t.display_name ?? t.name ?? "")
      )
    : [];
  const resources = Array.isArray(pkg.resources)
    ? (pkg.resources as Record<string, unknown>[]).map((r) => ({
        format: String(r.format ?? ""),
        url: String(r.url ?? ""),
        name: String(r.name ?? ""),
        description: r.description ? String(r.description) : null,
      }))
    : [];

  const name = String(pkg.name ?? "");
  return {
    id: String(pkg.id ?? ""),
    name,
    title: String(pkg.title ?? ""),
    notes: pkg.notes ? String(pkg.notes) : null,
    organization: org ? String(org.title ?? org.name ?? "") : null,
    url: name
      ? `https://data.gov.uk/dataset/${encodeURIComponent(name)}`
      : String(pkg.url ?? ""),
    metadata_created: String(pkg.metadata_created ?? ""),
    metadata_modified: String(pkg.metadata_modified ?? ""),
    tags: rawTags,
    resources,
  };
}

/**
 * General-purpose dataset search on data.gov.uk (CKAN API).
 */
export async function searchDatasets(
  query: string,
  limit = 20
): Promise<DataGovDataset[]> {
  try {
    const ac = makeAbort();
    const url = `${BASE}/package_search?q=${encodeURIComponent(query)}&rows=${limit}`;
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

    return results.map((pkg: Record<string, unknown>) => mapDataset(pkg));
  } catch {
    return [];
  }
}

/** Search for procurement-related datasets. */
export async function fetchProcurementDatasets(): Promise<DataGovDataset[]> {
  return searchDatasets("procurement contracts public sector");
}

/** Search for government spending datasets. */
export async function fetchSpendingDatasets(): Promise<DataGovDataset[]> {
  return searchDatasets("government spending expenditure");
}
