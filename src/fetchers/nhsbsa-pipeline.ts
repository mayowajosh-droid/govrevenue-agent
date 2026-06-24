const NHSBSA_BASE = "https://opendata.nhsbsa.net/api/3/action";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type NhsbsaDataset = {
  id: string;
  name: string;
  title: string;
  notes: string | null;
  url: string;
  metadata_created: string;
  metadata_modified: string;
  organization: string | null;
  tags: string[];
};

type CkanResult = {
  id?: string;
  name?: string;
  title?: string;
  notes?: string;
  url?: string;
  metadata_created?: string;
  metadata_modified?: string;
  organization?: { title?: string; name?: string } | null;
  tags?: Array<{ name?: string }>;
};

function mapDataset(r: CkanResult): NhsbsaDataset | null {
  if (!r.id || !r.name) return null;
  return {
    id: String(r.id),
    name: String(r.name),
    title: String(r.title || r.name),
    notes: r.notes ? String(r.notes).slice(0, 1000) : null,
    url: r.url
      ? String(r.url)
      : `https://opendata.nhsbsa.net/dataset/${encodeURIComponent(String(r.name))}`,
    metadata_created: String(r.metadata_created || ""),
    metadata_modified: String(r.metadata_modified || ""),
    organization: r.organization?.title
      ? String(r.organization.title)
      : r.organization?.name
        ? String(r.organization.name)
        : null,
    tags: Array.isArray(r.tags)
      ? r.tags.map((t) => String(t.name || "")).filter(Boolean)
      : [],
  };
}

/**
 * Search NHSBSA open-data CKAN portal for datasets matching a query.
 * Defaults to "procurement" if no query is provided.
 */
export async function searchNhsbsaDatasets(
  query?: string,
): Promise<NhsbsaDataset[]> {
  try {
    const q = (query || "procurement").trim();
    const url = new URL(`${NHSBSA_BASE}/package_search`);
    url.searchParams.set("q", q);
    url.searchParams.set("rows", "50");

    const ac = makeAbort();
    const res = await fetch(url.toString(), {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];

    const body = (await res.json()) as {
      success?: boolean;
      result?: { results?: CkanResult[] };
    };
    if (!body.success || !body.result?.results) return [];

    return body.result.results
      .map(mapDataset)
      .filter((d): d is NhsbsaDataset => d !== null);
  } catch {
    return [];
  }
}

/**
 * Convenience wrapper: searches for "procurement pipeline" datasets.
 */
export async function fetchNhsbsaPipelineData(): Promise<NhsbsaDataset[]> {
  return searchNhsbsaDatasets("procurement pipeline");
}
