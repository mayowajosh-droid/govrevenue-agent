const CKAN = "https://data.gov.uk/api/action";
const SPARQL = "https://statistics.gov.scot/sparql";
const TIMEOUT_MS = 20_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type ScottishDataset = {
  id: string;
  title: string;
  notes: string;
  resourceCount: number;
  updated: string | null;
};

/** Scottish Government national statistics datasets via data.gov.uk. No auth needed. */
export async function fetchScottishGovDatasets(): Promise<ScottishDataset[]> {
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({
      q: "scotland statistics national records government",
      rows: "20",
      sort: "metadata_modified desc",
    });
    const res = await fetch(`${CKAN}/package_search?${params}`, { signal: ac.signal });
    if (!res.ok) return [];
    const data = await res.json() as { result?: { results?: Record<string, unknown>[] } };
    return (data?.result?.results ?? []).map(r => ({
      id: String(r["id"] ?? ""),
      title: String(r["title"] ?? ""),
      notes: String(r["notes"] ?? "").slice(0, 200),
      resourceCount: Number(r["num_resources"] ?? 0),
      updated: (r["metadata_modified"] as string | undefined) ?? null,
    }));
  } catch { return []; }
}

/** Query the statistics.gov.scot SPARQL endpoint for dataset URIs. */
export async function fetchScottishStatsSparql(): Promise<{ uri: string; label: string }[]> {
  const query = `
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?dataset ?label WHERE {
      ?dataset a <http://purl.org/linked-data/cube#DataSet> .
      OPTIONAL { ?dataset rdfs:label ?label }
    } LIMIT 20
  `.trim();
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({ query, format: "json" });
    const res = await fetch(`${SPARQL}?${params}`, {
      headers: { Accept: "application/sparql-results+json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data = await res.json() as { results?: { bindings?: { dataset?: { value?: string }; label?: { value?: string } }[] } };
    return (data?.results?.bindings ?? []).map(b => ({
      uri: String(b.dataset?.value ?? ""),
      label: String(b.label?.value ?? ""),
    }));
  } catch { return []; }
}
