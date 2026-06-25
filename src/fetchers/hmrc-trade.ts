const CKAN = "https://data.gov.uk/api/action";
const TIMEOUT_MS = 20_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type UkTradeFlow = {
  commodity: string;
  description: string;
  flowType: string;
  value: number;
  period: string;
};

export type UkTradeCountry = {
  countryCode: string;
  countryName: string;
};

/** HMRC UK Trade statistics datasets via data.gov.uk. No auth required. */
export async function fetchTopTradeFlows(top = 20): Promise<UkTradeFlow[]> {
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({
      q: "UK trade statistics imports exports overseas commodities",
      rows: String(top),
      sort: "metadata_modified desc",
    });
    const res = await fetch(`${CKAN}/package_search?${params}`, { signal: ac.signal });
    if (!res.ok) return [];
    const data = await res.json() as { result?: { results?: Record<string, unknown>[] } };
    return (data?.result?.results ?? []).slice(0, top).map(r => ({
      commodity: String(r["id"] ?? ""),
      description: String(r["title"] ?? ""),
      flowType: "dataset",
      value: Number((r as Record<string, unknown>)["num_resources"] ?? 0),
      period: String(r["metadata_modified"] ?? ""),
    }));
  } catch { return []; }
}

export async function fetchTradeCountries(): Promise<UkTradeCountry[]> {
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({
      q: "HMRC trade by country bilateral overseas",
      fq: "organization:hm-revenue-and-customs",
      rows: "50",
    });
    const res = await fetch(`${CKAN}/package_search?${params}`, { signal: ac.signal });
    if (!res.ok) return [];
    const data = await res.json() as { result?: { results?: Record<string, unknown>[] } };
    return (data?.result?.results ?? []).map(r => ({
      countryCode: String(r["id"] ?? ""),
      countryName: String(r["title"] ?? ""),
    }));
  } catch { return []; }
}
