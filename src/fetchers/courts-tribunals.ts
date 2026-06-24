const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type CourtRecord = {
  name: string;
  slug: string;
  address: string | null;
  postcode: string | null;
  areasOfLaw: string[];
  courtTypes: string[];
  open: boolean;
  lat: number | null;
  lon: number | null;
};

type RawCourt = {
  name?: string;
  slug?: string;
  address?: { address_lines?: string[]; postcode?: string }[];
  areas_of_law?: { name?: string }[];
  types?: string[];
  open?: boolean;
  lat?: number;
  lon?: number;
};

function mapCourt(c: RawCourt): CourtRecord {
  const addr = c.address?.[0];
  const lines = (addr?.address_lines ?? []).join(", ");
  return {
    name: String(c.name || ""),
    slug: String(c.slug || ""),
    address: lines || null,
    postcode: addr?.postcode ?? null,
    areasOfLaw: (c.areas_of_law ?? []).map(a => a.name ?? "").filter(Boolean),
    courtTypes: c.types ?? [],
    open: c.open ?? true,
    lat: c.lat ?? null,
    lon: c.lon ?? null,
  };
}

export async function searchCourts(query: string): Promise<CourtRecord[]> {
  try {
    const ac = makeAbort();
    const res = await fetch(
      `https://www.find-court-tribunal.service.gov.uk/search/results.json?q=${encodeURIComponent(query)}`,
      { headers: { Accept: "application/json" }, signal: ac.signal }
    );
    if (!res.ok) return [];
    const data = await res.json() as RawCourt[];
    return Array.isArray(data) ? data.map(mapCourt) : [];
  } catch { return []; }
}

export async function getCourtBySlug(slug: string): Promise<CourtRecord | null> {
  try {
    const ac = makeAbort();
    const res = await fetch(
      `https://www.find-court-tribunal.service.gov.uk/courts/${encodeURIComponent(slug)}.json`,
      { headers: { Accept: "application/json" }, signal: ac.signal }
    );
    if (!res.ok) return null;
    const data = await res.json() as RawCourt;
    return mapCourt(data);
  } catch { return null; }
}

export async function fetchCourtsByAreaOfLaw(areaOfLaw: string): Promise<CourtRecord[]> {
  try {
    const ac = makeAbort();
    const res = await fetch(
      `https://www.find-court-tribunal.service.gov.uk/search/results.json?aol=${encodeURIComponent(areaOfLaw)}`,
      { headers: { Accept: "application/json" }, signal: ac.signal }
    );
    if (!res.ok) return [];
    const data = await res.json() as RawCourt[];
    return Array.isArray(data) ? data.map(mapCourt) : [];
  } catch { return []; }
}
