const BASE = "https://api.postcodes.io";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type PostcodeLookup = {
  postcode: string;
  longitude: number;
  latitude: number;
  admin_district: string | null;
  admin_county: string | null;
  parliamentary_constituency: string | null;
  region: string | null;
  country: string;
  nhs_ha: string | null;
  nuts: string | null;
};

function mapResult(r: any): PostcodeLookup | null {
  if (!r) return null;
  return {
    postcode: String(r.postcode ?? ""),
    longitude: Number(r.longitude ?? 0),
    latitude: Number(r.latitude ?? 0),
    admin_district: r.admin_district ? String(r.admin_district) : null,
    admin_county: r.admin_county ? String(r.admin_county) : null,
    parliamentary_constituency: r.parliamentary_constituency ? String(r.parliamentary_constituency) : null,
    region: r.region ? String(r.region) : null,
    country: String(r.country ?? ""),
    nhs_ha: r.nhs_ha ? String(r.nhs_ha) : null,
    nuts: r.nuts ? String(r.nuts) : null,
  };
}

export async function lookupPostcode(postcode: string): Promise<PostcodeLookup | null> {
  try {
    const ac = makeAbort();
    const res = await fetch(`${BASE}/postcodes/${encodeURIComponent(postcode)}`, {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return null;
    const data: any = await res.json();
    return mapResult(data.result);
  } catch {
    return null;
  }
}

export async function bulkLookupPostcodes(postcodes: string[]): Promise<PostcodeLookup[]> {
  if (postcodes.length === 0) return [];
  try {
    const ac = makeAbort();
    const batch = postcodes.slice(0, 100);
    const res = await fetch(`${BASE}/postcodes`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({ postcodes: batch }),
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data: any = await res.json();
    const results: any[] = Array.isArray(data.result) ? data.result : [];
    return results.map((r: any) => mapResult(r?.result)).filter((r): r is PostcodeLookup => r !== null);
  } catch {
    return [];
  }
}

export async function searchPostcodes(query: string): Promise<PostcodeLookup[]> {
  try {
    const ac = makeAbort();
    const res = await fetch(`${BASE}/postcodes?q=${encodeURIComponent(query)}&limit=10`, {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data: any = await res.json();
    const results: any[] = Array.isArray(data.result) ? data.result : [];
    return results.map((r: any) => mapResult(r)).filter((r): r is PostcodeLookup => r !== null);
  } catch {
    return [];
  }
}
