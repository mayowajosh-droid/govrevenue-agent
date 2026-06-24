const BASE = "https://data.police.uk/api";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type PoliceCrime = {
  category: string;
  locationType: string | null;
  latitude: number | null;
  longitude: number | null;
  streetName: string;
  month: string;
  outcome: string | null;
};

export type PoliceForce = {
  id: string;
  name: string;
};

export type CrimeCategory = {
  url: string;
  name: string;
};

export async function fetchPoliceForces(): Promise<PoliceForce[]> {
  try {
    const ac = makeAbort();
    const res = await fetch(`${BASE}/forces`, { headers: { Accept: "application/json" }, signal: ac.signal });
    if (!res.ok) return [];
    const data = await res.json() as { id?: string; name?: string }[];
    return Array.isArray(data)
      ? data.map(f => ({ id: String(f.id || ""), name: String(f.name || "") })).filter(f => f.id)
      : [];
  } catch { return []; }
}

export async function fetchCrimeCategories(): Promise<CrimeCategory[]> {
  try {
    const ac = makeAbort();
    const res = await fetch(`${BASE}/crime-categories`, { headers: { Accept: "application/json" }, signal: ac.signal });
    if (!res.ok) return [];
    const data = await res.json() as { url?: string; name?: string }[];
    return Array.isArray(data)
      ? data.map(c => ({ url: String(c.url || ""), name: String(c.name || "") }))
      : [];
  } catch { return []; }
}

export async function fetchCrimesAtLocation(lat: number, lon: number, date?: string): Promise<PoliceCrime[]> {
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({ lat: String(lat), lng: String(lon) });
    if (date) params.set("date", date);
    const res = await fetch(`${BASE}/crimes-at-location?${params}`, {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data = await res.json() as {
      category?: string;
      location_type?: string;
      location?: { latitude?: string; longitude?: string; street?: { name?: string } };
      month?: string;
      outcome_status?: { category?: string } | null;
    }[];
    return Array.isArray(data)
      ? data.map(c => ({
          category: String(c.category || ""),
          locationType: c.location_type ?? null,
          latitude: c.location?.latitude ? parseFloat(c.location.latitude) : null,
          longitude: c.location?.longitude ? parseFloat(c.location.longitude) : null,
          streetName: c.location?.street?.name ?? "",
          month: String(c.month || ""),
          outcome: c.outcome_status?.category ?? null,
        }))
      : [];
  } catch { return []; }
}

export async function fetchCrimesByForce(forceId: string, date?: string): Promise<PoliceCrime[]> {
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({ force: forceId });
    if (date) params.set("date", date);
    const res = await fetch(`${BASE}/crimes-no-location?${params}`, {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data = await res.json() as {
      category?: string;
      month?: string;
      outcome_status?: { category?: string } | null;
    }[];
    return Array.isArray(data)
      ? data.map(c => ({
          category: String(c.category || ""),
          locationType: null,
          latitude: null,
          longitude: null,
          streetName: forceId,
          month: String(c.month || ""),
          outcome: c.outcome_status?.category ?? null,
        }))
      : [];
  } catch { return []; }
}
