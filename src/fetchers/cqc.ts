// Care Quality Commission — public API (no key required)
// https://api.cqc.org.uk/public/v1/

const BASE_URL = "https://api.cqc.org.uk/public/v1";
const TIMEOUT_MS = 15_000;

export type CqcProvider = {
  providerId: string;
  name: string;
  type: string;
  registrationStatus: string;
  postalCode: string | null;
  region: string | null;
  lastInspectionDate: string | null;
  overallRating: string | null;
};

export type CqcLocation = {
  locationId: string;
  locationName: string;
  providerId: string;
  type: string;
  postalCode: string | null;
  region: string | null;
  registrationStatus: string;
};

async function cqcFetch<T>(path: string): Promise<T | null> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
  try {
    const resp = await fetch(`${BASE_URL}${path}`, {
      headers: { "User-Agent": "AtlasRevenue/1.0", "Accept": "application/json" },
      signal: ctrl.signal,
    });
    if (!resp.ok) return null;
    return await resp.json() as T;
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

export async function fetchCqcProviders(pageSize = 40): Promise<CqcProvider[]> {
  const data = await cqcFetch<{ providers: any[]; total: number }>(
    `/providers?page=1&perPage=${pageSize}&registrationStatus=Registered`
  );
  if (!data?.providers) return [];
  return data.providers.map((p: any) => ({
    providerId: p.providerId ?? "",
    name: p.name ?? "Unknown",
    type: p.type ?? "",
    registrationStatus: p.registrationStatus ?? "",
    postalCode: p.postalAddressPostcode ?? null,
    region: p.region ?? null,
    lastInspectionDate: p.lastInspection?.date ?? null,
    overallRating: p.currentRatings?.overall?.rating ?? null,
  }));
}

export async function fetchCqcLocations(pageSize = 40): Promise<CqcLocation[]> {
  const data = await cqcFetch<{ locations: any[]; total: number }>(
    `/locations?page=1&perPage=${pageSize}&registrationStatus=Registered`
  );
  if (!data?.locations) return [];
  return data.locations.map((l: any) => ({
    locationId: l.locationId ?? "",
    locationName: l.locationName ?? "Unknown",
    providerId: l.providerId ?? "",
    type: l.type ?? "",
    postalCode: l.postalAddressPostcode ?? null,
    region: l.region ?? null,
    registrationStatus: l.registrationStatus ?? "",
  }));
}
