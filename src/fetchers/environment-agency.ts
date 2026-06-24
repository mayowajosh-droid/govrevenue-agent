const BASE = "https://environment.data.gov.uk/flood-monitoring";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type FloodWarning = {
  id: string;
  description: string;
  severity: string;
  severityLevel: number;
  timeRaised: string;
  area: string;
  county: string | null;
  isTidal: boolean;
};

export type FloodStation = {
  id: string;
  label: string;
  lat: number | null;
  lon: number | null;
  riverName: string | null;
  catchmentName: string | null;
  stationType: string | null;
  latestValue: number | null;
};

export async function fetchFloodWarnings(): Promise<FloodWarning[]> {
  try {
    const ac = makeAbort();
    const res = await fetch(`${BASE}/id/floods?_limit=100`, {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data = await res.json() as { items?: {
      id?: string;
      description?: string;
      severity?: string;
      severityLevel?: number;
      timeRaised?: string;
      floodAreaID?: string;
      floodArea?: { county?: string };
      isTidal?: boolean;
    }[] };
    return (data.items ?? []).map(w => ({
      id: String(w.id || w.floodAreaID || ""),
      description: String(w.description || ""),
      severity: String(w.severity || ""),
      severityLevel: w.severityLevel ?? 0,
      timeRaised: String(w.timeRaised || ""),
      area: String(w.floodAreaID || ""),
      county: w.floodArea?.county ?? null,
      isTidal: w.isTidal ?? false,
    }));
  } catch { return []; }
}

export async function fetchActiveFloodWarningCount(): Promise<number> {
  try {
    const ac = makeAbort();
    const res = await fetch(`${BASE}/id/floods?_limit=1`, {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return 0;
    const data = await res.json() as { meta?: { totalResults?: number } };
    return data.meta?.totalResults ?? 0;
  } catch { return 0; }
}

export async function fetchFloodStations(limit = 20): Promise<FloodStation[]> {
  try {
    const ac = makeAbort();
    const res = await fetch(`${BASE}/id/stations?_limit=${limit}`, {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data = await res.json() as { items?: {
      id?: string;
      label?: string;
      lat?: number;
      long?: number;
      riverName?: string;
      catchmentName?: string;
      stationType?: string;
      latestReading?: { value?: number };
    }[] };
    return (data.items ?? []).map(s => ({
      id: String(s.id || ""),
      label: String(s.label || ""),
      lat: s.lat ?? null,
      lon: s.long ?? null,
      riverName: s.riverName ?? null,
      catchmentName: s.catchmentName ?? null,
      stationType: s.stationType ?? null,
      latestValue: s.latestReading?.value ?? null,
    }));
  } catch { return []; }
}

export async function fetchFloodStationsInArea(lat: number, lon: number, radiusKm = 10): Promise<FloodStation[]> {
  try {
    const ac = makeAbort();
    const res = await fetch(
      `${BASE}/id/stations?lat=${lat}&long=${lon}&dist=${radiusKm}&_limit=20`,
      { headers: { Accept: "application/json" }, signal: ac.signal }
    );
    if (!res.ok) return [];
    const data = await res.json() as { items?: {
      id?: string; label?: string; lat?: number; long?: number;
      riverName?: string; catchmentName?: string; stationType?: string;
      latestReading?: { value?: number };
    }[] };
    return (data.items ?? []).map(s => ({
      id: String(s.id || ""),
      label: String(s.label || ""),
      lat: s.lat ?? null,
      lon: s.long ?? null,
      riverName: s.riverName ?? null,
      catchmentName: s.catchmentName ?? null,
      stationType: s.stationType ?? null,
      latestValue: s.latestReading?.value ?? null,
    }));
  } catch { return []; }
}
