const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type TflLineStatus = {
  id: string;
  name: string;
  modeName: string;
  lineStatuses: { statusSeverity: number; statusSeverityDescription: string; reason: string | null }[];
};

export type TflStopPoint = {
  id: string;
  commonName: string;
  lat: number;
  lon: number;
  modes: string[];
};

export async function fetchLineStatuses(): Promise<TflLineStatus[]> {
  try {
    const ac = makeAbort();
    const res = await fetch(
      "https://api.tfl.gov.uk/Line/Mode/tube,dlr,overground,elizabeth-line/Status",
      { headers: { Accept: "application/json" }, signal: ac.signal },
    );
    if (!res.ok) return [];
    const data: any[] = await res.json();
    return data.map((line: any) => ({
      id: String(line.id ?? ""),
      name: String(line.name ?? ""),
      modeName: String(line.modeName ?? ""),
      lineStatuses: Array.isArray(line.lineStatuses)
        ? line.lineStatuses.map((s: any) => ({
            statusSeverity: Number(s.statusSeverity ?? 0),
            statusSeverityDescription: String(s.statusSeverityDescription ?? ""),
            reason: s.reason ? String(s.reason) : null,
          }))
        : [],
    }));
  } catch {
    return [];
  }
}

export async function searchStopPoints(query: string): Promise<TflStopPoint[]> {
  try {
    const ac = makeAbort();
    const res = await fetch(
      `https://api.tfl.gov.uk/StopPoint/Search/${encodeURIComponent(query)}`,
      { headers: { Accept: "application/json" }, signal: ac.signal },
    );
    if (!res.ok) return [];
    const data: any = await res.json();
    const matches: any[] = Array.isArray(data.matches) ? data.matches : [];
    return matches.map((sp: any) => ({
      id: String(sp.id ?? ""),
      commonName: String(sp.name ?? sp.commonName ?? ""),
      lat: Number(sp.lat ?? 0),
      lon: Number(sp.lon ?? 0),
      modes: Array.isArray(sp.modes) ? sp.modes.map(String) : [],
    }));
  } catch {
    return [];
  }
}
