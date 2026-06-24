const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type RailStationUsage = {
  stationName: string;
  stationCode: string;
  totalEntries: number;
  totalExits: number;
  year: string;
};

// ORR data portal does not expose a stable public JSON API for station usage.
// This stub attempts the endpoint and returns [] if unavailable.
export async function fetchStationUsage(): Promise<RailStationUsage[]> {
  try {
    const ac = makeAbort();
    const res = await fetch("https://dataportal.orr.gov.uk/api/station-usage", {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data: any = await res.json();
    const items: any[] = Array.isArray(data) ? data : Array.isArray(data.results) ? data.results : [];
    return items.map((r: any) => ({
      stationName: String(r.stationName ?? r.station_name ?? ""),
      stationCode: String(r.stationCode ?? r.station_code ?? ""),
      totalEntries: Number(r.totalEntries ?? r.total_entries ?? 0),
      totalExits: Number(r.totalExits ?? r.total_exits ?? 0),
      year: String(r.year ?? ""),
    }));
  } catch {
    return [];
  }
}
