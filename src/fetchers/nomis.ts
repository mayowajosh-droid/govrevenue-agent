const BASE = "https://www.nomisweb.co.uk/api/v01/dataset";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type NomisDataPoint = {
  date: string;
  geography: string;
  geographyCode: string;
  measure: string;
  value: number;
};

function parseRows(data: unknown): NomisDataPoint[] {
  if (!data || typeof data !== "object") return [];
  const obs = (data as Record<string, unknown>).obs;
  if (!Array.isArray(obs)) return [];
  return obs
    .map((row: Record<string, unknown>) => {
      const val = Number(row.obs_value ?? row.OBS_VALUE ?? NaN);
      if (Number.isNaN(val)) return null;
      return {
        date: String(row.date_name ?? row.DATE_NAME ?? ""),
        geography: String(row.geography_name ?? row.GEOGRAPHY_NAME ?? ""),
        geographyCode: String(row.geography_code ?? row.GEOGRAPHY_CODE ?? ""),
        measure: String(
          row.industry_name ??
          row.INDUSTRY_NAME ??
          row.measures_name ??
          row.MEASURES_NAME ??
          ""
        ),
        value: val,
      } satisfies NomisDataPoint;
    })
    .filter((r): r is NomisDataPoint => r !== null);
}

/**
 * Business Register Employment Survey — employees by SIC sector.
 * Dataset NM_17_5.
 */
export async function fetchEmploymentBySector(
  sectorSic: string,
  geographyCode?: string
): Promise<NomisDataPoint[]> {
  try {
    const ac = makeAbort();
    const geo = geographyCode ? encodeURIComponent(geographyCode) : "TYPE499";
    const industry = encodeURIComponent(sectorSic);
    const url =
      `${BASE}/NM_17_5.data.json` +
      `?geography=${geo}` +
      `&industry=${industry}` +
      `&date=latest` +
      `&select=date_name,geography_name,geography_code,industry_name,obs_value`;
    const res = await fetch(url, {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data: unknown = await res.json();
    return parseRows(data);
  } catch {
    return [];
  }
}

/**
 * Business demography — births / deaths by area.
 * Dataset NM_142_1.
 */
export async function fetchBusinessDemographyByArea(
  geographyCode?: string
): Promise<NomisDataPoint[]> {
  try {
    const ac = makeAbort();
    const geo = geographyCode ? encodeURIComponent(geographyCode) : "TYPE464";
    const url =
      `${BASE}/NM_142_1.data.json` +
      `?geography=${geo}` +
      `&date=latest` +
      `&measures=20100` +
      `&select=date_name,geography_name,geography_code,measures_name,obs_value`;
    const res = await fetch(url, {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data: unknown = await res.json();
    return parseRows(data);
  } catch {
    return [];
  }
}

/**
 * UK Business Count by area.
 * Dataset NM_189_1.
 */
export async function fetchBusinessCountByArea(): Promise<NomisDataPoint[]> {
  try {
    const ac = makeAbort();
    const url =
      `${BASE}/NM_189_1.data.json` +
      `?geography=TYPE464` +
      `&date=latest` +
      `&measures=20100` +
      `&select=date_name,geography_name,geography_code,measures_name,obs_value`;
    const res = await fetch(url, {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data: unknown = await res.json();
    return parseRows(data);
  } catch {
    return [];
  }
}
