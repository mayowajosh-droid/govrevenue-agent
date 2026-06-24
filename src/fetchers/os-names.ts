const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type OsNameResult = {
  name: string;
  type: string;
  localType: string;
  county: string | null;
  district: string | null;
  region: string | null;
  country: string;
  coordinates: { lat: number; lon: number };
};

export async function searchOsNames(query: string, limit = 25): Promise<OsNameResult[]> {
  const key = process.env.OS_API_KEY;
  if (!key) return [];
  try {
    const ac = makeAbort();
    const url = `https://api.os.uk/search/names/v1/find?query=${encodeURIComponent(query)}&maxresults=${limit}&key=${encodeURIComponent(key)}`;
    const res = await fetch(url, {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data: any = await res.json();
    const results: any[] = Array.isArray(data.results) ? data.results : [];
    return results.map((r: any) => {
      const g = r.GAZETTEER_ENTRY ?? r;
      return {
        name: String(g.NAME1 ?? ""),
        type: String(g.TYPE ?? ""),
        localType: String(g.LOCAL_TYPE ?? ""),
        county: g.COUNTY_UNITARY ? String(g.COUNTY_UNITARY) : null,
        district: g.DISTRICT_BOROUGH ? String(g.DISTRICT_BOROUGH) : null,
        region: g.REGION ? String(g.REGION) : null,
        country: String(g.COUNTRY ?? ""),
        coordinates: {
          lat: Number(g.GEOMETRY_Y ?? g.MBR_YMIN ?? 0),
          lon: Number(g.GEOMETRY_X ?? g.MBR_XMIN ?? 0),
        },
      };
    });
  } catch {
    return [];
  }
}
