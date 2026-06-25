const FSA_BASE = "https://api.ratings.food.gov.uk";
const TIMEOUT_MS = 20_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

// Major UK cities with FSA Local Authority IDs
const CITY_LA_MAP: Record<string, number> = {
  Birmingham: 374,
  Manchester: 180,
  Leeds: 397,
  Bristol: 324,
  Liverpool: 179,
  Sheffield: 399,
  "Newcastle upon Tyne": 122,
  "London (Westminster)": 120,
  Nottingham: 87,
  Leicester: 85,
  Cardiff: 339,
  Edinburgh: 210,
  Glasgow: 213,
};

// FSA Business Type IDs
const BUSINESS_TYPES: Record<string, number> = {
  Restaurant: 1,
  "Takeaway/sandwich shop": 7,
  Pub: 5,
  Cafe: 14,
  "Retailers - other": 4,
  "Retailers - supermarkets/hypermarkets": 0, // all types when 0
};

export type FsaCityStat = {
  city: string;
  localAuthorityId: number;
  totalEstablishments: number;
  restaurants: number;
  takeaways: number;
  pubs: number;
  cafes: number;
  recentlyRated: number;
};

export type FsaSnapshot = {
  fetchedAt: string;
  byCityStats: FsaCityStat[];
  totalAcrossCities: number;
};

async function fetchCityCount(laId: number, businessTypeId?: number): Promise<number> {
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({
      localAuthorityId: String(laId),
      pageSize: "1",
      pageNumber: "1",
      ...(businessTypeId ? { BusinessTypeId: String(businessTypeId) } : {}),
    });
    const res = await fetch(`${FSA_BASE}/Establishments?${params}`, {
      headers: { "x-api-version": "2", Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return 0;
    const data = await res.json() as { meta?: { totalCount?: number } };
    return data.meta?.totalCount ?? 0;
  } catch { return 0; }
}

/** FSA food business counts by city — total + by business type. No auth needed. */
export async function fetchFsaFoodBusinesses(): Promise<FsaSnapshot> {
  const empty: FsaSnapshot = {
    fetchedAt: new Date().toISOString(),
    byCityStats: [],
    totalAcrossCities: 0,
  };

  try {
    // Process cities sequentially to avoid FSA API rate limits
    const cityEntries = Object.entries(CITY_LA_MAP);
    const cityStats: FsaCityStat[] = [];
    for (const [city, laId] of cityEntries) {
      const [total, restaurants, takeaways, pubs, cafes] = await Promise.all([
        fetchCityCount(laId),
        fetchCityCount(laId, BUSINESS_TYPES.Restaurant),
        fetchCityCount(laId, BUSINESS_TYPES["Takeaway/sandwich shop"]),
        fetchCityCount(laId, BUSINESS_TYPES.Pub),
        fetchCityCount(laId, BUSINESS_TYPES.Cafe),
      ]);
      cityStats.push({
        city,
        localAuthorityId: laId,
        totalEstablishments: total,
        restaurants,
        takeaways,
        pubs,
        cafes,
        recentlyRated: 0,
      });
      await new Promise(r => setTimeout(r, 400));
    }

    const sorted = cityStats.sort((a, b) => b.totalEstablishments - a.totalEstablishments);
    const totalAcrossCities = sorted.reduce((s, c) => s + c.totalEstablishments, 0);

    return {
      fetchedAt: new Date().toISOString(),
      byCityStats: sorted,
      totalAcrossCities,
    };
  } catch { return empty; }
}

/** Single city lookup — fast path for on-demand queries. */
export async function fetchFsaForCity(city: string): Promise<FsaCityStat | null> {
  const laId = CITY_LA_MAP[city];
  if (!laId) return null;
  const total = await fetchCityCount(laId);
  const [restaurants, takeaways, pubs, cafes] = await Promise.all([
    fetchCityCount(laId, BUSINESS_TYPES.Restaurant),
    fetchCityCount(laId, BUSINESS_TYPES["Takeaway/sandwich shop"]),
    fetchCityCount(laId, BUSINESS_TYPES.Pub),
    fetchCityCount(laId, BUSINESS_TYPES.Cafe),
  ]);
  return { city, localAuthorityId: laId, totalEstablishments: total, restaurants, takeaways, pubs, cafes, recentlyRated: 0 };
}
