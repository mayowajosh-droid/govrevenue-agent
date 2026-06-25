const LAND_REG_CSV =
  "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv";
const TIMEOUT_MS = 45_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type LandRegistryCountyStat = {
  county: string;
  transactionCount: number;
  avgPrice: number;
  medianPrice: number;
  newBuildCount: number;
  period: string;
  propertyTypeBreakdown: Record<string, number>;
};

export type LandRegistrySnapshot = {
  fetchedAt: string;
  totalTransactions: number;
  periodCovered: string[];
  byCounty: LandRegistryCountyStat[];
  ukAvgPrice: number;
};

function median(arr: number[]): number {
  if (!arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

/**
 * Parse Land Registry monthly price paid CSV.
 * Columns: transaction_id, price, date, postcode, property_type (D/S/T/F/O),
 *          new_build (Y/N), tenure, PAON, SAON, street, locality, town, district, county, category, status
 */
export async function fetchLandRegistryTransactions(): Promise<LandRegistrySnapshot> {
  const empty: LandRegistrySnapshot = {
    fetchedAt: new Date().toISOString(),
    totalTransactions: 0,
    periodCovered: [],
    byCounty: [],
    ukAvgPrice: 0,
  };
  try {
    const ac = makeAbort();
    const res = await fetch(LAND_REG_CSV, { signal: ac.signal });
    if (!res.ok) return empty;
    const text = await res.text();
    const lines = text.trim().split("\n");

    const countyPrices: Record<string, number[]> = {};
    const countyNewBuilds: Record<string, number> = {};
    const countyTypes: Record<string, Record<string, number>> = {};
    const periodsSet = new Set<string>();
    const allPrices: number[] = [];

    // Find the 3 most recent months present in the data
    const allDates = new Set<string>();
    for (const line of lines) {
      const cols = line.split(",").map(c => c.replace(/^"|"$/g, "").trim());
      if (cols.length < 3) continue;
      const d = cols[2]?.slice(0, 7);
      if (d && /^\d{4}-\d{2}$/.test(d)) allDates.add(d);
    }
    const recentMonths = new Set([...allDates].sort().slice(-3));

    for (const line of lines) {
      // Simple CSV parse — Land Registry values never contain commas
      const cols = line.split(",").map(c => c.replace(/^"|"$/g, "").trim());
      if (cols.length < 14) continue;
      const price = parseInt(cols[1], 10);
      const date = cols[2]?.slice(0, 7);
      const propType = cols[4];
      const newBuild = cols[5] === "Y";
      const county = cols[13];
      if (!county || !date || isNaN(price) || price <= 0) continue;
      if (!recentMonths.has(date)) continue; // Only recent 3 months

      periodsSet.add(date);
      countyPrices[county] ??= [];
      countyPrices[county].push(price);
      allPrices.push(price);
      if (newBuild) countyNewBuilds[county] = (countyNewBuilds[county] ?? 0) + 1;
      countyTypes[county] ??= {};
      countyTypes[county][propType] = (countyTypes[county][propType] ?? 0) + 1;
    }

    const byCounty: LandRegistryCountyStat[] = Object.entries(countyPrices)
      .map(([county, prices]) => ({
        county,
        transactionCount: prices.length,
        avgPrice: Math.round(prices.reduce((s, p) => s + p, 0) / prices.length),
        medianPrice: Math.round(median(prices)),
        newBuildCount: countyNewBuilds[county] ?? 0,
        period: [...periodsSet].sort().at(-1) ?? "",
        propertyTypeBreakdown: countyTypes[county] ?? {},
      }))
      .sort((a, b) => b.transactionCount - a.transactionCount);

    return {
      fetchedAt: new Date().toISOString(),
      totalTransactions: allPrices.length,
      periodCovered: [...periodsSet].sort(),
      byCounty,
      ukAvgPrice: allPrices.length
        ? Math.round(allPrices.reduce((s, p) => s + p, 0) / allPrices.length)
        : 0,
    };
  } catch { return empty; }
}
