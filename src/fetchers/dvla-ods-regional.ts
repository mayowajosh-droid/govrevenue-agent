import * as XLSX from "xlsx";

const ODS_URL =
  "https://assets.publishing.service.gov.uk/media/69ef3553ed93f72cf81633fc/veh0105.ods";
const TIMEOUT_MS = 60_000;

// English regions + devolved nations ONS codes
const REGION_CODES = new Set([
  "E12000001", // North East
  "E12000002", // North West
  "E12000003", // Yorkshire and The Humber
  "E12000004", // East Midlands
  "E12000005", // West Midlands
  "E12000006", // East of England
  "E12000007", // London
  "E12000008", // South East
  "E12000009", // South West
  "W92000004", // Wales
  "S92000003", // Scotland
  "N92000002", // Northern Ireland
]);

export type DvlaRegionStat = {
  region: string;
  onsCode: string;
  totalCars: number;
  dieselCars: number;
  petrolCars: number;
  hybridCars: number;
  otherFuelCars: number;
  companyCars: number;
  privateCars: number;
  quarter: string;
};

export type DvlaOdsSnapshot = {
  fetchedAt: string;
  latestQuarter: string;
  byRegion: DvlaRegionStat[];
  ukTotal: number;
  topRegion: string;
};

/** Parse DVLA VEH0105 ODS — licensed cars by region, fuel type, keepership. No auth needed. */
export async function fetchDvlaRegionalStats(): Promise<DvlaOdsSnapshot> {
  const empty: DvlaOdsSnapshot = {
    fetchedAt: new Date().toISOString(),
    latestQuarter: "",
    byRegion: [],
    ukTotal: 0,
    topRegion: "",
  };

  try {
    const ac = new AbortController();
    setTimeout(() => ac.abort(), TIMEOUT_MS);
    const res = await fetch(ODS_URL, { signal: ac.signal });
    if (!res.ok) return empty;

    const buf = Buffer.from(await res.arrayBuffer());
    const wb = XLSX.read(buf, { type: "buffer" });
    const ws = wb.Sheets["VEH0105"];
    if (!ws) return empty;

    const rows = XLSX.utils.sheet_to_json<(string | number)[]>(ws, {
      header: 1,
      defval: "",
    });

    // Row 4 is header: Units, BodyType, Fuel, Keepership, ONSSort, ONSCode, ONSGeo, 2025Q4, ...
    const header = rows[4] as string[];
    const latestQuarter = String(header[7] ?? "").trim(); // e.g. "2025 Q4"

    // Accumulate by region+keepership+fuel
    const acc: Record<string, Record<string, Record<string, number>>> = {};
    // acc[onsCode][keepership][fuel] = thousands

    for (const row of rows.slice(5)) {
      const body = String(row[1] ?? "").trim();
      const fuel = String(row[2] ?? "").trim();
      const keep = String(row[3] ?? "").trim();
      const code = String(row[5] ?? "").trim();
      const val = parseFloat(String(row[7] ?? ""));

      if (body !== "Cars" || !REGION_CODES.has(code) || isNaN(val) || fuel === "Total" || keep === "Total") continue;

      acc[code] ??= {};
      acc[code][keep] ??= {};
      acc[code][keep][fuel] = (acc[code][keep][fuel] ?? 0) + val;
    }

    // Also get geo names
    const geoNames: Record<string, string> = {};
    for (const row of rows.slice(5)) {
      const code = String(row[5] ?? "").trim();
      const geo = String(row[6] ?? "").trim();
      if (REGION_CODES.has(code) && geo) geoNames[code] = geo;
    }

    const byRegion: DvlaRegionStat[] = Object.entries(acc).map(([code, keepMap]) => {
      const priv = keepMap["PRIVATE"] ?? {};
      const comp = keepMap["COMPANY"] ?? {};
      const allFuels = (k: Record<string, number>) =>
        Object.values(k).reduce((s, v) => s + v, 0);

      const privateCars = Math.round(allFuels(priv) * 1000);
      const companyCars = Math.round(allFuels(comp) * 1000);

      return {
        region: (geoNames[code] ?? code).trim(),
        onsCode: code,
        totalCars: privateCars + companyCars,
        dieselCars: Math.round(((priv["DIESEL"] ?? 0) + (comp["DIESEL"] ?? 0)) * 1000),
        petrolCars: Math.round(((priv["PETROL"] ?? 0) + (comp["PETROL"] ?? 0)) * 1000),
        hybridCars: Math.round(((priv["HYBRID ELECTRIC (PETROL)"] ?? 0) + (comp["HYBRID ELECTRIC (PETROL)"] ?? 0)) * 1000),
        otherFuelCars: Math.round(((priv["OTHER FUEL TYPES"] ?? 0) + (comp["OTHER FUEL TYPES"] ?? 0)) * 1000),
        companyCars,
        privateCars,
        quarter: latestQuarter,
      };
    }).sort((a, b) => b.totalCars - a.totalCars);

    const ukTotal = byRegion.reduce((s, r) => s + r.totalCars, 0);

    return {
      fetchedAt: new Date().toISOString(),
      latestQuarter,
      byRegion,
      ukTotal,
      topRegion: byRegion[0]?.region ?? "",
    };
  } catch { return empty; }
}
