const ONS_API = "https://api.beta.ons.gov.uk/v1";
const ONS_DOWNLOAD = "https://download.ons.gov.uk/downloads/datasets";
const TIMEOUT_MS = 45_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type SpendingCategory = {
  category: string;
  displayName: string;
  latestIndexValue: number;
  prevYearIndexValue: number;
  changePercent: number;
  latestYear: string;
};

export type OnsCardSpendingSnapshot = {
  fetchedAt: string;
  datasetVersion: number;
  categories: SpendingCategory[];
  aggregateChangePercent: number;
};

const CATEGORY_LABELS: Record<string, string> = {
  aggregate: "Total Consumer Spending",
  social: "Social & Entertainment",
  delayable: "Discretionary (Durables)",
  staple: "Essential Spending",
  "work-related": "Work-Related Spending",
};

async function getLatestVersion(): Promise<{ version: number; csvUrl: string } | null> {
  try {
    const ac = makeAbort();
    const res = await fetch(
      `${ONS_API}/datasets/uk-spending-on-cards/editions/time-series/versions?limit=1`,
      { signal: ac.signal },
    );
    if (!res.ok) return null;
    const data = await res.json() as { items?: { links?: { self?: { href?: string } }; version?: number }[] };
    const latest = data.items?.[0];
    if (!latest) return null;
    const version = latest.version ?? 0;
    if (!version) return null;
    const csvUrl = `${ONS_DOWNLOAD}/uk-spending-on-cards/editions/time-series/versions/${version}.csv`;
    return { version, csvUrl };
  } catch { return null; }
}

/** ONS UK Card Spending — weekly spending index by category, with YoY change. No auth needed. */
export async function fetchOnsCardSpending(): Promise<OnsCardSpendingSnapshot> {
  const empty: OnsCardSpendingSnapshot = {
    fetchedAt: new Date().toISOString(),
    datasetVersion: 0,
    categories: [],
    aggregateChangePercent: 0,
  };
  try {
    const meta = await getLatestVersion();
    if (!meta) return empty;

    const ac = makeAbort();
    const res = await fetch(meta.csvUrl, { signal: ac.signal });
    if (!res.ok) return empty;
    const text = await res.text();
    const lines = text.trim().split("\n");
    if (lines.length < 2) return empty;

    // Parse header to find column indices
    const header = lines[0].split(",").map(h => h.replace(/"/g, "").trim());
    const iVal = header.indexOf("v4_1");
    const iYear = header.findIndex(h => h === "Time" || h === "calendar-years");
    const iCat = header.findIndex(h => h === "Category" || h === "spend-category");
    if (iVal < 0 || iYear < 0 || iCat < 0) return empty;

    // Aggregate index values by (category, year)
    const catYearValues: Record<string, Record<string, number[]>> = {};

    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i].split(",").map(c => c.replace(/"/g, "").trim());
      const val = parseFloat(cols[iVal]);
      const year = cols[iYear]?.trim();
      const cat = cols[iCat]?.toLowerCase().trim();
      if (!cat || !year || isNaN(val)) continue;
      catYearValues[cat] ??= {};
      catYearValues[cat][year] ??= [];
      catYearValues[cat][year].push(val);
    }

    // Parse again with day-month to compare same weeks year-over-year
    const catYearDayValues: Record<string, Record<string, Record<string, number>>> = {};
    const header2 = lines[0].split(",").map(h => h.replace(/"/g, "").trim());
    const iDayMon = header2.findIndex(h => h === "DayMonth" || h === "dd-mm");

    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i].split(",").map(c => c.replace(/"/g, "").trim());
      const val = parseFloat(cols[iVal]);
      const year = cols[iYear]?.trim();
      const cat = cols[iCat]?.toLowerCase().trim();
      const dayMon = iDayMon >= 0 ? cols[iDayMon]?.trim() : "";
      if (!cat || !year || !dayMon || isNaN(val)) continue;
      catYearDayValues[cat] ??= {};
      catYearDayValues[cat][year] ??= {};
      catYearDayValues[cat][year][dayMon] = val;
    }

    const allYears = new Set<string>();
    for (const cat of Object.values(catYearValues)) {
      for (const y of Object.keys(cat)) allYears.add(y);
    }
    const sortedYears = [...allYears].filter(Boolean).sort();
    const latestYear = sortedYears.at(-1) ?? "";
    const prevYear = sortedYears.at(-2) ?? "";

    const categories: SpendingCategory[] = [];
    for (const [cat, yearMap] of Object.entries(catYearValues)) {
      // Compare same day-month keys across years to avoid partial-year bias
      const latestDays = catYearDayValues[cat]?.[latestYear] ?? {};
      const prevDays = catYearDayValues[cat]?.[prevYear] ?? {};
      const sharedDays = Object.keys(latestDays).filter(d => d in prevDays);

      let latestIdx: number;
      let prevIdx: number;

      if (sharedDays.length >= 4) {
        const avg = (vals: number[]) => vals.reduce((s, v) => s + v, 0) / vals.length;
        latestIdx = avg(sharedDays.map(d => latestDays[d]));
        prevIdx = avg(sharedDays.map(d => prevDays[d]));
      } else {
        const allVals = yearMap[latestYear] ?? [];
        const prevVals = yearMap[prevYear] ?? [];
        if (!allVals.length || !prevVals.length) continue;
        const avg = (arr: number[]) => arr.reduce((s, v) => s + v, 0) / arr.length;
        latestIdx = avg(allVals);
        prevIdx = avg(prevVals);
      }

      const changePercent = prevIdx ? ((latestIdx - prevIdx) / prevIdx) * 100 : 0;
      categories.push({
        category: cat,
        displayName: CATEGORY_LABELS[cat] ?? cat,
        latestIndexValue: Math.round(latestIdx * 10) / 10,
        prevYearIndexValue: Math.round(prevIdx * 10) / 10,
        changePercent: Math.round(changePercent * 10) / 10,
        latestYear,
      });
    }

    const aggCat = categories.find(c => c.category === "aggregate");
    return {
      fetchedAt: new Date().toISOString(),
      datasetVersion: meta.version,
      categories,
      aggregateChangePercent: aggCat?.changePercent ?? 0,
    };
  } catch { return empty; }
}
