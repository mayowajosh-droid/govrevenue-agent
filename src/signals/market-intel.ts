import type { Pool } from "pg";

// ── Types ──────────────────────────────────────────────────────────────────────

export type MarketSignalSector =
  | "automotive" | "property" | "construction" | "finance"
  | "retail" | "tech" | "energy" | "health" | "education"
  | "professional_services" | "food_beverage" | "transport" | "general";

export type MarketSignal = {
  id: string;
  source: string;         // "DVLA", "LAND REG", "ONS"
  stat: string;           // the core fact/number
  geography: string;      // "UK-wide", "Greater London"
  period: string;         // "Q1 2025", "Feb–Apr 2026"
  implication: string;    // commercial hook
  sectors: MarketSignalSector[];
  value?: number;         // raw numeric value for sorting
  changePercent?: number; // YoY or period change
  sourceUrl?: string;
  fetchedAt: string;
  formatted: string;      // "SOURCE · STAT · GEOGRAPHY · PERIOD · IMPLICATION →"
};

export type MarketIntelSnapshot = {
  generatedAt: string;
  signals: MarketSignal[];
  sourcesCovered: string[];
  totalSignals: number;
};

// ── Helpers ────────────────────────────────────────────────────────────────────

function fmt(n: number): string {
  return n.toLocaleString("en-GB");
}

function fmtGbp(n: number): string {
  if (n >= 1_000_000_000) return `£${(n / 1_000_000_000).toFixed(1)}bn`;
  if (n >= 1_000_000) return `£${Math.round(n / 1_000_000).toLocaleString("en-GB")}m`;
  if (n >= 1_000) return `£${Math.round(n / 1_000).toLocaleString("en-GB")}k`;
  return `£${n}`;
}

function fmtPct(n: number): string {
  const sign = n >= 0 ? "+" : "";
  return `${sign}${n.toFixed(1)}%`;
}

function signal(
  id: string,
  source: string,
  stat: string,
  geography: string,
  period: string,
  implication: string,
  sectors: MarketSignalSector[],
  extra: { value?: number; changePercent?: number; sourceUrl?: string; fetchedAt?: string } = {},
): MarketSignal {
  const fetchedAt = extra.fetchedAt ?? new Date().toISOString();
  const formatted = `${source}  ·  ${stat}  ·  ${geography}  ·  ${period}  ·  ${implication}  →`;
  return { id, source, stat, geography, period, implication, sectors, formatted, fetchedAt, ...extra };
}

async function getLatestPayload<T>(pool: Pool, source: string): Promise<T | null> {
  try {
    const r = await pool.query<{ raw_payload: unknown; fetched_at: string }>(
      `SELECT raw_payload, fetched_at FROM canonical_ingest
       WHERE source = $1 ORDER BY fetched_at DESC LIMIT 1`,
      [source],
    );
    if (!r.rows[0]) return null;
    const p = r.rows[0].raw_payload;
    return (typeof p === "string" ? JSON.parse(p) : p) as T;
  } catch { return null; }
}

async function getLatestPayloads<T>(pool: Pool, source: string, limit = 20): Promise<{ payload: T; fetchedAt: string }[]> {
  try {
    const r = await pool.query<{ raw_payload: unknown; fetched_at: string }>(
      `SELECT raw_payload, fetched_at FROM canonical_ingest
       WHERE source = $1 ORDER BY fetched_at DESC LIMIT $2`,
      [source, limit],
    );
    return r.rows.map(row => ({
      payload: (typeof row.raw_payload === "string" ? JSON.parse(row.raw_payload) : row.raw_payload) as T,
      fetchedAt: row.fetched_at,
    }));
  } catch { return []; }
}

// ── Signal Generators ──────────────────────────────────────────────────────────

async function dvlaVehicleSignals(pool: Pool): Promise<MarketSignal[]> {
  type DvlaStats = {
    fetchedAt: string; referenceYear: string;
    newFirstRegistrations: number; zeroEmissionRegistrations: number;
    zeroEmissionOnRoad: number; newFirstRegistrationsChangePercent: number;
    zeroEmissionChangePercent: number; totalLicensedVehiclesMillions: number;
    sourceUrl?: string;
  };

  const data = await getLatestPayload<DvlaStats>(pool, "dvla_vehicle_stats");
  if (!data || !data.newFirstRegistrations) return [];

  const signals: MarketSignal[] = [];
  const year = data.referenceYear || new Date().getFullYear().toString();
  const url = data.sourceUrl ?? "https://www.gov.uk/government/collections/vehicles-statistics";

  if (data.zeroEmissionRegistrations) {
    const evPct = data.zeroEmissionChangePercent ? ` · ${fmtPct(data.zeroEmissionChangePercent)} YoY` : "";
    signals.push(signal(
      "dvla-ev-regs", "DVLA",
      `${fmt(data.zeroEmissionRegistrations)} zero-emission vehicles registered`,
      "UK-wide", year,
      `EV cabins are odourless by design — in-car scent, air quality & accessories become the differentiator${evPct}`,
      ["automotive", "energy", "retail"],
      { value: data.zeroEmissionRegistrations, changePercent: data.zeroEmissionChangePercent, sourceUrl: url, fetchedAt: data.fetchedAt },
    ));
  }

  if (data.newFirstRegistrations) {
    const regPct = data.newFirstRegistrationsChangePercent ? ` ${fmtPct(data.newFirstRegistrationsChangePercent)} YoY` : "";
    signals.push(signal(
      "dvla-new-regs", "DVLA",
      `${fmt(data.newFirstRegistrations)} new vehicle registrations`,
      "UK-wide", year,
      `Every new owner is a procurement event — insurance, accessories, servicing & finance within 90 days${regPct}`,
      ["automotive", "retail", "finance"],
      { value: data.newFirstRegistrations, changePercent: data.newFirstRegistrationsChangePercent, sourceUrl: url, fetchedAt: data.fetchedAt },
    ));
  }

  if (data.totalLicensedVehiclesMillions) {
    signals.push(signal(
      "dvla-total-licensed", "DVLA",
      `${data.totalLicensedVehiclesMillions.toFixed(1)}m licensed vehicles on UK roads`,
      "UK-wide", year,
      "Fleet, servicing & accessories market is structurally large — recurring revenue at scale",
      ["automotive", "transport"],
      { value: data.totalLicensedVehiclesMillions * 1_000_000, sourceUrl: url, fetchedAt: data.fetchedAt },
    ));
  }

  if (data.zeroEmissionOnRoad) {
    signals.push(signal(
      "dvla-ev-on-road", "DVLA",
      `${fmt(data.zeroEmissionOnRoad)} EVs on UK roads`,
      "UK-wide", year,
      "Each EV owner is an active prospect for charging infrastructure, energy tariffs & in-car experience products",
      ["automotive", "energy"],
      { value: data.zeroEmissionOnRoad, sourceUrl: url, fetchedAt: data.fetchedAt },
    ));
  }

  return signals;
}

async function dvlaRegionalSignals(pool: Pool): Promise<MarketSignal[]> {
  type DvlaRegion = {
    region: string; onsCode: string; totalCars: number;
    dieselCars: number; petrolCars: number; hybridCars: number;
    companyCars: number; privateCars: number; quarter: string;
  };

  const rows = await getLatestPayloads<DvlaRegion>(pool, "dvla_ods_regional", 12);
  if (!rows.length) return [];

  const signals: MarketSignal[] = [];
  const quarter = rows[0]?.payload?.quarter || "";

  // Top 3 regions by total cars
  const regions = rows
    .map(r => r.payload)
    .filter(r => r.totalCars > 0)
    .sort((a, b) => b.totalCars - a.totalCars)
    .slice(0, 3);

  for (const r of regions) {
    const companyShare = r.totalCars ? Math.round((r.companyCars / r.totalCars) * 100) : 0;
    const retailShare = 100 - companyShare;
    signals.push(signal(
      `dvla-region-${r.onsCode}`, "DVLA",
      `${fmt(r.totalCars)} cars on the road — ${retailShare}% private/retail`,
      r.region, quarter,
      `One of the UK's largest car-owning markets — the addressable base for any vehicle product, accessory or service. ${retailShare}% are privately owned (retail/consumer); ${companyShare}% fleet/company (B2B)`,
      ["automotive", "transport", "retail"],
      { value: r.totalCars, changePercent: undefined, sourceUrl: "https://www.gov.uk/government/statistical-data-sets/vehicle-licensing-statistics-data-files", fetchedAt: rows[0].fetchedAt },
    ));
  }

  return signals;
}

async function landRegistrySignals(pool: Pool): Promise<MarketSignal[]> {
  type LandReg = {
    fetchedAt: string; totalTransactions: number; periodCovered: string[];
    ukAvgPrice: number;
    byCounty: { county: string; transactionCount: number; avgPrice: number; newBuildCount: number; medianPrice: number }[];
  };

  const data = await getLatestPayload<LandReg>(pool, "land_registry_transactions");
  if (!data || !data.byCounty?.length) return [];

  const signals: MarketSignal[] = [];
  const url = "https://landregistry.data.gov.uk/app/ppd";
  const periods = data.periodCovered ?? [];
  const period = periods.length >= 2
    ? `${new Date(periods[0] + "-01").toLocaleDateString("en-GB", { month: "short", year: "numeric" })}–${new Date(periods[periods.length - 1] + "-01").toLocaleDateString("en-GB", { month: "short", year: "numeric" })}`
    : periods[0] ?? "recent";

  // Overall UK summary
  if (data.totalTransactions) {
    signals.push(signal(
      "land-reg-uk-total", "LAND REG",
      `${fmt(data.totalTransactions)} property completions`,
      "UK-wide", period,
      `Each completion triggers legal, survey, removals & fit-out spend — avg ${fmtGbp(data.ukAvgPrice)} transaction value`,
      ["property", "construction", "professional_services"],
      { value: data.totalTransactions, sourceUrl: url, fetchedAt: data.fetchedAt },
    ));
  }

  // Top 5 counties by volume
  const topCounties = [...data.byCounty]
    .sort((a, b) => b.transactionCount - a.transactionCount)
    .slice(0, 5);

  for (const c of topCounties) {
    if (!c.transactionCount || !c.avgPrice) continue;
    const newBuildNote = c.newBuildCount
      ? ` · ${fmt(c.newBuildCount)} new-builds (developer-grade procurement)`
      : "";
    signals.push(signal(
      `land-reg-${c.county.toLowerCase().replace(/\s+/g, "-")}`, "LAND REG",
      `${fmt(c.transactionCount)} completions  ·  avg ${fmtGbp(c.avgPrice)}`,
      c.county, period,
      `New owners at this price point are active buyers — solicitors, surveyors, tradespeople & builders all procured within 60 days${newBuildNote}`,
      ["property", "construction"],
      { value: c.transactionCount, sourceUrl: url, fetchedAt: data.fetchedAt },
    ));
  }

  // New builds aggregate
  const totalNewBuilds = data.byCounty.reduce((s, c) => s + (c.newBuildCount ?? 0), 0);
  if (totalNewBuilds > 0) {
    signals.push(signal(
      "land-reg-new-builds", "LAND REG",
      `${fmt(totalNewBuilds)} new-build completions`,
      "UK-wide", period,
      "Developers procure across trades 6–18 months before handover — supply chain procurement window is open now",
      ["construction", "property", "energy"],
      { value: totalNewBuilds, sourceUrl: url, fetchedAt: data.fetchedAt },
    ));
  }

  return signals;
}

async function companiesHouseSignals(pool: Pool): Promise<MarketSignal[]> {
  type ChSnapshot = {
    fetchedAt: string; totalFound: number; periodFrom: string; periodTo: string;
    topCounties: { county: string; count: number }[];
    businesses: { sector: string; county: string; incorporatedOn: string }[];
  };

  const data = await getLatestPayload<ChSnapshot>(pool, "ch_new_businesses");
  if (!data || !data.totalFound) return [];

  const signals: MarketSignal[] = [];
  const url = "https://find-and-update.company-information.service.gov.uk/advanced-search/get-results";
  const from = data.periodFrom ? new Date(data.periodFrom).toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" }) : "";
  const to = data.periodTo ? new Date(data.periodTo).toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" }) : "";
  const period = from && to ? `${from}–${to}` : "last 30 days";

  signals.push(signal(
    "ch-total-incorporations", "COMPANIES HOUSE",
    `${fmt(data.totalFound)} new companies incorporated`,
    "UK-wide", period,
    "Every new company needs a bank account, accountant, insurance & software within 14 days of incorporation",
    ["finance", "tech", "professional_services"],
    { value: data.totalFound, sourceUrl: url, fetchedAt: data.fetchedAt },
  ));

  // Top county
  const top = data.topCounties?.[0];
  if (top) {
    signals.push(signal(
      `ch-county-${top.county.toLowerCase().replace(/\s+/g, "-")}`, "COMPANIES HOUSE",
      `${fmt(top.count)} new incorporations`,
      top.county, period,
      "Highest-density new-founder population — B2B acquisition campaigns here have lowest CPL",
      ["finance", "professional_services", "tech"],
      { value: top.count, sourceUrl: url, fetchedAt: data.fetchedAt },
    ));
  }

  // Sector breakdown
  const sectorCounts: Record<string, number> = {};
  for (const b of data.businesses ?? []) {
    if (b.sector && b.sector !== "Other") {
      sectorCounts[b.sector] = (sectorCounts[b.sector] ?? 0) + 1;
    }
  }
  const topSectors = Object.entries(sectorCounts)
    .sort(([, a], [, b]) => b - a)
    .slice(0, 3);

  const sectorSectorMap: Record<string, MarketSignalSector[]> = {
    "Software & Tech": ["tech"],
    "Automotive": ["automotive"],
    "Construction": ["construction"],
    "Health": ["health"],
    "Education": ["education"],
    "Food & Beverage": ["food_beverage"],
    "Retail": ["retail"],
    "Legal & Accounting": ["professional_services"],
    "Management Consulting": ["professional_services"],
    "Creative & Design": ["tech"],
    "Architecture & Engineering": ["construction"],
  };

  for (const [sec, count] of topSectors) {
    if (count < 3) continue;
    const sectors = sectorSectorMap[sec] ?? ["general" as MarketSignalSector];
    signals.push(signal(
      `ch-sector-${sec.toLowerCase().replace(/\s+/g, "-")}`, "COMPANIES HOUSE",
      `${fmt(count)} new ${sec} businesses incorporated`,
      "UK-wide", period,
      `${sec} founders are in active procurement mode — tools, services & support spend within first 30 days`,
      sectors,
      { value: count, sourceUrl: url, fetchedAt: data.fetchedAt },
    ));
  }

  return signals;
}

async function onsSpendingSignals(pool: Pool): Promise<MarketSignal[]> {
  type OnsSnapshot = {
    fetchedAt: string; datasetVersion: number;
    categories: { category: string; displayName: string; changePercent: number; latestYear: string }[];
    aggregateChangePercent: number;
  };

  const data = await getLatestPayload<OnsSnapshot>(pool, "ons_card_spending");
  if (!data || !data.categories?.length) return [];

  const signals: MarketSignal[] = [];
  const url = "https://www.ons.gov.uk/economy/economicoutputandproductivity/output/datasets/ukspendingoncreditanddebitcards";
  const latestYear = data.categories[0]?.latestYear ?? new Date().getFullYear().toString();

  const sectorMap: Record<string, { sectors: MarketSignalSector[]; implication: string }> = {
    delayable: {
      sectors: ["retail", "automotive", "property"],
      implication: "Consumer pullback on big-ticket items shifts spend to B2B and public sector — now the growth channel",
    },
    social: {
      sectors: ["food_beverage", "retail"],
      implication: "Leisure & entertainment spend signals a consumer cohort with disposable income — adjacent product opportunity",
    },
    staple: {
      sectors: ["retail", "food_beverage"],
      implication: "Essential spend holding up — working population is active and purchasing",
    },
    aggregate: {
      sectors: ["general"],
      implication: "Overall consumer spending trajectory sets the B2C demand environment for all sectors",
    },
  };

  for (const cat of data.categories) {
    const map = sectorMap[cat.category];
    if (!map) continue;
    const direction = cat.changePercent >= 0 ? "up" : "down";
    signals.push(signal(
      `ons-spending-${cat.category}`, "ONS",
      `${cat.displayName}  ·  ${fmtPct(cat.changePercent)} YoY`,
      "UK-wide", latestYear,
      map.implication,
      map.sectors,
      { changePercent: cat.changePercent, sourceUrl: url, fetchedAt: data.fetchedAt },
    ));
  }

  return signals;
}

async function ukriSignals(pool: Pool): Promise<MarketSignal[]> {
  type UkriProject = {
    id: string; title: string; status: string;
    valuePounds: number; funder: string; sector: string;
  };

  const rows = await getLatestPayloads<UkriProject>(pool, "ukri_grants", 50);
  if (!rows.length) return [];

  const signals: MarketSignal[] = [];
  const url = "https://gtr.ukri.org/";
  const fetchedAt = rows[0]?.fetchedAt ?? new Date().toISOString();

  const activeGrants = rows.map(r => r.payload).filter(p => p.status === "Active" || !p.status);
  const totalValue = activeGrants.reduce((s, p) => s + (p.valuePounds ?? 0), 0);
  const count = activeGrants.length;

  if (count > 0) {
    signals.push(signal(
      "ukri-active-grants", "UKRI",
      `${fmt(count)} active research & innovation grants${totalValue ? `  ·  ${fmtGbp(totalValue)} total value` : ""}`,
      "UK-wide", "live",
      "Each grant generates sub-contractor and equipment procurement — research institutions are active buyers",
      ["tech", "health", "education", "energy"],
      { value: count, sourceUrl: url, fetchedAt },
    ));
  }

  // Sector breakdown
  const sectorCounts: Record<string, number> = {};
  for (const p of activeGrants) {
    if (p.sector) sectorCounts[p.sector] = (sectorCounts[p.sector] ?? 0) + 1;
  }
  const topSec = Object.entries(sectorCounts).sort(([, a], [, b]) => b - a).slice(0, 2);
  for (const [sec, cnt] of topSec) {
    signals.push(signal(
      `ukri-sector-${sec.toLowerCase().replace(/\s+/g, "-").slice(0, 30)}`, "UKRI",
      `${fmt(cnt)} active grants  ·  ${sec}`,
      "UK-wide", "live",
      "Grant-funded teams procure specialist suppliers — being visible to these teams is a low-competition channel",
      ["tech", "health", "education"],
      { value: cnt, sourceUrl: url, fetchedAt },
    ));
  }

  return signals;
}

async function bbcNewsSignals(pool: Pool): Promise<MarketSignal[]> {
  type BbcItem = { title: string; link: string; description: string; pubDate: string };

  const rows = await getLatestPayloads<BbcItem>(pool, "bbc_news", 20);
  if (!rows.length) return [];

  const signals: MarketSignal[] = [];
  const today = new Date().toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" });

  // Filter for business-relevant headlines
  const businessKeywords = [
    "investment", "billion", "million", "jobs", "housing", "nhs", "energy",
    "economy", "budget", "interest rate", "inflation", "construction", "tech",
    "ev", "electric", "property", "market", "trade", "exports",
  ];

  for (const row of rows.slice(0, 10)) {
    const item = row.payload;
    if (!item?.title) continue;
    const titleLower = item.title.toLowerCase();
    const isRelevant = businessKeywords.some(k => titleLower.includes(k));
    if (!isRelevant) continue;

    const sectors: MarketSignalSector[] = [];
    if (/nhs|health|hospital/i.test(item.title)) sectors.push("health");
    if (/energy|ev|electric|oil|gas|solar/i.test(item.title)) sectors.push("energy");
    if (/housing|property|planning|build/i.test(item.title)) sectors.push("property", "construction");
    if (/tech|digital|ai|software/i.test(item.title)) sectors.push("tech");
    if (/economy|budget|tax|inflation|rate/i.test(item.title)) sectors.push("finance", "general");
    if (!sectors.length) sectors.push("general");

    signals.push(signal(
      `bbc-${Buffer.from(item.title).toString("base64").slice(0, 16)}`, "BBC",
      item.title,
      "UK", today,
      "Live market signal — monitor for downstream procurement and commercial impact",
      sectors,
      { sourceUrl: item.link, fetchedAt: row.fetchedAt },
    ));

    if (signals.length >= 5) break;
  }

  return signals;
}

// ── Main Export ────────────────────────────────────────────────────────────────

export async function generateMarketSignals(
  pool: Pool,
  options: { sectors?: MarketSignalSector[]; limit?: number } = {},
): Promise<MarketIntelSnapshot> {
  const [dvla, dvlaReg, landReg, ch, ons, ukri, bbc] = await Promise.allSettled([
    dvlaVehicleSignals(pool),
    dvlaRegionalSignals(pool),
    landRegistrySignals(pool),
    companiesHouseSignals(pool),
    onsSpendingSignals(pool),
    ukriSignals(pool),
    bbcNewsSignals(pool),
  ]);

  const allSignals = [
    ...(dvla.status === "fulfilled" ? dvla.value : []),
    ...(dvlaReg.status === "fulfilled" ? dvlaReg.value : []),
    ...(landReg.status === "fulfilled" ? landReg.value : []),
    ...(ch.status === "fulfilled" ? ch.value : []),
    ...(ons.status === "fulfilled" ? ons.value : []),
    ...(ukri.status === "fulfilled" ? ukri.value : []),
    ...(bbc.status === "fulfilled" ? bbc.value : []),
  ];

  const filtered = options.sectors?.length
    ? allSignals.filter(s => s.sectors.some(sec => options.sectors!.includes(sec)))
    : allSignals;

  const limited = options.limit ? filtered.slice(0, options.limit) : filtered;

  const sourcesCovered = [...new Set(allSignals.map(s => s.source))];

  return {
    generatedAt: new Date().toISOString(),
    signals: limited,
    sourcesCovered,
    totalSignals: filtered.length,
  };
}

export async function getSignalsForSector(
  pool: Pool,
  sector: MarketSignalSector,
  limit = 10,
): Promise<MarketSignal[]> {
  const snap = await generateMarketSignals(pool, { sectors: [sector], limit });
  return snap.signals;
}

export function formatSignalLine(s: MarketSignal): string {
  return s.formatted;
}

// ── Geo-demand exports (for the Atlas demand map) ───────────────────────────────
// These return RAW geographic demand arrays (county/region keyed) so the map can
// plot real private-sector demand proxies, not just procurement contracts.

export type GeoDemandPoint = {
  place: string;        // county or region name
  value: number;        // primary demand magnitude
  secondary?: number;   // optional secondary metric (e.g. new builds)
  detail: string;       // human label, e.g. "12,340 completions · 410 new builds"
};

/** Land Registry property demand by county — completions + new builds.
 *  New builds are a direct demand proxy for roofing/construction/fit-out trades. */
export async function getCountyPropertyDemand(pool: Pool): Promise<GeoDemandPoint[]> {
  type LandReg = {
    byCounty: { county: string; transactionCount: number; avgPrice: number; newBuildCount: number }[];
  };
  const data = await getLatestPayload<LandReg>(pool, "land_registry_transactions");
  if (!data?.byCounty?.length) return [];
  return data.byCounty
    .filter(c => c.county && c.transactionCount > 0)
    .map(c => ({
      place: c.county,
      value: c.transactionCount,
      secondary: c.newBuildCount ?? 0,
      detail: `${fmt(c.transactionCount)} completions${c.newBuildCount ? ` · ${fmt(c.newBuildCount)} new builds` : ""}${c.avgPrice ? ` · avg ${fmtGbp(c.avgPrice)}` : ""}`,
    }));
}

/** DVLA vehicle demand by region — total cars + company/fleet share. */
export async function getRegionalVehicleDemand(pool: Pool): Promise<GeoDemandPoint[]> {
  type DvlaRegion = { region: string; totalCars: number; companyCars: number; quarter: string };
  const rows = await getLatestPayloads<DvlaRegion>(pool, "dvla_ods_regional", 14);
  if (!rows.length) return [];
  return rows
    .map(r => r.payload)
    .filter(r => r?.region && r.totalCars > 0)
    .map(r => {
      const fleetPct = r.totalCars ? Math.round((r.companyCars / r.totalCars) * 100) : 0;
      return {
        place: r.region,
        value: r.totalCars,
        secondary: r.companyCars ?? 0,
        detail: `${fmt(r.totalCars)} licensed vehicles${fleetPct ? ` · ${fleetPct}% fleet/company` : ""}`,
      };
    });
}

/** Companies House new-business demand by county — proxy for B2B service demand. */
export async function getCountyBusinessDemand(pool: Pool): Promise<GeoDemandPoint[]> {
  type ChSnapshot = { topCounties: { county: string; count: number }[] };
  const data = await getLatestPayload<ChSnapshot>(pool, "ch_new_businesses");
  if (!data?.topCounties?.length) return [];
  return data.topCounties
    .filter(c => c.county && c.count > 0)
    .map(c => ({
      place: c.county,
      value: c.count,
      detail: `${fmt(c.count)} new companies incorporated`,
    }));
}

/** Companies House sector-specific density by county — competitive-landscape signal.
 *  Filters the CH businesses array by SIC-sector terms and groups by county.
 *  Uses sectorTotals to scale sample counts to estimated actuals — e.g. if we
 *  sampled 100 retail businesses from 7,500 total, a county with 5 in the sample
 *  gets scaled to ~375 estimated. */
export async function getCountySectorDensity(
  pool: Pool,
  sectorFilter: string[],
  sectorLabel: string,
): Promise<GeoDemandPoint[]> {
  type ChSnapshot = {
    topCounties: { county: string; count: number }[];
    businesses: { sector: string; county: string; incorporatedOn: string }[];
    sectorTotals?: Record<string, number>;
  };
  const data = await getLatestPayload<ChSnapshot>(pool, "ch_new_businesses");
  if (!data?.businesses?.length) return [];

  const filterLc = sectorFilter.map(s => s.toLowerCase());
  const filtered = data.businesses.filter(b =>
    b.sector && filterLc.some(sf => b.sector.toLowerCase().includes(sf)),
  );
  if (!filtered.length) return [];

  // Scale factor: if sectorTotals says there are 7,500 Retail businesses but we
  // only sampled 100, multiply each county count by 75 for estimated actuals.
  let scaleFactor = 1;
  if (data.sectorTotals) {
    let totalHits = 0;
    for (const sf of sectorFilter) {
      const sfLc = sf.toLowerCase();
      for (const [sector, hits] of Object.entries(data.sectorTotals)) {
        if (sector.toLowerCase().includes(sfLc)) totalHits += hits;
      }
    }
    if (totalHits > filtered.length) {
      scaleFactor = totalHits / filtered.length;
    }
  }

  const byCty: Record<string, number> = {};
  for (const b of filtered) {
    if (b.county) byCty[b.county] = (byCty[b.county] ?? 0) + 1;
  }

  const scaled = Object.entries(byCty).map(([county, raw]) => ({
    county,
    estimated: Math.round(raw * scaleFactor),
  }));
  const avg = scaled.reduce((s, c) => s + c.estimated, 0) / scaled.length;

  return scaled
    .filter(c => c.estimated > 0)
    .map(c => ({
      place: c.county,
      value: c.estimated,
      detail: `~${fmt(c.estimated)} ${sectorLabel} businesses`
        + (c.estimated >= avg * 1.5 ? " · proven market" : c.estimated <= avg * 0.5 ? " · opportunity gap" : " · active market"),
    }))
    .sort((a, b) => b.value - a.value);
}

// ── Named businesses (the WHO behind the WHERE) ─────────────────────────────────
// getCountySectorDensity collapses the ch_new_businesses payload to county counts.
// This returns the actual NAMED companies behind those counts so the map/scan can
// say "here are 25 newly-registered businesses in your sector you could pitch",
// not just "Manchester is dense". These are NEW INCORPORATIONS — warm prospects
// (new businesses need suppliers), not confirmed buyers. Frame them that way.

export type NamedBusiness = {
  name: string;
  number: string;        // Companies House company number → companieshouse.gov.uk link
  incorporatedOn: string;
  county: string;
  address: string;
  sector: string;
  sicCodes?: string[];   // raw 5-digit SIC codes from CH, used by the SIC-filtered leads path
};

export type NamedBusinessResult = {
  businesses: NamedBusiness[];
  sampleSize: number;     // how many we actually hold in the sample
  estimatedTotal: number; // sample scaled by sectorTotals (the real market size)
  label: string;
};

export async function getNamedBusinessesBySector(
  pool: Pool,
  sectorFilter: string[],
  label: string,
  opts: { county?: string; limit?: number; sicFilter?: string[] } = {},
): Promise<NamedBusinessResult> {
  type ChSnapshot = {
    businesses: NamedBusiness[];
    sectorTotals?: Record<string, number>;
  };
  const empty: NamedBusinessResult = { businesses: [], sampleSize: 0, estimatedTotal: 0, label };
  const data = await getLatestPayload<ChSnapshot>(pool, "ch_new_businesses");
  if (!data?.businesses?.length) return empty;

  // SIC-code filtering is tighter than sector-bucket filtering: it returns only
  // businesses whose own CH SIC codes match the keyword (e.g. perfumes → 47750,
  // not all of Retail). Falls back to sector matching when no SIC filter given.
  const sicSet = opts.sicFilter && opts.sicFilter.length
    ? new Set(opts.sicFilter)
    : null;
  let filtered: NamedBusiness[];
  if (sicSet) {
    filtered = data.businesses.filter(b => b.sicCodes && b.sicCodes.some(c => sicSet.has(c)));
  } else {
    const filterLc = sectorFilter.map(s => s.toLowerCase());
    filtered = data.businesses.filter(b =>
      b.sector && filterLc.some(sf => b.sector.toLowerCase().includes(sf)),
    );
  }
  if (!filtered.length) return empty;

  const sampleSize = filtered.length;

  // Scale the sample up to the real market size using sectorTotals (the CH API's
  // total `hits`), same approach as getCountySectorDensity. SIC-filtered queries
  // skip scaling — the sample IS the estimate, not a slice of a larger bucket.
  let scaleFactor = 1;
  if (!sicSet && data.sectorTotals) {
    let totalHits = 0;
    for (const sf of sectorFilter) {
      const sfLc = sf.toLowerCase();
      for (const [sector, hits] of Object.entries(data.sectorTotals)) {
        if (sector.toLowerCase().includes(sfLc)) totalHits += hits;
      }
    }
    if (totalHits > sampleSize) scaleFactor = totalHits / sampleSize;
  }

  if (opts.county) {
    const cty = opts.county.toLowerCase();
    filtered = filtered.filter(b => (b.county || "").toLowerCase() === cty);
  }

  // Newest incorporations first — the freshest prospects (just formed, buying now).
  filtered.sort((a, b) => (b.incorporatedOn || "").localeCompare(a.incorporatedOn || ""));

  const limit = Math.max(1, Math.min(opts.limit ?? 25, 200));
  return {
    businesses: filtered.slice(0, limit).map(b => ({
      name: b.name,
      number: b.number,
      incorporatedOn: b.incorporatedOn,
      county: b.county,
      address: b.address,
      sector: b.sector,
    })),
    sampleSize,
    estimatedTotal: Math.round(sampleSize * scaleFactor),
    label,
  };
}

// ── Regional Intelligence ──────────────────────────────────────────────────────

export type RegionIntel = {
  region: string;
  dvla?: { totalCars: number; companyCars: number; fleetPct: number; quarter: string };
  landReg?: { completions: number; avgPrice: number; newBuilds: number; period: string };
  newBusinesses?: { count: number; period: string };
  topSectors: string[];
  activityScore: number; // 0–100 composite
};

const COUNTY_TO_REGION: Record<string, string> = {
  "GREATER LONDON": "London",
  "GREATER MANCHESTER": "North West", "LANCASHIRE": "North West", "MERSEYSIDE": "North West", "CHESHIRE": "North West", "CUMBRIA": "North West",
  "WEST YORKSHIRE": "Yorkshire", "SOUTH YORKSHIRE": "Yorkshire", "NORTH YORKSHIRE": "Yorkshire", "EAST RIDING OF YORKSHIRE": "Yorkshire",
  "WEST MIDLANDS": "West Midlands", "STAFFORDSHIRE": "West Midlands", "WARWICKSHIRE": "West Midlands", "WORCESTERSHIRE": "West Midlands", "SHROPSHIRE": "West Midlands", "HEREFORDSHIRE": "West Midlands",
  "DERBYSHIRE": "East Midlands", "NOTTINGHAMSHIRE": "East Midlands", "LEICESTERSHIRE": "East Midlands", "NORTHAMPTONSHIRE": "East Midlands", "LINCOLNSHIRE": "East Midlands", "RUTLAND": "East Midlands",
  "ESSEX": "East of England", "HERTFORDSHIRE": "East of England", "NORFOLK": "East of England", "SUFFOLK": "East of England", "CAMBRIDGESHIRE": "East of England", "BEDFORDSHIRE": "East of England",
  "KENT": "South East", "SURREY": "South East", "HAMPSHIRE": "South East", "WEST SUSSEX": "South East", "EAST SUSSEX": "South East", "OXFORDSHIRE": "South East", "BERKSHIRE": "South East", "BUCKINGHAMSHIRE": "South East", "ISLE OF WIGHT": "South East",
  "DEVON": "South West", "CORNWALL": "South West", "SOMERSET": "South West", "WILTSHIRE": "South West", "GLOUCESTERSHIRE": "South West", "DORSET": "South West", "BRISTOL": "South West",
  "TYNE AND WEAR": "North East", "COUNTY DURHAM": "North East", "NORTHUMBERLAND": "North East", "CLEVELAND": "North East",
  "SOUTH GLAMORGAN": "Wales", "WEST GLAMORGAN": "Wales", "MID GLAMORGAN": "Wales", "GWENT": "Wales", "CLWYD": "Wales", "POWYS": "Wales", "DYFED": "Wales", "GWYNEDD": "Wales",
  "STRATHCLYDE": "Scotland", "LOTHIAN": "Scotland", "TAYSIDE": "Scotland", "GRAMPIAN": "Scotland", "HIGHLAND": "Scotland", "FIFE": "Scotland", "CENTRAL": "Scotland", "BORDERS": "Scotland",
};

// DVLA regional names → canonical region
const DVLA_TO_REGION: Record<string, string> = {
  "London": "London", "South East": "South East", "East": "East of England",
  "East Midlands": "East Midlands", "West Midlands": "West Midlands",
  "Yorkshire and The Humber": "Yorkshire", "North West": "North West",
  "North East": "North East", "South West": "South West",
  "Wales": "Wales", "Scotland": "Scotland", "Northern Ireland": "Northern Ireland",
};

export async function getRegionalIntelligence(pool: Pool): Promise<RegionIntel[]> {
  type DvlaRegion = { region: string; totalCars: number; companyCars: number; quarter: string };
  type LandReg = { fetchedAt: string; periodCovered: string[]; byCounty: { county: string; transactionCount: number; avgPrice: number; newBuildCount: number }[] };
  type ChSnapshot = { fetchedAt: string; totalFound: number; periodFrom: string; periodTo: string; topCounties: { county: string; count: number }[] };

  const [dvlaRows, landRegData, chData] = await Promise.all([
    getLatestPayloads<DvlaRegion>(pool, "dvla_ods_regional", 12),
    getLatestPayload<LandReg>(pool, "land_registry_transactions"),
    getLatestPayload<ChSnapshot>(pool, "ch_new_businesses"),
  ]);

  const regionMap: Record<string, Partial<RegionIntel>> = {};

  const ensure = (r: string) => {
    if (!regionMap[r]) regionMap[r] = { region: r, topSectors: [] };
    return regionMap[r];
  };

  // DVLA regional data
  for (const row of dvlaRows) {
    const p = row.payload;
    const region = DVLA_TO_REGION[p.region] ?? p.region;
    if (!region) continue;
    const fleetPct = p.totalCars ? Math.round((p.companyCars / p.totalCars) * 100) : 0;
    ensure(region).dvla = { totalCars: p.totalCars, companyCars: p.companyCars, fleetPct, quarter: p.quarter };
    if (fleetPct > 20) ensure(region).topSectors?.push("fleet");
  }

  // Land Registry county → region aggregation
  if (landRegData?.byCounty) {
    const periods = landRegData.periodCovered ?? [];
    const period = periods.length >= 2
      ? `${new Date(periods[0] + "-01").toLocaleDateString("en-GB", { month: "short", year: "numeric" })}–${new Date(periods[periods.length - 1] + "-01").toLocaleDateString("en-GB", { month: "short", year: "numeric" })}`
      : periods[0] ?? "recent";

    const regionAgg: Record<string, { completions: number; totalPrice: number; newBuilds: number; countyCount: number }> = {};

    for (const county of landRegData.byCounty) {
      const region = COUNTY_TO_REGION[county.county.toUpperCase()] ?? county.county;
      if (!regionAgg[region]) regionAgg[region] = { completions: 0, totalPrice: 0, newBuilds: 0, countyCount: 0 };
      regionAgg[region].completions += county.transactionCount;
      regionAgg[region].totalPrice += county.avgPrice * county.transactionCount;
      regionAgg[region].newBuilds += county.newBuildCount ?? 0;
      regionAgg[region].countyCount++;
    }

    for (const [region, agg] of Object.entries(regionAgg)) {
      const avgPrice = agg.completions ? Math.round(agg.totalPrice / agg.completions) : 0;
      ensure(region).landReg = { completions: agg.completions, avgPrice, newBuilds: agg.newBuilds, period };
      if (agg.completions > 500) ensure(region).topSectors?.push("property");
    }
  }

  // Companies House county breakdown
  if (chData?.topCounties) {
    const from = chData.periodFrom ? new Date(chData.periodFrom).toLocaleDateString("en-GB", { day: "2-digit", month: "short" }) : "";
    const to = chData.periodTo ? new Date(chData.periodTo).toLocaleDateString("en-GB", { day: "2-digit", month: "short" }) : "";
    const period = from && to ? `${from}–${to}` : "last 30 days";
    for (const c of chData.topCounties) {
      const region = DVLA_TO_REGION[c.county] ?? COUNTY_TO_REGION[c.county.toUpperCase()] ?? c.county;
      ensure(region).newBusinesses = { count: c.count, period };
      if (c.count > 5) ensure(region).topSectors?.push("startups");
    }
  }

  // Compute composite activity score and deduplicate sectors
  const regions = Object.values(regionMap).map(r => {
    const landScore = Math.min((r.landReg?.completions ?? 0) / 50, 40);
    const dvlaScore = Math.min((r.dvla?.totalCars ?? 0) / 200_000, 30);
    const bizScore = Math.min((r.newBusinesses?.count ?? 0) * 2, 20);
    const nbScore = Math.min((r.landReg?.newBuilds ?? 0) * 2, 10);
    const topSectors = [...new Set(r.topSectors ?? [])];
    return { ...r, region: r.region ?? "Unknown", topSectors, activityScore: Math.round(landScore + dvlaScore + bizScore + nbScore) } as RegionIntel;
  });

  return regions.sort((a, b) => b.activityScore - a.activityScore);
}
