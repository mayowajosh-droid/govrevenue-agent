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
    signals.push(signal(
      `dvla-region-${r.onsCode}`, "DVLA",
      `${fmt(r.totalCars)} licensed vehicles — ${companyShare}% fleet/company`,
      r.region, quarter,
      `Fleet concentration signals B2B aftermarket opportunity — company car drivers are serviced separately from retail`,
      ["automotive", "professional_services"],
      { value: r.totalCars, sourceUrl: "https://www.gov.uk/government/statistical-data-sets/vehicle-licensing-statistics-data-files", fetchedAt: rows[0].fetchedAt },
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
      ["property", "construction", "professional_services", "retail"],
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
      `New owners at this price point are active buyers — solicitors, surveyors, tradespeople & furniture all procured within 60 days${newBuildNote}`,
      ["property", "construction", "professional_services"],
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
