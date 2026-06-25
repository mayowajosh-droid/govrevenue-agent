/**
 * External signal pre-fetcher — runs BEFORE the main report call.
 * Layer 1: DB market signals (DVLA, Land Reg, Companies House, ONS) already stored
 * Layer 2: Targeted web-search pre-pass using Claude Sonnet to pull sector-specific
 *          external data (SMMT model breakdowns, Euromonitor CAGR, Amazon UK trends,
 *          Google Trends, Mintel, ONS demographic cohorts) that the main Opus call
 *          would waste its 5 search budget trying to find.
 */

import Anthropic from "@anthropic-ai/sdk";
import type { Pool } from "pg";
import {
  generateMarketSignals,
  type MarketSignal,
  type MarketSignalSector,
} from "./market-intel.js";

// ── Types ─────────────────────────────────────────────────────────────────────

export interface PreloadedSignal {
  source: string;
  stat: string;
  geography: string;
  period: string;
  implication: string;
  formatted: string; // "SOURCE  ·  stat  ·  geo  ·  period  ·  implication →"
  layer: "db" | "web"; // which layer produced it
}

// ── Sector detection from intake text ────────────────────────────────────────

export function detectSectorsFromIntake(
  mainServices: string,
  idealBuyers = "",
  secondaryServices = "",
): MarketSignalSector[] {
  const text = `${mainServices} ${idealBuyers} ${secondaryServices}`.toLowerCase();
  const sectors = new Set<MarketSignalSector>();

  if (/\bcar\b|vehicle|fleet|automotive|motoring|\bvan\b|\btruck\b|lorr|fragrance|diffuser|car care|detailing|accessori/.test(text))
    sectors.add("automotive");
  if (/transport|rail|aviation|maritime|airport|shipping|courier|last.?mile/.test(text))
    sectors.add("transport");
  if (/constru|civil engineer|housebuilding|scaffold|demolish|\bbuild\b|groundwork|utilities contractor/.test(text))
    sectors.add("construction");
  if (/facilit|cleaning|maintenance|\bFM\b|mechanical|electrical|hvac|plumb|pest|ground/.test(text))
    sectors.add("professional_services");
  if (/health|nhs|hospital|clinical|medical|care home|social care|pharmacy|dental|gp\b/.test(text))
    sectors.add("health");
  if (/education|school|university|college|learning|training|pupil|student|academy/.test(text))
    sectors.add("education");
  if (/digital|software|tech|\bit\b|cyber|cloud|data|\bai\b|saas|\bapp\b|platform|devops/.test(text))
    sectors.add("tech");
  if (/energy|solar|\bev\b|charge point|decarboni|retrofit|net zero|renewable|hydrogen/.test(text))
    sectors.add("energy");
  if (/recruit|staffing|agency|temporary staff|\bhr\b|workforce|labour|payroll|perm\b/.test(text))
    sectors.add("professional_services");
  if (/propert|housing|estate|land|planning|develop|let\b|tenant|landlord|conveyancing/.test(text))
    sectors.add("property");
  if (/waste|recycl|environment|sustainability|refuse|landfill|compost/.test(text))
    sectors.add("general");
  if (/food|catering|hospitality|restaurant|meal|beverage|drink|vend/.test(text))
    sectors.add("food_beverage");
  if (/security|guard|cctv|access control|patrol|manned|surveillance|door/.test(text))
    sectors.add("professional_services");
  if (/legal|solicitor|\blaw\b|compliance|regulatory|contract management/.test(text))
    sectors.add("finance");
  if (/financ|accounting|audit|\btax\b|insurance|pension|payroll|vat\b|bookkeep/.test(text))
    sectors.add("finance");
  if (/retail|e.?commerc|online shop|marketplace|amazon|ecommerce/.test(text))
    sectors.add("retail");

  return sectors.size > 0 ? [...sectors] : ["general"];
}

// ── Targeted search query bank per sector ────────────────────────────────────

const SECTOR_SEARCH_QUERIES: Record<string, string[]> = {
  automotive: [
    "SMMT 2024 UK new car registrations luxury brands Porsche Range Rover Mercedes BMW annual statistics site:smmt.co.uk OR site:gov.uk",
    "DVLA UK electric vehicle zero emission registrations statistics Q1 2025 data",
    "Euromonitor IBISWorld UK automotive accessories in-car products market size CAGR 2024 2025",
    "ONS UK household luxury goods consumer spending 2024 annual data",
    "Mintel UK car accessories in-car fragrance consumer research 2024",
    "Amazon UK car air freshener luxury diffuser search volume trending 2025",
    "Google Trends UK car diffuser in-car fragrance all-time high 2025",
    "ONS UK male 35-54 age group luxury discretionary spending 2024",
    "SMMT UK fleet business company car registrations 2024 percentage of total",
    "ONS UK gifting luxury non-clothing personal care spend 2024 annual",
    "Land Registry UK residential completions new homeowners 2025",
    "Companies House UK new company formations London 2026 Q1 statistics",
  ],
  construction: [
    "ONS UK construction output new orders statistics Q1 2025 site:ons.gov.uk",
    "DLUHC housing pipeline new build starts completions 2025 UK",
    "CITB construction workforce skills statistics 2024 2025 training levy",
    "CCS Crown Commercial Service construction framework total spend 2024",
    "Build UK construction industry output value statistics 2025",
    "ONS UK construction sector employment regional breakdown 2025",
    "BCIS Building Cost Information Service tender price index 2025",
    "Glenigan UK construction pipeline starts value 2025",
  ],
  health: [
    "NHS England total procurement spend statistics 2024 2025 site:england.nhs.uk",
    "NHS Supply Chain total purchase categories value 2024",
    "DHSC Department Health Social Care capital spending 2024 2025",
    "ONS UK health social care sector employment workforce 2025",
    "NHS Digital health technology digital transformation budget 2025",
    "CCS NHS framework spend professional services 2024",
  ],
  education: [
    "DfE UK school capital expenditure pupil numbers 2024 2025 site:gov.uk",
    "ESFA education funding agency total allocation 2025 statistics",
    "ONS UK education sector spending 2024 2025",
    "UK universities higher education procurement spend 2024 2025",
    "DfE multi-academy trust MAT procurement spend statistics 2024",
  ],
  tech: [
    "techUK UK public sector digital technology spend 2024 2025 site:techuk.org",
    "G-Cloud Crown Commercial Service cloud spend total value 2024",
    "DCMS UK digital economy sector size employment 2024 annual report",
    "HMRC UK software SaaS digital businesses VAT registrations 2024",
    "NCSC UK cyber security sector revenue employment 2024",
    "CIPS UK IT procurement outsourcing spend 2024",
  ],
  energy: [
    "OFGEM UK energy transition smart grid investment 2025 statistics",
    "BEIS DESNZ UK renewable energy capacity MW installed 2025",
    "UKRI Innovate UK clean energy R&D investment grant 2024 2025",
    "OZEV UK EV charge point public installations 2025 statistics",
    "ONS UK energy sector employment output 2024 2025",
  ],
  professional_services: [
    "Companies House UK new company incorporations monthly Q1 2026 statistics",
    "ONS UK professional business services employment output 2024 2025",
    "REC UK recruitment industry market size revenue 2024 2025 site:rec.uk.com",
    "CIPS UK procurement outsourcing facilities market size 2024",
    "HMRC UK professional services VAT registrations 2024",
  ],
  property: [
    "HM Land Registry UK residential property transactions completions 2025 statistics",
    "ONS UK house price index regional breakdown 2025 site:ons.gov.uk",
    "DLUHC UK housing completions new build starts 2025",
    "RICS UK residential property market survey Q1 2025",
    "Land Registry UK commercial property transaction volumes 2024 2025",
  ],
  transport: [
    "DfT UK road freight statistics vehicle kilometres 2024 2025 site:gov.uk",
    "DVLA UK commercial vehicle HGV van registrations 2024 statistics",
    "ORR UK rail freight passenger statistics 2024 2025",
    "BIFA UK freight logistics market size revenue 2024 2025",
    "ONS UK transport sector employment output 2025",
  ],
  finance: [
    "FCA UK financial services sector size employment 2024 2025 site:fca.org.uk",
    "ONS UK financial insurance sector output 2024 2025",
    "Companies House UK financial services company formations 2025",
    "HMRC UK financial services tax receipts 2024 annual",
  ],
  retail: [
    "ONS UK retail sales statistics 2025 site:ons.gov.uk",
    "BRC British Retail Consortium UK retail market size 2024 2025",
    "Amazon UK marketplace seller growth e-commerce 2024 2025 statistics",
    "ONS UK online retail percentage of total sales 2025",
    "HMRC UK e-commerce businesses VAT registrations 2024",
  ],
  food_beverage: [
    "Food and Drink Federation UK industry output employment 2024 2025 site:fdf.org.uk",
    "ONS UK food beverage consumer spending household 2024",
    "CGA UK hospitality pub restaurant sector statistics 2024 2025",
    "UK public sector catering food procurement framework spend 2024",
    "WRAP UK food waste reduction statistics 2024 2025",
  ],
  general: [
    "ONS UK GDP sector output statistics Q1 2025 site:ons.gov.uk",
    "Companies House UK new company formations monthly 2026 statistics",
    "CCS Crown Commercial Service public sector procurement total spend 2024",
    "ONS UK employment sector regional breakdown 2025",
  ],
};

// ── Layer 2: Web-search pre-pass via Claude Sonnet ───────────────────────────

async function fetchWebSearchSignals(
  anthropicClient: Anthropic,
  companyName: string,
  mainServices: string,
  idealBuyers: string,
  sectors: MarketSignalSector[],
): Promise<PreloadedSignal[]> {
  const primarySector = sectors[0] ?? "general";
  const primaryQueries = SECTOR_SEARCH_QUERIES[primarySector] ?? SECTOR_SEARCH_QUERIES.general;
  const secondaryQueries = sectors[1]
    ? (SECTOR_SEARCH_QUERIES[sectors[1]] ?? []).slice(0, 2)
    : [];
  // Keep the query list aligned with the search budget (8) so we don't ask the model
  // to cover more topics than it has searches for. Credit-conscious: each search costs.
  const queries = [...primaryQueries, ...secondaryQueries].slice(0, 8);

  const prompt = `You are a UK market research analyst compiling a signals brief for "${companyName}".

Company services: ${mainServices}
Target buyers: ${idealBuyers || "UK public sector and B2B buyers"}
Primary sector: ${primarySector}

Use web search to find REAL, CURRENT UK market statistics. Run searches on these topics:
${queries.map((q, i) => `${i + 1}. ${q}`).join("\n")}

Go deep — find model/brand-level breakdowns, demographic splits, named CAGR percentages, Amazon UK search volumes, Google Trends peaks. Not just industry totals.

Return ONLY pipe-separated lines in this EXACT format — nothing else, no headers, no explanations:
SOURCE|statistic with specific number|geography|time period|direct commercial implication one sentence

Good examples of the expected format and quality:
SMMT|Porsche: 16,800 new registrations +8% YoY|UK-wide|2024|core luxury buyer segment, new keys in new hands
EUROMONITOR|car fragrance & ambient accessories +19% CAGR|Europe|2022–2026|fastest-growing in-car accessory segment — structural tailwind
DVLA|528,400 zero-emission vehicles registered +24% YoY|UK-wide|Q1 2025|EV cabins are odourless by design — scent is the new luxury differentiator
AMAZON UK|"luxury car air freshener" search volume +43% YoY|UK|Apr–Jun 2025|consumer demand is growing — timing favours new market entry
ONS|male 35–54 luxury discretionary spend +14% YoY|UK-wide|2024|Viano's primary buyer cohort growing fastest in luxury personal spend
GOOGLE TRENDS|"car diffuser" UK interest — all-time high|UK|May 2025|peak search interest week-on-week — market entry timing ideal
MINTEL|71% of luxury car owners say interior personalisation matters|UK|2024|scent is most personal — purchase intent rises with vehicle price

Rules:
- Real numbers only. Never invent statistics.
- Be efficient with searches — a single good search often yields several usable stats. Extract multiple signals per search result.
- Minimum 12 lines. Aim for 16.
- Return ONLY the pipe-separated lines.`;

  try {
    const message = await anthropicClient.messages.create({
      model: "claude-sonnet-4-6",
      max_tokens: 3000,
      messages: [{ role: "user", content: prompt }],
      tools: [{ type: "web_search_20250305", name: "web_search", max_uses: 8 }] as any,
    });

    const raw = message.content
      .map(b => (b.type === "text" ? b.text : ""))
      .join("")
      .trim();

    const signals: PreloadedSignal[] = [];
    for (const line of raw.split("\n")) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#") || trimmed.startsWith("-")) continue;
      const parts = trimmed.split("|").map(p => p.trim());
      if (parts.length < 4) continue;
      const [source, stat, geography, period, implication = ""] = parts;
      if (!source || !stat || stat.length < 8) continue;
      const src = source.replace(/^\*+|\*+$/g, "").toUpperCase();
      signals.push({
        source: src,
        stat,
        geography: geography || "UK",
        period: period || String(new Date().getFullYear()),
        implication,
        formatted: `${src}  ·  ${stat}  ·  ${geography}  ·  ${period}  ·  ${implication}  →`,
        layer: "web",
      });
    }
    return signals;
  } catch (err) {
    console.warn("[external-signals] web search pre-pass failed:", String(err).slice(0, 120));
    return [];
  }
}

// ── Layer 1: Convert existing DB market signals ───────────────────────────────

function toPreloaded(s: MarketSignal): PreloadedSignal {
  return {
    source: s.source,
    stat: s.stat,
    geography: s.geography,
    period: s.period,
    implication: s.implication,
    formatted: s.formatted,
    layer: "db",
  };
}

// ── Main orchestrator ─────────────────────────────────────────────────────────

export async function fetchExternalSignals(opts: {
  companyName: string;
  mainServices: string;
  idealBuyers?: string;
  secondaryServices?: string;
  pool: Pool | null;
  anthropic: Anthropic | null;
}): Promise<PreloadedSignal[]> {
  const {
    companyName,
    mainServices,
    idealBuyers = "",
    secondaryServices = "",
    pool,
    anthropic: client,
  } = opts;

  const sectors = detectSectorsFromIntake(mainServices, idealBuyers, secondaryServices);

  const [dbResult, webResult] = await Promise.allSettled([
    pool
      ? generateMarketSignals(pool, { sectors, limit: 10 }).then(snap =>
          snap.signals.map(toPreloaded),
        )
      : Promise.resolve<PreloadedSignal[]>([]),

    client
      ? fetchWebSearchSignals(client, companyName, mainServices, idealBuyers, sectors)
      : Promise.resolve<PreloadedSignal[]>([]),
  ]);

  const dbSignals = dbResult.status === "fulfilled" ? dbResult.value : [];
  const webSignals = webResult.status === "fulfilled" ? webResult.value : [];

  // Web signals lead (more specific); DB signals fill gaps
  const seen = new Set<string>();
  const combined: PreloadedSignal[] = [];
  for (const s of [...webSignals, ...dbSignals]) {
    const key = s.stat.slice(0, 25).toLowerCase().replace(/\W/g, "");
    if (!seen.has(key)) {
      seen.add(key);
      combined.push(s);
    }
  }

  return combined;
}
