// Pure, side-effect-free helpers extracted from index.ts so they can be unit tested
// in isolation (no server, DB, queue or network boot). index.ts imports from here.

export function escapeHtml(value: string) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

export function formatMoney(value: number | null | undefined) {
  if (typeof value !== "number" || Number.isNaN(value)) return "-";
  return new Intl.NumberFormat("en-GB", {
    style: "currency",
    currency: "GBP",
    maximumFractionDigits: 0
  }).format(value);
}

export function formatDate(value: string | null | undefined) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" });
}

export function fmtMoney(v: number): string {
  if (v >= 1_000_000_000) return `£${(v / 1_000_000_000).toFixed(2)}bn`;
  if (v >= 1_000_000) return `£${(v / 1_000_000).toFixed(1)}m`;
  if (v >= 1_000) return `£${Math.round(v / 1_000)}k`;
  return `£${Math.round(v)}`;
}

export function slugify(text: string): string {
  return text.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

// Self-calibrating outlier threshold: median notice value × 10,000, clamped between
// £50m (floor) and £10bn (ceiling). Excludes magnitude data errors (academy school at
// £18bn when the desk median is £200k → threshold £2bn → excluded) without capping
// legitimate large contracts (HS2 £4bn when median £500k → threshold £5bn → kept).
export function computeOutlierThreshold(values: number[]): number {
  const sorted = values.filter(v => v > 0).sort((a, b) => a - b);
  const median = sorted.length > 0 ? sorted[Math.floor(sorted.length / 2)] : 0;
  return median > 0
    ? Math.min(Math.max(median * 10_000, 50_000_000), 10_000_000_000)
    : 10_000_000_000;
}

export interface ParsedEdp {
  verdict: string;
  canTheyWinNow: string;
  bestFirstMoneyRoute: string;
  fastestActionThisWeek: string;
  mainBlocker: string;
  evidenceGrade: string;
  recommendedRoute: string;
}

export function parseEdpFromMarkdown(markdown: string): ParsedEdp | null {
  const sectionMatch = markdown.match(
    /##\s*1\.\s*Executive Decision Panel([\s\S]*?)(?=\n##\s+\d+\.|\n##\s+[A-Z]|$)/i
  );
  if (!sectionMatch) return null;

  const section = sectionMatch[1];

  function extractRow(fieldPattern: string): string {
    const re = new RegExp(
      `\\|\\s*${fieldPattern}\\s*\\|\\s*([^|\\n]+?)\\s*\\|`,
      "i"
    );
    const m = section.match(re);
    return m ? m[1].trim() : "";
  }

  const verdict = extractRow("Verdict");
  const evidenceGrade = extractRow("Evidence Grade");

  if (!verdict && !evidenceGrade) return null;

  return {
    verdict: verdict || "",
    canTheyWinNow: extractRow("Can they win now\\?"),
    bestFirstMoneyRoute: extractRow("Best first money route"),
    fastestActionThisWeek: extractRow("Fastest action this week"),
    mainBlocker: extractRow("Main blocker"),
    evidenceGrade: evidenceGrade || "",
    recommendedRoute: extractRow("Recommended route"),
  };
}

export function stripEdpFromMarkdown(markdown: string): string {
  return markdown.replace(
    /##\s*1\.\s*Executive Decision Panel[\s\S]*?(?=\n##\s+\d+\.|\n##\s+[A-Z])/i,
    ""
  );
}

export interface ConsistencyReport {
  valid: boolean;
  errors: string[];
  conflicts: string[];
}

export function validateReportConsistency(
  edp: ParsedEdp | null,
  markdown: string
): ConsistencyReport {
  const errors: string[] = [];
  const conflicts: string[] = [];

  if (!edp) {
    errors.push("Executive Decision Panel could not be parsed from the report markdown.");
    return { valid: false, errors, conflicts };
  }

  if (!edp.verdict) errors.push("EDP verdict is missing.");
  if (!edp.evidenceGrade) errors.push("EDP evidence grade is missing.");
  if (!edp.canTheyWinNow) errors.push("EDP can-they-win-now is missing.");
  if (!edp.recommendedRoute) errors.push("EDP recommended route is missing.");

  const gradePattern = /\|\s*Evidence Grade\s*\|\s*([A-E])\s*\|/gi;
  const foundGrades = new Set<string>();
  for (const m of markdown.matchAll(gradePattern)) {
    foundGrades.add(m[1].toUpperCase());
  }
  if (foundGrades.size > 1) {
    conflicts.push(
      `Conflicting evidence grades in report: ${[...foundGrades].join(", ")}`
    );
  }

  const verdictPattern = /\|\s*Verdict\s*\|\s*([^|\n]+?)\s*\|/gi;
  const foundVerdicts = new Set<string>();
  for (const m of markdown.matchAll(verdictPattern)) {
    foundVerdicts.add(m[1].trim().toLowerCase());
  }
  if (foundVerdicts.size > 1) {
    conflicts.push(
      `Conflicting verdicts in report: ${[...foundVerdicts].join("; ")}`
    );
  }

  return {
    valid: errors.length === 0 && conflicts.length === 0,
    errors,
    conflicts,
  };
}

export const AGGREGATOR_FRAGMENTS = [
  // BIP / procurement portals
  "bip solutions",
  // Palladium
  "palladium group", "palladium international", "palladium international limited",
  // CHIC — Communities and Housing Investment Consortium
  "chic consortium", "housing investment consortium",
  // YPO
  " ypo ", "yorkshire purchasing organisation",
  // EN Procure / North East councils
  "en procure", "enact procurement",
  "the association of north east councils", "north east procurement",
  // ESPO — Eastern Shires
  "eastern shires purchasing", " espo ",
  // Procurement Hub / Pagabo
  "procurement hub", "pagabo",
  // Scape
  "scape group", "scape procure", "scape framework",
  // Crescent
  "crescent purchasing", "crescent group",
  // Laser (Kent)
  "laser (kent)", "laser purchasing",
  // Pro-quote / Proactis
  "pro-quote", "proactis",
  // NHS Supply Chain (legal name: Supply Chain Coordination Limited)
  "nhs supply chain", "supply chain coordination",
  // NHS SBS
  "nhs shared business services", "n h s shared business services",
  // Crown Commercial Service
  "crown commercial service",
  // National Procurement Service
  "national procurement service",
  // Fusion21
  "fusion21",
  // LHC (London Housing Consortium)
  "london housing consortium", " lhc group",
  // NEPO (North East Procurement Organisation)
  "north east procurement organisation", " nepo ",
  // Advantage South West
  "advantage south west",
  // Procurement for Housing
  "procurement for housing",
  // Pro5 / Pro4 frameworks
  " pro5 ", " pro4 ",
  // Westworks / Pretium
  "westworks procurement", "pretium frameworks",
  // NEUPC / JISC / HEPCW (higher ed consortia)
  "neupc", "jisc procurement", "hepcw",
  // OJEU aggregators
  "ojeu ltd",
  // Cirrus Purchasing — procurement intermediary for social housing/FM
  "cirrus purchasing",
  // PROSPER — housing association procurement consortium (framework aggregator)
  " prosper ",
  // IN-TEND — e-tendering portal provider, appears as buyer on notices it manages
  "in-tend limited", "intend limited"
];

export function isAggregatorBuyer(buyer: string): boolean {
  const b = ` ${buyer.toLowerCase()} `;
  return AGGREGATOR_FRAGMENTS.some(f => b.includes(f));
}
