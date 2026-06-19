// Contracts You Can Chase Now — deterministic notice scoring, bucketing, and HTML rendering.
// No OpenAI calls. Used from desk pages, notices board, and post-scan report panel.

// ─── Types ───────────────────────────────────────────────────────────────────

export type OpportunityPursuitBucket =
  | "chase_now"
  | "worth_checking"
  | "prepare_first"
  | "partner_route"
  | "watchlist"
  | "low_confidence";

export type OpportunityFitLabel =
  | "Strong fit"
  | "Possible fit"
  | "Prepare first"
  | "Partner route"
  | "Watch"
  | "Low confidence";

export type NormalisedNotice = {
  id: string;
  title: string;
  buyer: string;
  description: string;
  status: string;
  source: string;
  url: string;
  publishedDate: string | null;
  deadlineDate: string | null;
  valueLow: number | null;
  valueHigh: number | null;
  awardedValue: number | null;
  awardedSupplier: string;
  region: string;
  keyword: string;
  suitableForSme: boolean | null;
};

export type ScoredOpportunity = NormalisedNotice & {
  score: number;
  bucket: OpportunityPursuitBucket;
  fitLabel: OpportunityFitLabel;
  matchReasons: string[];
  cautions: string[];
  isNoisy: boolean;
  noisyReason: string;
  suggestedNextAction: string;
  displayValue: string;
  displayDate: string;
};

export type WinBrief = {
  title: string;
  buyer: string;
  source: string;
  url: string;
  value: string;
  deadline: string;
  fitLabel: OpportunityFitLabel;
  bidRecommendation: string;
  whyFits: string[];
  whyCaution: string[];
  likelyDocuments: string[];
  howToApply: string[];
  suggestedWinAngles: string[];
  evidenceGaps: string[];
  nextActions: string[];
};

export type DeskOpportunityContext = {
  type: "desk";
  slug: string;
  label: string;
  keywords: string[];
};

export type ScanOpportunityContext = {
  type: "scan";
  services: string;
  sector: string;
  regions: string;
  idealBuyers: string;
  keywords: string[];
};

export type OpportunityContext = DeskOpportunityContext | ScanOpportunityContext;

// ─── Utilities ───────────────────────────────────────────────────────────────

export function esc(v: unknown): string {
  return String(v ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function fmt(v: number | null | undefined): string {
  if (v == null || v <= 0) return "Not stated";
  if (v >= 1_000_000_000) return `£${(v / 1_000_000_000).toFixed(2)}bn`;
  if (v >= 1_000_000) return `£${(v / 1_000_000).toFixed(1)}m`;
  if (v >= 1_000) return `£${Math.round(v / 1_000)}k`;
  return `£${Math.round(v)}`;
}

function fmtDate(d: string | null | undefined): string {
  if (!d) return "—";
  try {
    return new Date(d).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" });
  } catch {
    return d;
  }
}

function noticeVal(n: NormalisedNotice): number {
  return n.valueHigh ?? n.valueLow ?? n.awardedValue ?? 0;
}

function isOpenNotice(n: NormalisedNotice): boolean {
  const s = (n.status || "").toLowerCase();
  return s === "open" || s === "active" || s === "published";
}

export function buildSafeUrl(url: string): string {
  if (!url) return "#";
  try {
    const u = new URL(url);
    if (u.protocol === "https:" || u.protocol === "http:") return url;
    return "#";
  } catch {
    return "#";
  }
}

export function normaliseFromProcurementNotice(n: {
  id: string; title: string; buyer: string; description: string; status: string;
  source: string; url: string; publishedDate: string | null; deadlineDate: string | null;
  valueLow: number | null; valueHigh: number | null; awardedValue: number | null;
  awardedSupplier: string; region: string; keyword: string; suitableForSme: boolean | null;
}): NormalisedNotice {
  return {
    id: String(n.id || ""),
    title: String(n.title || "").trim(),
    buyer: String(n.buyer || "Not stated").trim(),
    description: String(n.description || "").trim(),
    status: String(n.status || "").trim(),
    source: String(n.source || "Contracts Finder"),
    url: String(n.url || "").trim(),
    publishedDate: n.publishedDate || null,
    deadlineDate: n.deadlineDate || null,
    valueLow: n.valueLow ?? null,
    valueHigh: n.valueHigh ?? null,
    awardedValue: n.awardedValue ?? null,
    awardedSupplier: String(n.awardedSupplier || "").trim(),
    region: String(n.region || "").trim(),
    keyword: String(n.keyword || "").trim(),
    suitableForSme: n.suitableForSme ?? null,
  };
}

// ─── Noise detection ─────────────────────────────────────────────────────────

const NOISE_BY_DESK: Record<string, string[]> = {
  construction: ["marketing", "therapy services", "food bank", "passenger assistant", "erp system", "software licence", "hr services", "payroll", "translation services", "legal advice", "counselling", "social work"],
  recruitment: ["installation", "equipment supply", "building works", "food supply", "transport contract", "construction works", "maintenance services", "software development"],
  education: ["highways maintenance", "housing maintenance", "road resurfacing", "drainage works", "kerbing", "tarmac"],
  facilities: ["christmas lighting", "food bank", "translation", "training course", "erp system", "payroll system", "legal services", "social care"],
  health: ["construction", "building works", "it development", "software development", "marketing", "highways"],
  digital: ["construction", "building works", "cleaning", "catering", "security guarding", "grounds maintenance"],
};

const STRONG_DESK_TERMS: Record<string, string[]> = {
  construction: ["construction", "building", "refurb", "estates", "maintenance works", "roofing", "cladding", "retrofit", "m&e", "plumbing", "electrical works"],
  recruitment: ["recruitment", "staffing", "workforce", "agency", "temporary", "permanent", "locum", "supply teacher"],
  education: ["school", "college", "university", "academy", "education", "training", "learning", "skills"],
  facilities: ["facilities", "hard fm", "soft fm", "caretaking", "cleaning", "security", "waste", "catering", "grounds", "mechanical", "electrical"],
  health: ["nhs", "health", "clinical", "patient", "medical", "care", "hospital", "gp"],
  digital: ["digital", "software", "technology", "data", "cyber", "cloud", "it services", "development"],
};

export function detectNoisyNotice(notice: NormalisedNotice, deskSlug: string): { isNoisy: boolean; reason: string } {
  const text = `${notice.title} ${notice.description}`.toLowerCase();
  const noiseTerms = NOISE_BY_DESK[deskSlug] || [];
  const strongTerms = STRONG_DESK_TERMS[deskSlug] || [];

  const matchedNoise = noiseTerms.filter(t => text.includes(t));
  const hasStrong = strongTerms.some(t => text.includes(t));

  if (matchedNoise.length > 0 && !hasStrong) {
    return { isNoisy: true, reason: `Contains off-sector term: "${matchedNoise[0]}". Weak match against this desk.` };
  }
  return { isNoisy: false, reason: "" };
}

// ─── Scoring ─────────────────────────────────────────────────────────────────

function scoreForDesk(notice: NormalisedNotice, context: DeskOpportunityContext): { score: number; reasons: string[]; cautions: string[] } {
  const title = notice.title.toLowerCase();
  const desc = notice.description.toLowerCase();
  const reasons: string[] = [];
  const cautions: string[] = [];
  let score = 0;

  const titleMatches = context.keywords.filter(kw => title.includes(kw.toLowerCase()));
  const descMatches = context.keywords.filter(kw => !titleMatches.includes(kw) && desc.includes(kw.toLowerCase()));

  if (titleMatches.length > 0) {
    score += Math.min(45, titleMatches.length * 15);
    reasons.push(`Title match: ${titleMatches.slice(0, 3).join(", ")}`);
  }
  if (descMatches.length > 0) {
    score += Math.min(20, descMatches.length * 8);
    reasons.push(`Description match: ${descMatches.slice(0, 2).join(", ")}`);
  }

  if (isOpenNotice(notice)) { score += 15; reasons.push("Open opportunity"); }
  else if (notice.awardedSupplier) { score += 5; reasons.push("Awarded demand signal"); }

  const v = noticeVal(notice);
  if (v > 0) { score += 5; reasons.push("Value stated"); }
  if (notice.deadlineDate) { score += 5; reasons.push("Deadline stated"); }
  if (notice.buyer && notice.buyer !== "Not stated") { score += 5; reasons.push("Named buyer"); }
  if (notice.url) { score += 5; }
  if (notice.suitableForSme === true) { score += 5; reasons.push("SME-suitable flag"); }

  // Noise penalty applied by caller
  if (v > 5_000_000) cautions.push("High value — likely requires established track record.");
  if (notice.source === "Find a Tender" && isOpenNotice(notice)) {
    cautions.push("Above-threshold OJEU tender — may need ESPD, policies, or framework access.");
  }
  if (notice.deadlineDate) {
    const days = Math.floor((new Date(notice.deadlineDate).getTime() - Date.now()) / 86_400_000);
    if (days >= 0 && days <= 7) cautions.push(`Deadline in ${days} day${days === 1 ? "" : "s"} — verify immediately.`);
    else if (days < 0) cautions.push("Deadline may have passed — verify on source.");
  }

  return { score: Math.max(0, Math.min(100, score)), reasons, cautions };
}

function scoreForScan(notice: NormalisedNotice, context: ScanOpportunityContext): { score: number; reasons: string[]; cautions: string[] } {
  const title = notice.title.toLowerCase();
  const desc = notice.description.toLowerCase();
  const text = `${title} ${desc}`;
  const reasons: string[] = [];
  const cautions: string[] = [];
  let score = 0;

  const kwTitleMatches = context.keywords.filter(kw => title.includes(kw.toLowerCase()));
  const kwDescMatches = context.keywords.filter(kw => !kwTitleMatches.includes(kw) && desc.includes(kw.toLowerCase()));

  if (kwTitleMatches.length > 0) {
    score += Math.min(40, kwTitleMatches.length * 15);
    reasons.push(`Keyword in title: ${kwTitleMatches.slice(0, 3).join(", ")}`);
  }
  if (kwDescMatches.length > 0) {
    score += Math.min(15, kwDescMatches.length * 8);
    reasons.push(`Keyword in description: ${kwDescMatches.slice(0, 2).join(", ")}`);
  }

  const serviceTokens = context.services.toLowerCase().split(/[\s,;]+/).filter(t => t.length > 3);
  const svcMatches = serviceTokens.filter(t => text.includes(t));
  if (svcMatches.length > 0) {
    score += Math.min(20, svcMatches.length * 7);
    reasons.push(`Services match: ${svcMatches.slice(0, 2).join(", ")}`);
  }

  if (context.sector && text.includes(context.sector.toLowerCase().split(" ")[0])) {
    score += 8;
    reasons.push(`Sector match: ${context.sector}`);
  }

  if (isOpenNotice(notice)) { score += 15; reasons.push("Open opportunity"); }
  else if (notice.awardedSupplier) { score += 5; reasons.push("Awarded demand signal"); }

  const v = noticeVal(notice);
  if (v > 0) { score += 5; reasons.push("Value stated"); }
  if (notice.deadlineDate) { score += 5; reasons.push("Deadline stated"); }
  if (notice.url) { score += 5; }
  if (notice.suitableForSme === true) { score += 5; reasons.push("SME-suitable"); }

  const buyerTokens = context.idealBuyers.toLowerCase().split(/[\s,;]+/).filter(t => t.length > 3);
  const buyerMatch = buyerTokens.filter(t => notice.buyer.toLowerCase().includes(t));
  if (buyerMatch.length > 0) {
    score += 10;
    reasons.push(`Buyer match: ${buyerMatch[0]}`);
  }

  const regionTokens = context.regions.toLowerCase().split(/[\s,;]+/).filter(t => t.length > 3);
  const regionMatch = regionTokens.filter(t => (notice.region || "").toLowerCase().includes(t) || text.includes(t));
  if (regionMatch.length > 0) {
    score += 8;
    reasons.push(`Region match: ${regionMatch[0]}`);
  }

  if (v > 5_000_000) cautions.push("High value — may need established track record or consortium.");
  if (notice.source === "Find a Tender" && isOpenNotice(notice)) {
    cautions.push("Above-threshold OJEU tender — ESPD or SQ likely required.");
  }
  if (notice.deadlineDate) {
    const days = Math.floor((new Date(notice.deadlineDate).getTime() - Date.now()) / 86_400_000);
    if (days >= 0 && days <= 7) cautions.push(`Deadline in ${days} day${days === 1 ? "" : "s"}.`);
    else if (days < 0) cautions.push("Deadline may have passed — verify live notice.");
  }

  return { score: Math.max(0, Math.min(100, score)), reasons, cautions };
}

// ─── Bucketing ────────────────────────────────────────────────────────────────

function determineBucket(notice: NormalisedNotice, score: number, isNoisy: boolean): OpportunityPursuitBucket {
  if (isNoisy || score < 30) return "low_confidence";

  const v = noticeVal(notice);
  const isOpen = isOpenNotice(notice);
  const text = `${notice.title} ${notice.description}`.toLowerCase();

  const isFrameworkHeavy =
    text.includes("framework agreement") || text.includes("dynamic purchasing") ||
    text.includes(" dps ") || text.includes("approved list") || text.includes("pre-qualification") ||
    text.includes("prior information notice");
  const isTooLarge = v > 10_000_000;

  if (!isOpen && score < 50) return "watchlist";

  if (score >= 55) {
    if (isTooLarge || isFrameworkHeavy) return "partner_route";
    if (score >= 70 && isOpen && notice.url) {
      const missingCritical = !v && !notice.deadlineDate && !notice.buyer;
      return missingCritical ? "worth_checking" : "chase_now";
    }
    const missingData = !v && !notice.deadlineDate;
    if (missingData) return "worth_checking";
    return score >= 65 ? "chase_now" : "prepare_first";
  }

  return score >= 35 ? "watchlist" : "low_confidence";
}

const BUCKET_LABEL: Record<OpportunityPursuitBucket, OpportunityFitLabel> = {
  chase_now: "Strong fit",
  worth_checking: "Possible fit",
  prepare_first: "Prepare first",
  partner_route: "Partner route",
  watchlist: "Watch",
  low_confidence: "Low confidence",
};

function nextActionForBucket(bucket: OpportunityPursuitBucket, isOpen: boolean): string {
  if (bucket === "chase_now") return isOpen ? "Review tender documents and confirm deadline." : "Map buyer and watch for next open notice.";
  if (bucket === "worth_checking") return "Verify notice details and confirm scope before committing.";
  if (bucket === "prepare_first") return "Address evidence or cert gaps, then add to pipeline.";
  if (bucket === "partner_route") return "Identify consortium partner or prime with framework access.";
  if (bucket === "watchlist") return "Track buyer activity for future opportunities.";
  return "Review for sector fit before acting.";
}

// ─── Main export: score and bucket ───────────────────────────────────────────

export function scoreAndBucketNotices(notices: NormalisedNotice[], context: OpportunityContext): ScoredOpportunity[] {
  return notices.map(notice => {
    const { score, reasons, cautions } = context.type === "desk"
      ? scoreForDesk(notice, context)
      : scoreForScan(notice, context);

    const { isNoisy, reason: noisyReason } = context.type === "desk"
      ? detectNoisyNotice(notice, context.slug)
      : { isNoisy: false, reason: "" };

    const adjustedScore = isNoisy ? Math.max(0, score - 25) : score;
    const bucket = determineBucket(notice, adjustedScore, isNoisy);
    const allCautions = isNoisy ? [noisyReason, ...cautions] : cautions;

    return {
      ...notice,
      score: adjustedScore,
      bucket,
      fitLabel: BUCKET_LABEL[bucket],
      matchReasons: reasons,
      cautions: allCautions,
      isNoisy,
      noisyReason,
      suggestedNextAction: nextActionForBucket(bucket, isOpenNotice(notice)),
      displayValue: fmt(noticeVal(notice) || null),
      displayDate: fmtDate(notice.publishedDate),
    };
  }).sort((a, b) => b.score - a.score);
}

// ─── Win Brief builder (post-scan only) ──────────────────────────────────────

export function buildWinBrief(scored: ScoredOpportunity, context: ScanOpportunityContext): WinBrief {
  const v = noticeVal(scored);
  const isOpen = isOpenNotice(scored);

  const likelyDocs: string[] = ["Company registration / certificate of incorporation"];
  if (v > 100_000) likelyDocs.push("Public liability insurance certificate (min £5m)");
  if (v > 250_000) likelyDocs.push("Employer’s liability insurance certificate");
  if (scored.source === "Find a Tender") {
    likelyDocs.push("ESPD (European Single Procurement Document) or SQ");
    likelyDocs.push("Financial accounts (last 2–3 years)");
    likelyDocs.push("Method statement and technical submission");
    likelyDocs.push("Pricing schedule in buyer’s format");
  } else {
    likelyDocs.push("Selection questionnaire (SQ) responses");
    likelyDocs.push("Case studies / references (2–3 examples)");
    likelyDocs.push("Method statement or technical submission");
    likelyDocs.push("Pricing schedule");
  }
  if (v > 1_000_000) {
    likelyDocs.push("ISO 9001 or equivalent quality accreditation");
    likelyDocs.push("Equality & Diversity policy");
    likelyDocs.push("Environmental / sustainability policy");
    likelyDocs.push("Modern Slavery statement (if turnover >£36m or public contract)");
  }

  let bidRec: string;
  if (scored.bucket === "chase_now") bidRec = "Bid. Strong match against stated services and buyer type. Prioritise.";
  else if (scored.bucket === "worth_checking") bidRec = "Review first. Confirm scope and requirements before committing resource.";
  else if (scored.bucket === "prepare_first") bidRec = "Prepare to bid. Address evidence or cert gaps identified below, then bid next cycle.";
  else if (scored.bucket === "partner_route") bidRec = "Do not bid alone. Identify a lead partner or prime contractor with framework access.";
  else bidRec = "Monitor. Not immediately chaseable but worth tracking for buyer intel.";

  const winAngles: string[] = [
    "Lead with concrete, measurable outcomes from comparable contracts.",
    "Reference the buyer’s own stated procurement priorities in your method statement.",
    "Show continuity of service and mobilisation speed — buyers value low handover risk.",
  ];
  const bl = scored.buyer.toLowerCase();
  if (bl.includes("nhs") || bl.includes("health") || bl.includes("clinical") || bl.includes("trust")) {
    winAngles.push("CQC registration, infection control policy, and NHS-compatible processes strengthen credibility.");
  }
  if (bl.includes("council") || bl.includes("authority") || bl.includes("borough") || bl.includes("district")) {
    winAngles.push("Reference local authority contracts of comparable scope and value.");
  }
  if (bl.includes("housing") || bl.includes("homes") || bl.includes("association")) {
    winAngles.push("Show housing-specific compliance experience — WHQS, HHSRS, or equivalent.");
  }

  const evidenceGaps: string[] = [];
  if (!context.keywords.some(kw => scored.title.toLowerCase().includes(kw.toLowerCase()))) {
    evidenceGaps.push("Keyword match is indirect — confirm your services fully cover this scope before bidding.");
  }
  if (v > 1_000_000) {
    evidenceGaps.push("High-value contract: evidence of delivery at this scale significantly strengthens the bid.");
  }
  if (scored.source === "Find a Tender") {
    evidenceGaps.push("OJEU threshold: check for mandatory accreditations, financial standing thresholds, and turnover requirements.");
  }
  evidenceGaps.push("Verify buyer portal — some notices redirect to a separate e-tendering system.");

  const howToApply = [
    "Open the source notice link and confirm it is still live.",
    "Verify the buyer’s portal and check for additional instructions or addenda.",
    "Register on the buyer’s e-tendering system if required.",
    "Download all tender documents and read the specification in full.",
    "Check the submission deadline and any clarification windows.",
    "Complete the selection questionnaire (SQ) accurately and in full.",
    "Prepare your pricing schedule using the buyer’s template.",
    "Prepare your method statement and technical submission.",
    "Upload all required documents before the deadline.",
    "Submit and save your confirmation receipt.",
    "Always verify the live notice and buyer portal before acting.",
  ];

  const nextActions = [
    `Open notice: ${isOpen && scored.url ? scored.url : "verify on source platform"}`,
    "Confirm the submission deadline and any clarification window.",
    "Identify the buyer’s e-tendering portal and register if required.",
    `Review your matched services: ${context.keywords.slice(0, 3).join(", ")}.`,
    "Assign a bid manager or allocate writing resource this week.",
  ];

  return {
    title: scored.title,
    buyer: scored.buyer,
    source: scored.source,
    url: scored.url,
    value: fmt(v || null),
    deadline: scored.deadlineDate ? fmtDate(scored.deadlineDate) : "Not stated",
    fitLabel: scored.fitLabel,
    bidRecommendation: bidRec,
    whyFits: scored.matchReasons.length > 0 ? scored.matchReasons : ["Matches keyword search against this profile."],
    whyCaution: scored.cautions.length > 0 ? scored.cautions : ["Standard procurement risks apply — verify scope before bidding."],
    likelyDocuments: likelyDocs,
    howToApply,
    suggestedWinAngles: winAngles,
    evidenceGaps,
    nextActions,
  };
}

// ─── CSS ─────────────────────────────────────────────────────────────────────

export function oppCardCss(): string {
  return `
.opp-board-section{margin-bottom:40px}
.opp-section-head{display:flex;align-items:baseline;gap:12px;margin-bottom:16px;padding-bottom:10px;border-bottom:2px solid var(--line-strong)}
.opp-section-title{font-family:var(--mono);font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--ink)}
.opp-section-count{font-family:var(--mono);font-size:10px;color:var(--slate)}
.opp-cards{display:grid;gap:12px}
.opp-card{background:var(--paper);border:1px solid var(--line-strong);padding:18px 20px;transition:border-color .15s}
.opp-card:hover{border-color:var(--ink)}
.opp-card--low_confidence{opacity:.8;border-color:var(--line)}
.opp-card-head{display:flex;align-items:center;gap:8px;margin-bottom:10px;flex-wrap:wrap}
.opp-fit-chip{font-family:var(--mono);font-size:9.5px;letter-spacing:.08em;text-transform:uppercase;padding:3px 8px;border-radius:2px;font-weight:600}
.opp-fit-chip--chase_now{background:#e8f5f0;color:#1d6b4f;border:1px solid #1d6b4f44}
.opp-fit-chip--worth_checking{background:#fff8e6;color:#a07820;border:1px solid #a0782044}
.opp-fit-chip--prepare_first{background:#f0f4ff;color:#3a5bb8;border:1px solid #3a5bb844}
.opp-fit-chip--partner_route{background:#f5f0ff;color:#6b3ab8;border:1px solid #6b3ab844}
.opp-fit-chip--watchlist{background:var(--paper-2);color:var(--slate);border:1px solid var(--line-strong)}
.opp-fit-chip--low_confidence{background:var(--paper-2);color:var(--slate);border:1px solid var(--line)}
.opp-status-chip{font-family:var(--mono);font-size:9.5px;letter-spacing:.06em;padding:2px 7px;border-radius:2px}
.opp-status-open{background:#e8f5f0;color:#1d6b4f;border:1px solid #1d6b4f33}
.opp-status-awarded{background:var(--paper-2);color:var(--slate);border:1px solid var(--line-strong)}
.opp-src-badge{font-family:var(--mono);font-size:9px;letter-spacing:.06em;padding:2px 6px;border:1px solid var(--line-strong);color:var(--slate)}
.opp-title{font-family:var(--serif);font-size:15.5px;font-weight:600;margin:0 0 6px;line-height:1.35}
.opp-title a{color:var(--ink);text-decoration:underline;text-underline-offset:3px;text-decoration-color:var(--line-strong)}
.opp-title a:hover{color:var(--accent)}
.opp-buyer{font-size:12.5px;color:var(--slate);margin-bottom:8px}
.opp-meta{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:10px;font-family:var(--mono);font-size:11px;color:var(--slate)}
.opp-meta-val{color:var(--ink);font-weight:500}
.opp-why{font-size:12.5px;color:var(--ink);margin-bottom:6px;line-height:1.5}
.opp-why strong{color:var(--ink)}
.opp-caution{font-size:12px;color:#8a4820;background:#fff8f0;border-left:3px solid #c86020;padding:6px 10px;margin-bottom:8px;line-height:1.5}
.opp-action{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-top:10px;padding-top:10px;border-top:1px solid var(--line)}
.opp-action-hint{font-family:var(--mono);font-size:10.5px;color:var(--slate)}
.opp-cta{font-family:var(--mono);font-size:10.5px;letter-spacing:.08em;text-transform:uppercase;color:var(--accent);text-decoration:underline;text-underline-offset:3px;white-space:nowrap}
.opp-cta:hover{color:var(--ink)}
.opp-cold{padding:24px;background:var(--paper-2);border:1px solid var(--line);color:var(--slate);font-size:14px;line-height:1.6}
.opp-cold strong{color:var(--ink)}
`;
}

export function winBriefCss(): string {
  return `
.wb-panel{background:var(--paper);border:1px solid var(--line-strong);margin-bottom:20px;overflow:hidden}
.wb-head{padding:18px 20px 14px;border-bottom:1px solid var(--line)}
.wb-title{font-family:var(--serif);font-size:15px;font-weight:600;margin:0 0 4px;line-height:1.3}
.wb-buyer{font-size:12.5px;color:var(--slate);margin-bottom:8px}
.wb-head-meta{display:flex;gap:10px;flex-wrap:wrap;align-items:center}
.wb-body{padding:16px 20px}
.wb-section{margin-bottom:16px}
.wb-section-label{font-family:var(--mono);font-size:9.5px;letter-spacing:.12em;text-transform:uppercase;color:var(--slate);margin-bottom:6px}
.wb-rec{font-size:13.5px;font-weight:600;color:var(--ink);padding:10px 14px;background:var(--paper-2);border-left:3px solid var(--accent);margin-bottom:4px}
.wb-list{margin:0;padding-left:18px;font-size:13px;line-height:1.7;color:var(--ink)}
.wb-list li{margin-bottom:2px}
.wb-caution-list{margin:0;padding-left:18px;font-size:13px;line-height:1.7;color:#8a4820}
.wb-apply-list{margin:0;padding:0;list-style:none;counter-reset:apply-counter;font-size:13px;line-height:1.7}
.wb-apply-list li{counter-increment:apply-counter;padding-left:24px;position:relative;margin-bottom:2px}
.wb-apply-list li::before{content:counter(apply-counter);position:absolute;left:0;font-family:var(--mono);font-size:10px;color:var(--slate);top:4px}
.wb-caveat{font-family:var(--mono);font-size:10.5px;color:var(--slate);margin-top:10px;padding-top:10px;border-top:1px solid var(--line)}
.wb-src{font-family:var(--mono);font-size:10.5px}
.wb-src a{color:var(--accent);text-decoration:underline}
`;
}

// ─── Card renderer ────────────────────────────────────────────────────────────

export function renderOpportunityCard(
  scored: ScoredOpportunity,
  opts: { deskSlug?: string; showWinBrief?: boolean; scanContext?: ScanOpportunityContext }
): string {
  const statusClass = isOpenNotice(scored) ? "opp-status-open" : "opp-status-awarded";
  const statusLabel = isOpenNotice(scored) ? "OPEN" : "AWARDED";
  const safeUrl = buildSafeUrl(scored.url);
  const scanParams = opts.deskSlug ? `/scan?desk=${esc(opts.deskSlug)}&noticeId=${esc(scored.id)}` : "/scan";

  const deadlineHtml = scored.deadlineDate
    ? `<span>Deadline: <span class="opp-meta-val">${esc(fmtDate(scored.deadlineDate))}</span></span>`
    : "";

  const cautionHtml = scored.cautions.length > 0
    ? `<div class="opp-caution">${esc(scored.cautions[0])}</div>`
    : "";

  const reasonText = scored.matchReasons.length > 0
    ? scored.matchReasons.slice(0, 2).join(" &middot; ")
    : "Broad keyword match.";

  let winBriefHtml = "";
  if (opts.showWinBrief && opts.scanContext && (scored.bucket === "chase_now" || scored.bucket === "prepare_first")) {
    const wb = buildWinBrief(scored, opts.scanContext);
    winBriefHtml = renderWinBriefHtml(wb);
  }

  return `<article class="opp-card opp-card--${esc(scored.bucket)}">
  <div class="opp-card-head">
    <span class="opp-fit-chip opp-fit-chip--${esc(scored.bucket)}">${esc(scored.fitLabel)}</span>
    <span class="opp-status-chip ${statusClass}">${statusLabel}</span>
    <span class="opp-src-badge">${esc(scored.source === "Find a Tender" ? "FTS" : "CF")}</span>
  </div>
  <h3 class="opp-title">${safeUrl !== "#" ? `<a href="${esc(safeUrl)}" target="_blank" rel="noopener noreferrer">${esc(scored.title.slice(0, 120))}</a>` : esc(scored.title.slice(0, 120))}</h3>
  <div class="opp-buyer">${esc(scored.buyer.slice(0, 60))}</div>
  <div class="opp-meta">
    <span>Value: <span class="opp-meta-val">${esc(scored.displayValue)}</span></span>
    <span>Published: <span class="opp-meta-val">${esc(scored.displayDate)}</span></span>
    ${deadlineHtml}
  </div>
  <div class="opp-why"><strong>Why matched:</strong> ${reasonText}</div>
  ${cautionHtml}
  <div class="opp-action">
    <span class="opp-action-hint">${esc(scored.suggestedNextAction)}</span>
    <a href="${esc(scanParams)}" class="opp-cta">Run fit check &rarr;</a>
  </div>
  ${winBriefHtml}
</article>`;
}

// ─── Win Brief renderer ───────────────────────────────────────────────────────

export function renderWinBriefHtml(wb: WinBrief): string {
  const safeUrl = buildSafeUrl(wb.url);
  return `<div class="wb-panel">
  <div class="wb-head">
    <div class="opp-card-head">
      <span class="opp-fit-chip opp-fit-chip--${fitLabelToBucket(wb.fitLabel)}">${esc(wb.fitLabel)}</span>
      <span class="wb-src">${safeUrl !== "#" ? `<a href="${esc(safeUrl)}" target="_blank" rel="noopener noreferrer">Source notice &rarr;</a>` : "Source not available"}</span>
    </div>
    <h3 class="wb-title">${esc(wb.title.slice(0, 140))}</h3>
    <div class="wb-buyer">${esc(wb.buyer)}</div>
    <div class="wb-head-meta">
      <span class="opp-src-badge">${esc(wb.source === "Find a Tender" ? "FTS" : "CF")}</span>
      ${wb.value !== "Not stated" ? `<span style="font-family:var(--mono);font-size:11px">${esc(wb.value)}</span>` : ""}
      ${wb.deadline !== "Not stated" ? `<span style="font-family:var(--mono);font-size:11px;color:var(--slate)">Deadline: ${esc(wb.deadline)}</span>` : ""}
    </div>
  </div>
  <div class="wb-body">
    <div class="wb-section">
      <div class="wb-section-label">Bid / No-bid recommendation</div>
      <div class="wb-rec">${esc(wb.bidRecommendation)}</div>
    </div>
    <div class="wb-section">
      <div class="wb-section-label">Why this fits</div>
      <ul class="wb-list">${wb.whyFits.map(r => `<li>${esc(r)}</li>`).join("")}</ul>
    </div>
    <div class="wb-section">
      <div class="wb-section-label">Why to be careful</div>
      <ul class="wb-caution-list">${wb.whyCaution.map(r => `<li>${esc(r)}</li>`).join("")}</ul>
    </div>
    <div class="wb-section">
      <div class="wb-section-label">Likely documents needed</div>
      <ul class="wb-list">${wb.likelyDocuments.map(d => `<li>${esc(d)}</li>`).join("")}</ul>
    </div>
    <div class="wb-section">
      <div class="wb-section-label">Suggested win angles</div>
      <ul class="wb-list">${wb.suggestedWinAngles.map(a => `<li>${esc(a)}</li>`).join("")}</ul>
    </div>
    <div class="wb-section">
      <div class="wb-section-label">Evidence gaps to address</div>
      <ul class="wb-caution-list">${wb.evidenceGaps.map(g => `<li>${esc(g)}</li>`).join("")}</ul>
    </div>
    <div class="wb-section">
      <div class="wb-section-label">How to apply (step by step)</div>
      <ol class="wb-apply-list">${wb.howToApply.map(s => `<li>${esc(s)}</li>`).join("")}</ol>
    </div>
    <div class="wb-section">
      <div class="wb-section-label">Next 5 actions</div>
      <ul class="wb-list">${wb.nextActions.slice(0, 5).map(a => `<li>${esc(a)}</li>`).join("")}</ul>
    </div>
    <div class="wb-caveat">Always verify the live notice and buyer portal before acting. Public record only. No insider information. This is a pursuit signal, not a guarantee.</div>
  </div>
</div>`;
}

function fitLabelToBucket(label: OpportunityFitLabel): OpportunityPursuitBucket {
  const map: Record<OpportunityFitLabel, OpportunityPursuitBucket> = {
    "Strong fit": "chase_now",
    "Possible fit": "worth_checking",
    "Prepare first": "prepare_first",
    "Partner route": "partner_route",
    "Watch": "watchlist",
    "Low confidence": "low_confidence",
  };
  return map[label];
}

// ─── Notices board card (new design) ─────────────────────────────────────────

function nbDeadlineChip(dateStr: string | null): string {
  if (!dateStr) return "";
  const days = Math.floor((new Date(dateStr).getTime() - Date.now()) / 86_400_000);
  if (days < 0) return `<span class="nb-chip nb-deadline-red">Deadline passed</span>`;
  if (days <= 7) return `<span class="nb-chip nb-deadline-red">&#9889; ${days}d left</span>`;
  if (days <= 30) return `<span class="nb-chip nb-deadline-amber">&#8987; ${days}d left</span>`;
  return `<span class="nb-chip nb-deadline-ok">&#10003; ${days}d left</span>`;
}

const NB_BUCKET_COLORS: Record<OpportunityPursuitBucket, string> = {
  chase_now:      "#22c55e",
  worth_checking: "#3b82f6",
  prepare_first:  "#f59e0b",
  partner_route:  "#a855f7",
  watchlist:      "#6b7280",
  low_confidence: "#9ca3af",
};

const NB_BUCKET_PRIORITY: Record<OpportunityPursuitBucket, number> = {
  chase_now: 0, worth_checking: 1, prepare_first: 2,
  partner_route: 3, watchlist: 4, low_confidence: 5,
};

function renderNoticesCard(scored: ScoredOpportunity, deskSlug: string): string {
  const safeUrl = buildSafeUrl(scored.url);
  const scanParams = `/scan?desk=${esc(deskSlug)}&noticeId=${esc(scored.id)}`;
  const src = scored.source === "Find a Tender" ? "FTS" : "CF";
  const deadlineEpoch = scored.deadlineDate ? new Date(scored.deadlineDate).getTime() : 0;
  const publishedEpoch = scored.publishedDate ? new Date(scored.publishedDate).getTime() : 0;
  const valueNum = noticeVal(scored);
  const valueStr = valueNum > 0 ? esc(fmt(valueNum)) : (scored.displayValue !== "Not stated" ? esc(scored.displayValue) : "");
  const bucketColor = NB_BUCKET_COLORS[scored.bucket] || "#6b7280";

  const matchTags = scored.matchReasons.slice(0, 3).map(r => {
    const label = r.includes(": ") ? r.split(": ").slice(1).join(": ") : r;
    return `<span class="nb-tag">${esc(label.slice(0, 28))}</span>`;
  }).join("");

  const cautionHtml = scored.cautions.length > 0
    ? `<div class="nb-caution">${esc(scored.cautions[0])}</div>`
    : "";

  return `<article class="nb-card nb-card--${esc(scored.bucket)}"
  data-src="${esc(src)}"
  data-bucket="${esc(scored.bucket)}"
  data-deadline-ts="${deadlineEpoch}"
  data-value="${valueNum}"
  data-published-ts="${publishedEpoch}"
  data-score="${scored.score}">
  <div class="nb-card-top">
    <div class="nb-chips">
      <span class="nb-chip" style="background:${bucketColor}18;color:${bucketColor};border:1px solid ${bucketColor}30;filter:brightness(.85)">${esc(scored.fitLabel)}</span>
      ${nbDeadlineChip(scored.deadlineDate)}
    </div>
    ${valueStr ? `<div class="nb-value">${valueStr}</div>` : ""}
  </div>
  <h3 class="nb-card-title">${safeUrl !== "#" ? `<a href="${esc(safeUrl)}" target="_blank" rel="noopener noreferrer">${esc(scored.title.slice(0, 120))}</a>` : esc(scored.title.slice(0, 120))}</h3>
  <div class="nb-buyer">&#127963; ${esc(scored.buyer.slice(0, 70))}</div>
  <div class="nb-tags">${matchTags}<span class="nb-chip nb-src-chip">${esc(src)}</span></div>
  ${cautionHtml}
  <div class="nb-cta-row">
    <a href="${esc(scanParams)}" class="nb-cta-primary">Run Fit Check &rarr;</a>
    ${safeUrl !== "#" ? `<a href="${esc(safeUrl)}" target="_blank" rel="noopener noreferrer" class="nb-cta-secondary">View &rarr;</a>` : ""}
  </div>
</article>`;
}

// ─── Board renderer (flat paginated grid) ─────────────────────────────────────

export function renderOpportunityBoardContent(
  scored: ScoredOpportunity[],
  deskSlug: string,
  _awardedIntel: ScoredOpportunity[]
): string {
  if (scored.length === 0) {
    return `<div class="nb-empty">No notices found for this desk. Check back after the next data refresh.</div>`;
  }

  const sorted = [...scored].sort((a, b) => {
    const pd = (NB_BUCKET_PRIORITY[a.bucket] ?? 5) - (NB_BUCKET_PRIORITY[b.bucket] ?? 5);
    return pd !== 0 ? pd : b.score - a.score;
  });

  return `<div class="nb-flat-grid" id="nb-grid">
${sorted.map(n => renderNoticesCard(n, deskSlug)).join("\n")}
</div>
<div class="nb-pagination" id="nb-pagination"></div>`;
}

// ─── Post-scan action panel ───────────────────────────────────────────────────

export function renderChaseNowPanel(
  scored: ScoredOpportunity[],
  context: ScanOpportunityContext
): string {
  if (scored.length === 0) return "";

  const chaseNow = scored.filter(s => s.bucket === "chase_now").slice(0, 8);
  const prepareFirst = scored.filter(s => s.bucket === "prepare_first").slice(0, 5);
  const partnerRoute = scored.filter(s => s.bucket === "partner_route").slice(0, 4);
  const watchlist = scored.filter(s => s.bucket === "watchlist").slice(0, 4);
  const ignore = scored.filter(s => s.bucket === "low_confidence" || s.bucket === "worth_checking");

  const renderGroup = (title: string, notices: ScoredOpportunity[], showWinBrief: boolean) => {
    if (notices.length === 0) return "";
    return `<div style="margin-bottom:32px">
  <div style="font-family:var(--mono);font-size:10.5px;letter-spacing:.14em;text-transform:uppercase;color:var(--muted);margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid var(--line)">${esc(title)} <span style="opacity:.6">(${notices.length})</span></div>
  ${notices.map(n => renderOpportunityCard(n, { showWinBrief, scanContext: context })).join("")}
</div>`;
  };

  const ignoreListHtml = ignore.length > 0
    ? `<div style="margin-bottom:24px">
  <div style="font-family:var(--mono);font-size:10.5px;letter-spacing:.14em;text-transform:uppercase;color:var(--muted);margin-bottom:10px">Ignore / low confidence (${ignore.length})</div>
  <ul style="margin:0;padding-left:18px;font-size:12.5px;color:var(--muted);line-height:1.8">
    ${ignore.slice(0, 8).map(n => `<li>${esc(n.title.slice(0, 80))} &mdash; ${esc(n.fitLabel)}</li>`).join("")}
    ${ignore.length > 8 ? `<li>+ ${ignore.length - 8} more low-confidence matches</li>` : ""}
  </ul>
</div>` : "";

  return `<div class="chase-now-panel no-print">
<div class="chase-now-head">
  <div class="chase-eyebrow">CONTRACTS YOU CAN CHASE NOW</div>
  <h2 class="chase-title">Matched opportunities from the public record</h2>
  <p class="chase-sub">Matched from live public notices against your services, regions, capacity, evidence and likely buyer fit. <strong>Matched against the submitted profile. This is a pursuit signal, not a guarantee.</strong></p>
  <p class="chase-caveat">Public record only. No insider information. Verify on the source before acting.</p>
</div>
${renderGroup("Chase now", chaseNow, true)}
${renderGroup("Prepare first", prepareFirst, true)}
${renderGroup("Partner route", partnerRoute, false)}
${renderGroup("Watchlist", watchlist, false)}
${ignoreListHtml}
<p style="font-size:12px;color:var(--muted);font-family:var(--mono);margin-top:16px">Public record only &middot; No insider information &middot; Verify on the source before acting &middot; Matched against the submitted profile</p>
</div>`;
}

// ─── Homepage "Contracts you can chase now" section ───────────────────────────

export type HomepageTeaserSignal = {
  category: string;
  title: string;
  buyer: string | null;
  source: string;
  notice_date: string | null;
  value_amount: number | null;
  status: string | null;
  notice_url?: string | null;
  deadline_date?: string | null;
};

export type ChaseStats = {
  totalOpen: number;
  avgValueK: number | null;
  closingThisMonth: number;
  byDesk: { category: string; count: number }[];
};

function cncValue(v: number | null | undefined): string {
  if (!v || v <= 0) return "";
  if (v >= 1_000_000) return `£${(v / 1_000_000).toFixed(1)}m`;
  if (v >= 1_000) return `£${Math.round(v / 1_000)}k`;
  return `£${Math.round(v)}`;
}

function cncDeskLabel(cat: string): string {
  return cat.replace(/-/g, " ").replace(/\b\w/g, c => c.toUpperCase()).slice(0, 16);
}

function cncDeadlineChip(deadline: string | null | undefined): string {
  if (!deadline) return "";
  const days = Math.ceil((new Date(deadline).getTime() - Date.now()) / 86_400_000);
  if (days <= 0) return "";
  if (days <= 7)  return `<span class="cnc-deadline cnc-deadline--red">Closes ${days}d</span>`;
  if (days <= 30) return `<span class="cnc-deadline cnc-deadline--amber">Closes ${days}d</span>`;
  const wk = Math.ceil(days / 7);
  return `<span class="cnc-deadline cnc-deadline--ok">Closes ${wk}w</span>`;
}

function cncNoticeId(url: string | null | undefined): string {
  if (!url) return "";
  const m = url.match(/\/Notice\/([a-f0-9-]{36})/i);
  return m ? m[1] : "";
}

function cncLabel(sig: HomepageTeaserSignal): string {
  if (sig.source === "FTS" || sig.source === "Find a Tender") return "Framework";
  return "Open now";
}

export function renderChaseNowSection(signals: HomepageTeaserSignal[], stats: ChaseStats): string {
  if (signals.length === 0 && stats.totalOpen === 0) return "";

  const maxVal = Math.max(...signals.map(s => s.value_amount ?? 0), 1);

  const cards = signals.slice(0, 6).map(sig => {
    const label = cncLabel(sig);
    const labelCls = label === "Framework" ? "cnc-chip--framework" : "cnc-chip--open";
    const val = cncValue(sig.value_amount);
    const barPct = sig.value_amount ? Math.max(4, Math.round((sig.value_amount / maxVal) * 100)) : 4;
    const deskLabel = cncDeskLabel(sig.category);
    const srcLabel = sig.source === "Find a Tender" || sig.source === "FTS" ? "FTS" : "CF";
    const noticeId = cncNoticeId(sig.notice_url);
    const scanUrl = `/scan?desk=${esc(sig.category)}${noticeId ? `&noticeId=${esc(noticeId)}` : ""}`;
    const deskUrl = `/desk/${esc(sig.category)}/notices`;
    const deadlineHtml = cncDeadlineChip(sig.deadline_date);

    return `<div class="cnc-card">
  <a href="${deskUrl}" class="cnc-card-bg-link" aria-label="${esc(sig.title.slice(0,60))}"></a>
  <div class="cnc-card-top">
    <span class="cnc-chip ${esc(labelCls)}">${esc(label)}</span>
    <span class="cnc-chip cnc-chip--desk">${esc(deskLabel)}</span>
    <span class="cnc-chip cnc-chip--src">${esc(srcLabel)}</span>
  </div>
  <div class="cnc-card-title">${sig.notice_url
    ? `<a href="${esc(sig.notice_url)}" target="_blank" rel="noopener noreferrer" class="cnc-inner-link">${esc(sig.title.slice(0, 88))}</a>`
    : esc(sig.title.slice(0, 88))}</div>
  <div class="cnc-card-buyer">${esc((sig.buyer || "Buyer not stated").slice(0, 55))}</div>
  <div class="cnc-card-meta">
    ${val ? `<span class="cnc-val">${esc(val)}</span>` : `<span class="cnc-val cnc-val--dim">Value TBC</span>`}
    ${deadlineHtml}
  </div>
  <div class="cnc-val-bar"><div class="cnc-val-fill" style="width:${barPct}%"></div></div>
  <div class="cnc-card-foot">
    <a href="${scanUrl}" class="cnc-fit-cta cnc-inner-link">Run fit check &rarr;</a>
    ${sig.notice_url ? `<a href="${esc(sig.notice_url)}" target="_blank" rel="noopener noreferrer" class="cnc-src-link cnc-inner-link">Source &nearr;</a>` : ""}
  </div>
</div>`;
  }).join("");

  const remaining = Math.max(0, stats.totalOpen - Math.min(signals.length, 6));
  const seeAllCard = `<a class="cnc-card-see-all" href="/desks">
  <div>
    <div class="cnc-see-count">${remaining > 0 ? remaining : stats.totalOpen}</div>
    <div class="cnc-see-label">more open contracts<br>across 24 desks</div>
  </div>
  <div class="cnc-see-cta">Browse all open notices &rarr;</div>
</a>`;

  const maxDesk = Math.max(...stats.byDesk.map(d => d.count), 1);
  const miniChart = stats.byDesk.length > 0
    ? `<div class="cnc-mini-chart">
  <span class="cnc-mini-label">Open notices by desk</span>
  ${stats.byDesk.map(d => `<div class="cnc-bar-row">
  <span class="cnc-bar-desk">${esc(cncDeskLabel(d.category))}</span>
  <div class="cnc-bar-track"><div class="cnc-bar-fill" style="width:${Math.round((d.count/maxDesk)*100)}%"></div></div>
  <span class="cnc-bar-count">${d.count}</span>
</div>`).join("")}
</div>` : "";

  return `<section class="cnc-section">
<div class="cnc-inner">
  <div class="cnc-head">
    <div class="cnc-eyebrow"><span class="cnc-dot"></span>Live public record &middot; Updated hourly</div>
    <h2 class="cnc-title">Contracts you can chase now</h2>
    <p class="cnc-sub">Open public tenders currently accepting bids. Run a fit check to find your matches.</p>
  </div>
  <div class="cnc-stat-bar">
    <div class="cnc-stat"><span class="cnc-stat-label">Open now</span><span class="cnc-stat-val cnc-stat-val--green">${stats.totalOpen || "—"}</span></div>
    <div class="cnc-stat"><span class="cnc-stat-label">Avg. contract size</span><span class="cnc-stat-val">${stats.avgValueK ? `£${stats.avgValueK}k` : "—"}</span></div>
    <div class="cnc-stat"><span class="cnc-stat-label">Closing this month</span><span class="cnc-stat-val">${stats.closingThisMonth || "—"}</span></div>
    ${miniChart}
  </div>
  <div class="cnc-grid">
    ${cards}
    ${seeAllCard}
  </div>
</div>
</section>`;
}

export function chaseNowCss(): string {
  return `
.cnc-section{padding:60px 0;border-top:1px solid var(--line-strong);background:var(--paper)}
.cnc-inner{padding:0 32px}
.cnc-head{margin-bottom:24px}
.cnc-eyebrow{font-family:var(--mono);font-size:10px;letter-spacing:.22em;text-transform:uppercase;color:#1d6b4f;display:flex;align-items:center;gap:6px;margin-bottom:10px}
.cnc-dot{width:6px;height:6px;border-radius:50%;background:#1d6b4f;animation:cnc-pulse 2s ease-in-out infinite;flex-shrink:0}
@keyframes cnc-pulse{0%,100%{opacity:1}50%{opacity:.25}}
.cnc-title{font-family:var(--serif);font-size:30px;font-weight:600;letter-spacing:-.015em;margin-bottom:8px}
.cnc-sub{font-size:15px;color:var(--slate);max-width:38em}
.cnc-stat-bar{display:flex;background:#fff;border:1px solid var(--line-strong);margin-bottom:24px;flex-wrap:wrap}
.cnc-stat{flex:1;min-width:120px;padding:14px 20px;border-right:1px solid rgba(15,20,25,.1)}
.cnc-stat-label{font-family:var(--mono);font-size:9px;letter-spacing:.18em;text-transform:uppercase;color:var(--slate);display:block;margin-bottom:4px}
.cnc-stat-val{font-family:var(--mono);font-size:20px;font-weight:700;color:var(--ink)}
.cnc-stat-val--green{color:#1d6b4f}
.cnc-mini-chart{flex:2;min-width:200px;padding:14px 20px;border-right:1px solid rgba(15,20,25,.1)}
.cnc-mini-label{font-family:var(--mono);font-size:9px;letter-spacing:.18em;text-transform:uppercase;color:var(--slate);display:block;margin-bottom:8px}
.cnc-bar-row{display:flex;align-items:center;gap:6px;margin-bottom:5px}
.cnc-bar-desk{font-family:var(--mono);font-size:9px;color:var(--slate);width:76px;flex-shrink:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.cnc-bar-track{flex:1;height:6px;background:rgba(15,20,25,.07);position:relative}
.cnc-bar-fill{position:absolute;left:0;top:0;bottom:0;background:#1d6b4f}
.cnc-bar-count{font-family:var(--mono);font-size:9px;color:var(--ink);width:20px;text-align:right;flex-shrink:0}
.cnc-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}
.cnc-card{background:#fff;border:1px solid var(--line-strong);padding:18px 20px 42px;transition:border-color .15s,box-shadow .15s;position:relative;overflow:hidden}
.cnc-card:hover{border-color:var(--ink);box-shadow:0 6px 24px -8px rgba(11,15,20,.16)}
.cnc-card-bg-link{position:absolute;inset:0;z-index:1}
.cnc-inner-link{position:relative;z-index:2}
.cnc-card-top{display:flex;align-items:center;gap:6px;margin-bottom:10px;flex-wrap:wrap}
.cnc-chip{font-family:var(--mono);font-size:9px;letter-spacing:.07em;text-transform:uppercase;padding:2px 7px;border-radius:2px;font-weight:600}
.cnc-chip--open{background:#e8f5f0;color:#1d6b4f;border:1px solid rgba(29,107,79,.3)}
.cnc-chip--framework{background:#f5f0ff;color:#6b3ab8;border:1px solid rgba(107,58,184,.3)}
.cnc-chip--desk{background:var(--paper-2);color:var(--slate);border:1px solid var(--line-strong)}
.cnc-chip--src{background:transparent;color:var(--slate);border:1px solid var(--line-strong)}
.cnc-card-title{font-family:var(--serif);font-size:14.5px;font-weight:600;line-height:1.35;margin-bottom:6px;color:var(--ink)}
.cnc-card-title a{color:var(--ink);text-decoration:underline;text-underline-offset:3px;text-decoration-color:var(--line-strong)}
.cnc-card-title a:hover{color:var(--accent)}
.cnc-card-buyer{font-size:12px;color:var(--slate);margin-bottom:10px}
.cnc-card-meta{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:8px}
.cnc-val{font-family:var(--mono);font-size:15px;font-weight:700;color:var(--ink)}
.cnc-val--dim{color:var(--slate)}
.cnc-deadline{font-family:var(--mono);font-size:9.5px;padding:2px 8px;border-radius:2px}
.cnc-deadline--red{background:#fff0f0;color:#c0392b;border:1px solid rgba(192,57,43,.3)}
.cnc-deadline--amber{background:#fff8e6;color:#a07820;border:1px solid rgba(160,120,32,.3)}
.cnc-deadline--ok{background:var(--paper-2);color:var(--slate);border:1px solid var(--line-strong)}
.cnc-val-bar{height:2px;background:rgba(15,20,25,.07);position:absolute;bottom:40px;left:0;right:0}
.cnc-val-fill{height:100%;background:#1d6b4f}
.cnc-card-foot{position:absolute;bottom:0;left:0;right:0;padding:9px 20px;border-top:1px solid rgba(15,20,25,.08);display:flex;align-items:center;justify-content:space-between;background:#fff}
.cnc-fit-cta{font-family:var(--mono);font-size:10px;letter-spacing:.06em;text-transform:uppercase;color:var(--accent);text-decoration:underline;text-underline-offset:3px}
.cnc-fit-cta:hover{color:var(--ink)}
.cnc-src-link{font-family:var(--mono);font-size:10px;color:var(--slate);text-decoration:underline;text-underline-offset:3px}
.cnc-card-see-all{background:var(--ink);border:1px solid var(--ink);padding:24px;display:flex;flex-direction:column;justify-content:space-between;min-height:200px;text-decoration:none;transition:background .15s,border-color .15s}
.cnc-card-see-all:hover{background:var(--accent);border-color:var(--accent)}
.cnc-see-count{font-family:var(--mono);font-size:40px;font-weight:700;color:#fff;line-height:1}
.cnc-see-label{font-family:var(--serif);font-size:14px;color:rgba(255,255,255,.65);margin-top:6px;line-height:1.4}
.cnc-see-cta{font-family:var(--mono);font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:rgba(255,255,255,.5);margin-top:auto;padding-top:20px}
.cnc-see-cta:hover{color:#fff}
@media(max-width:960px){.cnc-grid{grid-template-columns:repeat(2,1fr)}}
@media(max-width:600px){.cnc-grid{grid-template-columns:1fr}.cnc-stat-bar{flex-direction:column}.cnc-mini-chart{border-right:none}.cnc-inner{padding:0 20px}}
`;
}

export function deskOpportunityCss(): string {
  return `
.desk-opp-section{margin:32px 0}
.desk-opp-head{display:flex;align-items:baseline;justify-content:space-between;gap:12px;margin-bottom:6px}
.desk-opp-eyebrow{font-family:var(--mono);font-size:10px;letter-spacing:.18em;text-transform:uppercase;color:var(--slate)}
.desk-opp-link{font-family:var(--mono);font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:var(--accent);text-decoration:underline;text-underline-offset:3px}
.desk-opp-sub{font-size:12.5px;color:var(--slate);margin-bottom:16px}
`;
}

export function reportChaseNowCss(): string {
  return `
.chase-now-panel{margin:32px 0;padding:28px;background:var(--cream);border:1px solid var(--line)}
.chase-now-head{margin-bottom:24px;padding-bottom:16px;border-bottom:1px solid var(--line)}
.chase-eyebrow{font-family:var(--mono,monospace);font-size:10px;letter-spacing:.18em;text-transform:uppercase;color:var(--muted,#6f5b50);margin-bottom:8px}
.chase-title{font-family:var(--serif,Georgia,serif);font-size:22px;font-weight:600;margin:0 0 8px}
.chase-sub{font-size:14px;line-height:1.6;color:var(--ink,#24140f);margin-bottom:6px}
.chase-caveat{font-family:var(--mono,monospace);font-size:11px;color:var(--muted,#6f5b50)}
`;
}

export function noticesBoardCss(): string {
  return `
/* ── Notices board stats pills ─────────────────────────── */
.nb-stats{display:flex;gap:12px;margin-top:20px;flex-wrap:wrap}
.nb-stat-pill{background:var(--paper-2);border:1px solid var(--line-strong);border-radius:6px;padding:12px 18px;display:flex;align-items:center;gap:12px;flex:1;min-width:140px}
.nb-stat-icon{width:30px;height:30px;border-radius:6px;display:flex;align-items:center;justify-content:center;font-size:15px;flex-shrink:0}
.nb-stat-num{font-family:var(--serif);font-size:22px;font-weight:700;letter-spacing:-.02em;line-height:1;display:flex;align-items:center;gap:6px}
.nb-stat-label{font-family:var(--mono);font-size:10px;letter-spacing:.06em;text-transform:uppercase;color:var(--slate);margin-top:3px}
.nb-pulse{width:6px;height:6px;border-radius:50%;background:#22c55e;display:inline-block;box-shadow:0 0 0 0 rgba(34,197,94,.4);animation:nb-pulse 2s infinite;flex-shrink:0}
@keyframes nb-pulse{0%,100%{box-shadow:0 0 0 0 rgba(34,197,94,.4)}50%{box-shadow:0 0 0 5px rgba(34,197,94,0)}}

/* ── Board wrapper ──────────────────────────────────────── */
.nb-board-wrap{padding:0 48px 64px}

/* ── Filter bar ─────────────────────────────────────────── */
.nb-filter-bar{display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:18px 0 16px;border-bottom:1px solid var(--line-strong);margin-bottom:20px}
.nb-filter-label{font-family:var(--mono);font-size:11px;color:var(--slate);letter-spacing:.04em;white-space:nowrap}
.nb-filter-btn{padding:5px 14px;border-radius:20px;border:1px solid var(--line-strong);background:transparent;color:var(--slate);font-family:var(--mono);font-size:11px;cursor:pointer;letter-spacing:.03em;transition:border-color .15s,color .15s}
.nb-filter-btn:hover{border-color:var(--ink);color:var(--ink)}
.nb-filter-btn.nb-active{border-color:var(--accent);color:var(--accent);background:rgba(155,44,44,.06)}
.nb-filter-sep{width:1px;height:16px;background:var(--line-strong);flex-shrink:0;margin:0 4px}
.nb-sort-select{background:var(--paper);border:1px solid var(--line-strong);color:var(--ink);border-radius:4px;padding:5px 10px;font-family:var(--mono);font-size:11px;cursor:pointer;letter-spacing:.03em}
.nb-action-btn{font-family:var(--mono);font-size:11px;letter-spacing:.04em;padding:5px 14px;border-radius:20px;border:1px solid var(--ink);background:var(--ink);color:var(--paper);text-decoration:none;white-space:nowrap;transition:background .15s,border-color .15s}
.nb-action-btn:hover{background:var(--accent);border-color:var(--accent)}

/* ── Main content ───────────────────────────────────────── */
.nb-disclaimer{font-family:var(--mono);font-size:11px;color:var(--slate);margin-bottom:28px}

/* ── Flat grid + pagination ──────────────────────────────── */
.nb-flat-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}
.nb-pagination{display:flex;align-items:center;justify-content:space-between;margin-top:28px;padding-top:20px;border-top:1px solid var(--line-strong);font-family:var(--mono);font-size:12px;color:var(--slate)}
.nb-pg-info{letter-spacing:.03em}
.nb-pg-btns{display:flex;gap:8px}
.nb-pg-btn{padding:7px 16px;border:1px solid var(--line-strong);background:var(--paper);color:var(--ink);cursor:pointer;font-family:var(--mono);font-size:11px;letter-spacing:.04em;transition:.15s}
.nb-pg-btn:hover:not(:disabled){border-color:var(--ink)}
.nb-pg-btn:disabled{opacity:.35;cursor:default}
.nb-pg-nums{display:flex;gap:4px}
.nb-pg-num{padding:6px 10px;border:1px solid var(--line-strong);background:var(--paper);color:var(--slate);cursor:pointer;font-family:var(--mono);font-size:11px;transition:.15s;min-width:34px;text-align:center}
.nb-pg-num:hover{border-color:var(--ink);color:var(--ink)}
.nb-pg-num.nb-pg-active{background:var(--ink);color:var(--paper);border-color:var(--ink)}

/* ── Card grid (kept for post-scan panel compat) ─────────── */
.nb-card-grid{display:grid;gap:14px}
.nb-card-grid--2col{grid-template-columns:1fr 1fr}
.nb-card-grid--1col{grid-template-columns:1fr}

/* ── Card ────────────────────────────────────────────────── */
.nb-card{background:var(--paper);border:1px solid var(--line-strong);border-left:3px solid var(--line-strong);padding:16px;transition:border-color .15s,box-shadow .15s}
.nb-card:hover{border-top-color:var(--ink);border-right-color:var(--ink);border-bottom-color:var(--ink);box-shadow:0 2px 10px rgba(11,15,20,.07)}
.nb-card--chase_now{border-left-color:#22c55e}
.nb-card--worth_checking{border-left-color:#3b82f6}
.nb-card--prepare_first{border-left-color:#f59e0b}
.nb-card--partner_route{border-left-color:#a855f7}
.nb-card--watchlist{border-left-color:var(--line-strong);opacity:.88}
.nb-card--low_confidence{border-left-color:var(--line);opacity:.75}

.nb-card-top{display:flex;justify-content:space-between;align-items:flex-start;gap:10px;margin-bottom:10px}
.nb-chips{display:flex;gap:5px;flex-wrap:wrap;flex:1}
.nb-chip{font-family:var(--mono);font-size:9.5px;font-weight:600;letter-spacing:.05em;text-transform:uppercase;padding:2px 8px;border-radius:10px}
.nb-deadline-red{background:rgba(185,28,28,.08);color:#b91c1c;border:1px solid rgba(185,28,28,.18)}
.nb-deadline-amber{background:rgba(146,64,14,.08);color:#92400e;border:1px solid rgba(146,64,14,.18)}
.nb-deadline-ok{background:rgba(22,101,52,.06);color:#166534;border:1px solid rgba(22,101,52,.15)}
.nb-fit-chip{background:rgba(155,44,44,.07);color:var(--accent);border:1px solid rgba(155,44,44,.14)}
.nb-src-chip{background:var(--paper-2);color:var(--slate);border:1px solid var(--line-strong)}
.nb-value{font-family:var(--serif);font-size:18px;font-weight:700;letter-spacing:-.02em;color:var(--ink);white-space:nowrap;flex-shrink:0}
.nb-card--watchlist .nb-value,.nb-card--low_confidence .nb-value{font-size:14px;color:var(--slate)}

.nb-card-title{font-family:var(--serif);font-size:14.5px;font-weight:600;line-height:1.35;margin:0 0 7px;color:var(--ink)}
.nb-card-title a{color:var(--ink);text-decoration:underline;text-underline-offset:3px;text-decoration-color:var(--line-strong)}
.nb-card-title a:hover{color:var(--accent)}
.nb-buyer{font-size:12px;color:var(--slate);margin-bottom:10px}
.nb-tags{display:flex;gap:5px;flex-wrap:wrap;margin-bottom:10px;align-items:center}
.nb-tag{font-family:var(--mono);font-size:10px;padding:2px 8px;border-radius:3px;background:var(--paper-2);color:var(--slate);border:1px solid var(--line-strong)}
.nb-caution{font-size:11.5px;color:#92400e;background:#fffbeb;border-left:2px solid #f59e0b;padding:6px 10px;margin-bottom:10px;line-height:1.5}

.nb-cta-row{display:flex;align-items:center;gap:8px;margin-top:12px;padding-top:12px;border-top:1px solid var(--line)}
.nb-cta-primary{flex:1;text-align:center;font-family:var(--mono);font-size:11px;letter-spacing:.06em;text-transform:uppercase;padding:9px 14px;background:var(--ink);color:var(--paper);border:1px solid var(--ink);transition:background .15s,border-color .15s;white-space:nowrap}
.nb-cta-primary:hover{background:var(--accent);border-color:var(--accent)}
.nb-card--chase_now .nb-cta-primary{background:#15803d;border-color:#15803d;color:#fff}
.nb-card--chase_now .nb-cta-primary:hover{background:#166534;border-color:#166534}
.nb-cta-secondary{font-family:var(--mono);font-size:11px;letter-spacing:.04em;text-transform:uppercase;padding:8px 12px;border:1px solid var(--line-strong);color:var(--slate);transition:border-color .15s,color .15s;white-space:nowrap}
.nb-cta-secondary:hover{border-color:var(--ink);color:var(--ink)}
.nb-empty{padding:24px;background:var(--paper-2);border:1px solid var(--line);color:var(--slate);font-family:var(--mono);font-size:12px;line-height:1.6}

/* ── Responsive ──────────────────────────────────────────── */
@media(max-width:1100px){
  .nb-board-wrap{padding:0 20px 48px}
  .nb-card-grid--2col{grid-template-columns:1fr}
  .nb-flat-grid{grid-template-columns:repeat(2,1fr)}
}
@media(max-width:760px){
  .nb-stats{flex-direction:column}
  .nb-stat-pill{min-width:unset}
  .nb-flat-grid{grid-template-columns:1fr}
  .nb-action-btn{display:none}
}
`;
}
