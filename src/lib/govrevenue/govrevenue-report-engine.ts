/* eslint-disable @typescript-eslint/no-explicit-any */

/**
 * GovRevenue Report Engine
 * Trust-first procurement intelligence engine.
 *
 * Fixes:
 * 1. Blocks leaked template variables before PDF export.
 * 2. Uses sector-specific search keywords.
 * 3. Separates pulled, relevant, addressable, verified, and noise records.
 * 4. Calculates realistic addressable value instead of leading with raw totals.
 * 5. Labels every buyer/opportunity by evidence confidence.
 * 6. Quarantines irrelevant records.
 * 7. Requires source IDs/URLs for major recommendations unless clearly marked strategic.
 * 8. Produces table-safe premium HTML.
 * 9. Adds clean chart data only after relevance filtering.
 * 10. Generates sharp 30-day action plan.
 */

export type EvidenceLabel =
  | "VERIFIED_RECORD"
  | "PULLED_RECORD"
  | "INFERRED"
  | "STRATEGIC_TARGET"
  | "NOT_CONFIRMED"
  | "LOW_RELEVANCE_NOISE";

export type ProcurementStatus =
  | "open"
  | "award"
  | "closed"
  | "planned"
  | "unknown";

export type RouteType =
  | "direct_bid"
  | "framework"
  | "dps"
  | "partner"
  | "monitor_only"
  | "ignore";

export interface CompanyIntake {
  companyName: string;
  website?: string;
  sector: string;
  sectorLens?: string;
  baseLocation?: string;
  regions: string[];
  services: string[];
  secondaryServices?: string[];
  excludedServices?: string[];
  idealBuyerTypes?: string[];
  idealContractMin?: number;
  idealContractMax?: number;
  maxDeliverableContractValue?: number;
  currentTeamSize?: number;
  publicSectorExperience?: "none" | "some" | "strong" | string;
  accreditations?: string[];
  insuranceConfirmed?: boolean;
  tupeReady?: boolean;
  mobilisationReady?: boolean;
  caseStudiesConfirmed?: boolean;
  mainGoal?: string;
  biggestConcern?: string;
}

export interface ProcurementRecord {
  id: string;
  source: "Contracts Finder" | "Find a Tender" | "Manual" | "Web" | string;
  sourceUrl?: string;
  title: string;
  buyerName?: string;
  supplierName?: string;
  status?: ProcurementStatus | string;
  description?: string;
  region?: string;
  publishedDate?: string;
  deadline?: string;
  awardDate?: string;
  startDate?: string;
  endDate?: string;
  value?: number;
  valueLow?: number;
  valueHigh?: number;
  cpvCodes?: string[];
  raw?: any;
}

export interface ScoredRecord extends ProcurementRecord {
  relevanceScore: number;
  evidenceLabel: EvidenceLabel;
  routeType: RouteType;
  sectorMatchReasons: string[];
  riskReasons: string[];
  conservativeValue: number;
  displayValue: string;
  isRelevant: boolean;
  isAddressable: boolean;
  isOpenOpportunity: boolean;
  isAwardSignal: boolean;
}

export interface SectorConfig {
  key: string;
  label: string;
  sectorLens: string;
  coreKeywords: string[];
  specialistKeywords: string[];
  buyerKeywords: string[];
  frameworkKeywords: string[];
  negativeKeywords: string[];
  forbiddenCarryoverKeywords: string[];
  cpvHints: string[];
  buyerTypes: Array<{
    buyerType: string;
    fit: "High" | "Medium" | "Low";
    spendLogic: string;
    bestEntryRoute: string;
    priority: "High" | "Medium" | "Low";
  }>;
  routeTemplates: Array<{
    route: string;
    routeType: RouteType;
    baseScore: number;
    whyMoneyExists: string;
    bestActionThisWeek: string;
  }>;
}

export interface BuyerWatchItem {
  buyer: string;
  whyTheyMayBuy: string;
  serviceToPitch: string;
  bestRoute: string;
  sourceConfidence: EvidenceLabel;
  evidenceUrls: string[];
  nextAction: string;
  score: number;
}

export interface OpportunityItem {
  opportunity: string;
  likelyBuyer: string;
  sourceConfidence: EvidenceLabel;
  whyTheySpend: string;
  route: RouteType;
  actionabilityScore: number;
  evidenceNeeded: string;
  nextActionThisWeek: string;
  evidenceUrls: string[];
}

export interface ValueSummary {
  totalPulledRecordValue: number;
  relevantPulledRecordValue: number;
  addressableOpportunityValue: number;
  verifiedOpenOpportunityValue: number;
  awardBenchmarkValue: number;
  largestPulledRecordValue: number;
  largestRelevantRecordValue: number;
}

export interface DataQualitySummary {
  level: "Strong" | "Moderate" | "Weak" | "Unsafe";
  averageRelevanceScore: number;
  pulledRecords: number;
  relevantRecords: number;
  quarantinedNoiseRecords: number;
  openRecords: number;
  awardSignals: number;
  distinctBuyers: number;
  namedSuppliers: number;
  warnings: string[];
}

export interface QaResult {
  passed: boolean;
  errors: string[];
  warnings: string[];
}

export interface ReportModel {
  intake: CompanyIntake;
  sectorConfig: SectorConfig;
  generatedAt: string;
  searchQueries: string[];
  pulledRecords: ScoredRecord[];
  relevantRecords: ScoredRecord[];
  noiseRecords: ScoredRecord[];
  openRelevantRecords: ScoredRecord[];
  awardSignalRecords: ScoredRecord[];
  buyerWatchlist: BuyerWatchItem[];
  opportunities: OpportunityItem[];
  valueSummary: ValueSummary;
  dataQuality: DataQualitySummary;
  evidenceGaps: Array<{
    asset: string;
    status: "Green" | "Amber" | "Red";
    sourceConfidence: EvidenceLabel;
    whyItMatters: string;
    fixThisWeek: string;
  }>;
  thirtyDayPlan: Array<{
    week: string;
    focus: string;
    actions: string[];
    output: string;
  }>;
  qa: QaResult;
}

export class GovRevenueQualityGateError extends Error {
  public readonly qa: QaResult;

  constructor(qa: QaResult) {
    super(`GovRevenue quality gate failed: ${qa.errors.join(" | ")}`);
    this.name = "GovRevenueQualityGateError";
    this.qa = qa;
  }
}

const GBP = new Intl.NumberFormat("en-GB", {
  style: "currency",
  currency: "GBP",
  maximumFractionDigits: 0,
});

const NUMBER = new Intl.NumberFormat("en-GB", {
  maximumFractionDigits: 0,
});

const CLEANING_CONFIG: SectorConfig = {
  key: "cleaning",
  label: "Specialist cleaning and facilities hygiene",
  sectorLens:
    "Specialist cleaning, healthcare deep cleaning, education facilities cleaning, local authority estate cleaning and reactive hygiene support",
  coreKeywords: [
    "cleaning services",
    "specialist cleaning",
    "deep cleaning",
    "clinical cleaning",
    "healthcare cleaning",
    "infection control cleaning",
    "public facilities cleaning",
    "estate cleaning",
    "school cleaning",
    "academy cleaning",
    "office cleaning",
    "communal cleaning",
    "reactive cleaning",
    "emergency cleaning",
    "kitchen deep cleaning",
    "bio fogging",
    "sanitisation",
    "disinfection",
    "environmental cleaning",
    "facilities cleaning",
  ],
  specialistKeywords: [
    "operating theatre cleaning",
    "icu cleaning",
    "ward cleaning",
    "nhs deep cleaning",
    "hospital cleaning",
    "medical centre cleaning",
    "dental surgery cleaning",
    "mortuary cleaning",
    "catering equipment cleaning",
    "extract canopy cleaning",
    "grease filter cleaning",
    "infection prevention",
    "healthcare estates cleaning",
  ],
  buyerKeywords: [
    "nhs trust",
    "integrated care board",
    "integrated care system",
    "hospital",
    "local authority",
    "city council",
    "borough council",
    "county council",
    "academy trust",
    "school",
    "university",
    "housing association",
    "care home",
    "medical centre",
    "dental practice",
    "public estate",
  ],
  frameworkKeywords: [
    "framework",
    "dynamic purchasing system",
    "dps",
    "facilities management",
    "soft facilities management",
    "estates services",
    "maintenance and cleaning",
  ],
  negativeKeywords: [
    "building surveying",
    "condition survey",
    "estate consultancy",
    "asset management consultancy",
    "property consultancy",
    "quantity surveying",
    "road traffic survey",
    "transport survey",
    "architectural services",
    "structural engineering",
    "mechanical engineering consultancy",
    "legal services",
    "children partnership",
  ],
  forbiddenCarryoverKeywords: [
    "building surveying",
    "condition survey",
    "estate consultancy",
    "asset management",
    "property consultancy",
    "built asset consultancy",
  ],
  cpvHints: [
    "90910000",
    "90911000",
    "90911200",
    "90911300",
    "90919000",
    "90919200",
    "90919300",
    "90920000",
    "98341130",
  ],
  buyerTypes: [
    {
      buyerType: "NHS Trusts",
      fit: "High",
      spendLogic:
        "Clinical safety, infection control, planned deep cleans and emergency hygiene needs.",
      bestEntryRoute: "Direct tender / NHS framework / specialist subcontract",
      priority: "High",
    },
    {
      buyerType: "Local Authorities",
      fit: "Medium",
      spendLogic:
        "Public estate hygiene, civic buildings, communal facilities and reactive cleaning requirements.",
      bestEntryRoute: "Direct tender / DPS / local procurement portal",
      priority: "Medium",
    },
    {
      buyerType: "Academy Trusts and Schools",
      fit: "Medium",
      spendLogic:
        "Recurring site cleaning, termly deep cleans, kitchens, hygiene compliance and urgent remediation.",
      bestEntryRoute: "Direct tender / trust-level procurement",
      priority: "Medium",
    },
    {
      buyerType: "Universities and Colleges",
      fit: "Medium",
      spendLogic:
        "Large estates, student facilities, labs, kitchens, accommodation and periodic deep cleans.",
      bestEntryRoute: "Direct tender / framework",
      priority: "Medium",
    },
    {
      buyerType: "Housing Associations",
      fit: "Medium",
      spendLogic:
        "Communal areas, void-property cleans, estate hygiene and reactive environmental cleaning.",
      bestEntryRoute: "DPS / framework / direct contract",
      priority: "Medium",
    },
    {
      buyerType: "Private Medical Centres",
      fit: "Low",
      spendLogic:
        "Smaller specialist cleaning demand but often fragmented and less visible in public data.",
      bestEntryRoute: "Direct outreach",
      priority: "Low",
    },
  ],
  routeTemplates: [
    {
      route: "NHS specialist healthcare deep-cleaning",
      routeType: "direct_bid",
      baseScore: 86,
      whyMoneyExists:
        "NHS estates require clinical hygiene, infection-control support, planned deep cleans and urgent reactive cleaning.",
      bestActionThisWeek:
        "Map NHS trusts within the target region and collect live/expired deep-cleaning notices with buyer contacts.",
    },
    {
      route: "Education and academy trust cleaning",
      routeType: "direct_bid",
      baseScore: 72,
      whyMoneyExists:
        "Schools and academy trusts buy daily cleaning, periodic deep cleans, kitchen cleans and reactive hygiene services.",
      bestActionThisWeek:
        "Create a regional academy-trust target list and search for cleaning contract renewal dates.",
    },
    {
      route: "Local authority estate and communal cleaning",
      routeType: "dps",
      baseScore: 68,
      whyMoneyExists:
        "Councils manage civic buildings, depots, libraries, housing estates and public facilities with recurring cleaning needs.",
      bestActionThisWeek:
        "Register on target council procurement portals and identify cleaning, FM and estate-service DPS routes.",
    },
    {
      route: "Soft FM / cleaning frameworks",
      routeType: "framework",
      baseScore: 62,
      whyMoneyExists:
        "Frameworks aggregate repeat public-sector demand and can unlock multi-buyer access.",
      bestActionThisWeek:
        "Check whether cleaning lots are open, relevant and realistically accessible for the company’s size.",
    },
    {
      route: "Reactive and emergency cleaning",
      routeType: "direct_bid",
      baseScore: 58,
      whyMoneyExists:
        "Outbreaks, contamination, floods, void properties and emergency compliance issues create urgent demand.",
      bestActionThisWeek:
        "Prepare a rapid-response capability statement, mobilisation proof and risk-assessment pack.",
    },
  ],
};

const GENERIC_CONFIG: SectorConfig = {
  key: "generic",
  label: "Public-sector services",
  sectorLens:
    "Public-sector demand, buyer fit, supplier readiness and procurement route analysis",
  coreKeywords: ["public sector services", "contract opportunity", "framework"],
  specialistKeywords: [],
  buyerKeywords: ["local authority", "nhs", "school", "university"],
  frameworkKeywords: ["framework", "dps", "dynamic purchasing system"],
  negativeKeywords: [],
  forbiddenCarryoverKeywords: [],
  cpvHints: [],
  buyerTypes: [],
  routeTemplates: [],
};

export function generateGovRevenueReport(input: {
  intake: CompanyIntake;
  rawRecords: ProcurementRecord[];
  strict?: boolean;
}): {
  model: ReportModel;
  html: string;
  qa: QaResult;
} {
  const strict = input.strict ?? true;
  const intake = normaliseIntake(input.intake);
  const sectorConfig = detectSectorConfig(intake);
  const searchQueries = buildSectorSearchQueries(intake, sectorConfig);

  assertSearchQueriesMatchSector(searchQueries, sectorConfig);

  const pulledRecords = input.rawRecords.map((record) =>
    scoreAndClassifyRecord(record, intake, sectorConfig),
  );

  const relevantRecords = pulledRecords
    .filter((record) => record.isRelevant)
    .sort(sortByScoreThenValue);

  const noiseRecords = pulledRecords
    .filter((record) => !record.isRelevant)
    .sort(sortByLowestScore);

  const openRelevantRecords = relevantRecords.filter(
    (record) => record.isOpenOpportunity,
  );

  const awardSignalRecords = relevantRecords.filter(
    (record) => record.isAwardSignal,
  );

  const valueSummary = calculateValueSummary({
    pulledRecords,
    relevantRecords,
    openRelevantRecords,
    awardSignalRecords,
  });

  const buyerWatchlist = buildBuyerWatchlist({
    intake,
    sectorConfig,
    relevantRecords,
  });

  const opportunities = buildOpportunityMap({
    intake,
    sectorConfig,
    relevantRecords,
    openRelevantRecords,
    awardSignalRecords,
  });

  const evidenceGaps = buildEvidenceGaps(intake);

  const dataQuality = buildDataQualitySummary({
    pulledRecords,
    relevantRecords,
    noiseRecords,
    openRelevantRecords,
    awardSignalRecords,
  });

  const thirtyDayPlan = buildThirtyDayPlan({
    intake,
    sectorConfig,
    opportunities,
    evidenceGaps,
  });

  const baseModel: Omit<ReportModel, "qa"> = {
    intake,
    sectorConfig,
    generatedAt: new Date().toISOString(),
    searchQueries,
    pulledRecords,
    relevantRecords,
    noiseRecords,
    openRelevantRecords,
    awardSignalRecords,
    buyerWatchlist,
    opportunities,
    valueSummary,
    dataQuality,
    evidenceGaps,
    thirtyDayPlan,
  };

  const html = renderPremiumReportHtml(baseModel);
  const qa = runQualityGate(baseModel, html);
  const model: ReportModel = { ...baseModel, qa };

  if (strict && !qa.passed) {
    throw new GovRevenueQualityGateError(qa);
  }

  return { model, html, qa };
}

function normaliseIntake(intake: CompanyIntake): CompanyIntake {
  const cleaned: CompanyIntake = {
    ...intake,
    companyName: requiredCleanText(intake.companyName, "companyName"),
    sector: requiredCleanText(intake.sector, "sector"),
    sectorLens: cleanText(intake.sectorLens),
    website: cleanText(intake.website),
    baseLocation: cleanText(intake.baseLocation),
    regions: uniqueCleanArray(intake.regions),
    services: uniqueCleanArray(intake.services),
    secondaryServices: uniqueCleanArray(intake.secondaryServices ?? []),
    excludedServices: uniqueCleanArray(intake.excludedServices ?? []),
    idealBuyerTypes: uniqueCleanArray(intake.idealBuyerTypes ?? []),
    accreditations: uniqueCleanArray(intake.accreditations ?? []),
    mainGoal: cleanText(intake.mainGoal),
    biggestConcern: cleanText(intake.biggestConcern),
  };

  if (!cleaned.regions.length) cleaned.regions = ["United Kingdom"];
  if (!cleaned.services.length) cleaned.services = [cleaned.sector];

  return cleaned;
}

function detectSectorConfig(intake: CompanyIntake): SectorConfig {
  const sectorText = normalise(
    [
      intake.sector,
      intake.sectorLens,
      ...intake.services,
      ...(intake.secondaryServices ?? []),
    ].join(" "),
  );

  if (
    sectorText.includes("clean") ||
    sectorText.includes("hygiene") ||
    sectorText.includes("sanitis") ||
    sectorText.includes("disinfect") ||
    sectorText.includes("deep clean")
  ) {
    return CLEANING_CONFIG;
  }

  return GENERIC_CONFIG;
}

function buildSectorSearchQueries(
  intake: CompanyIntake,
  sectorConfig: SectorConfig,
): string[] {
  const regions = intake.regions.length ? intake.regions : ["United Kingdom"];
  const buyerSeeds = [
    ...sectorConfig.buyerKeywords.slice(0, 8),
    ...(intake.idealBuyerTypes ?? []),
  ];

  const serviceSeeds = [
    ...intake.services,
    ...(intake.secondaryServices ?? []),
    ...sectorConfig.coreKeywords,
    ...sectorConfig.specialistKeywords.slice(0, 8),
  ];

  const queries: string[] = [];

  for (const region of regions) {
    for (const service of serviceSeeds.slice(0, 18)) {
      queries.push(`${service} ${region}`);
    }

    for (const buyer of buyerSeeds.slice(0, 8)) {
      queries.push(`${buyer} cleaning ${region}`);
    }

    for (const framework of sectorConfig.frameworkKeywords.slice(0, 5)) {
      queries.push(`${framework} cleaning ${region}`);
    }
  }

  return uniqueCleanArray(queries)
    .filter((query) => query.length >= 4)
    .slice(0, 40);
}

function assertSearchQueriesMatchSector(
  queries: string[],
  sectorConfig: SectorConfig,
): void {
  const joined = normalise(queries.join(" "));

  const forbidden = sectorConfig.forbiddenCarryoverKeywords.filter((keyword) =>
    joined.includes(normalise(keyword)),
  );

  if (forbidden.length) {
    throw new Error(
      `Sector keyword contamination detected. Remove wrong carryover keywords: ${forbidden.join(
        ", ",
      )}`,
    );
  }
}

function scoreAndClassifyRecord(
  record: ProcurementRecord,
  intake: CompanyIntake,
  sectorConfig: SectorConfig,
): ScoredRecord {
  const title = cleanText(record.title);
  const description = cleanText(record.description);
  const buyerName = cleanText(record.buyerName);
  const supplierName = cleanText(record.supplierName);
  const sourceUrl = cleanText(record.sourceUrl);
  const region = cleanText(record.region);
  const cpvCodes = uniqueCleanArray(record.cpvCodes ?? []);

  const text = normalise(
    [
      title,
      description,
      buyerName,
      supplierName,
      region,
      cpvCodes.join(" "),
      record.status ?? "",
    ].join(" "),
  );

  const sectorMatchReasons: string[] = [];
  const riskReasons: string[] = [];

  let score = 0;

  const coreMatches = matchedTerms(text, sectorConfig.coreKeywords);
  const specialistMatches = matchedTerms(text, sectorConfig.specialistKeywords);
  const buyerMatches = matchedTerms(text, sectorConfig.buyerKeywords);
  const frameworkMatches = matchedTerms(text, sectorConfig.frameworkKeywords);
  const negativeMatches = matchedTerms(text, sectorConfig.negativeKeywords);

  if (coreMatches.length) {
    score += Math.min(34, coreMatches.length * 8);
    sectorMatchReasons.push(`Core cleaning match: ${coreMatches.slice(0, 4).join(", ")}`);
  }

  if (specialistMatches.length) {
    score += Math.min(30, specialistMatches.length * 10);
    sectorMatchReasons.push(
      `Specialist match: ${specialistMatches.slice(0, 4).join(", ")}`,
    );
  }

  if (buyerMatches.length) {
    score += Math.min(18, buyerMatches.length * 5);
    sectorMatchReasons.push(`Buyer match: ${buyerMatches.slice(0, 4).join(", ")}`);
  }

  if (frameworkMatches.length) {
    score += Math.min(12, frameworkMatches.length * 4);
    sectorMatchReasons.push(
      `Framework route match: ${frameworkMatches.slice(0, 3).join(", ")}`,
    );
  }

  const cpvMatches = cpvCodes.filter((cpv) =>
    sectorConfig.cpvHints.some((hint) => cpv.startsWith(hint)),
  );

  if (cpvMatches.length) {
    score += 22;
    sectorMatchReasons.push(`Cleaning CPV hint: ${cpvMatches.slice(0, 3).join(", ")}`);
  }

  const intakeServiceMatches = matchedTerms(text, intake.services);
  if (intakeServiceMatches.length) {
    score += Math.min(16, intakeServiceMatches.length * 6);
    sectorMatchReasons.push(
      `Client service match: ${intakeServiceMatches.slice(0, 3).join(", ")}`,
    );
  }

  const regionMatches = matchedTerms(text, intake.regions);
  if (regionMatches.length) {
    score += 6;
    sectorMatchReasons.push(`Region match: ${regionMatches.slice(0, 2).join(", ")}`);
  }

  if (sourceUrl || record.id) {
    score += 4;
  }

  if (negativeMatches.length) {
    const penalty = Math.min(45, negativeMatches.length * 14);
    score -= penalty;
    riskReasons.push(`Low-relevance / wrong-sector signal: ${negativeMatches.join(", ")}`);
  }

  const excludedMatches = matchedTerms(text, intake.excludedServices ?? []);
  if (excludedMatches.length) {
    score -= 18;
    riskReasons.push(`Client excluded service match: ${excludedMatches.join(", ")}`);
  }

  score = clamp(Math.round(score), 0, 100);

  const status = normaliseStatus(record.status);
  const routeType = classifyRouteType(text);
  const conservativeValue = getConservativeValue(record);
  const maxDeliverable = intake.maxDeliverableContractValue ?? intake.idealContractMax ?? 700_000;

  const isOpenOpportunity =
    status === "open" || status === "planned" || hasFutureDeadline(record.deadline);

  const isAwardSignal = status === "award" || Boolean(record.awardDate) || Boolean(record.supplierName);

  const hasDirectSource = Boolean(sourceUrl || record.id);
  const hasCleaningEvidence = coreMatches.length > 0 || specialistMatches.length > 0 || cpvMatches.length > 0;

  let evidenceLabel: EvidenceLabel;

  if (score >= 72 && hasDirectSource && hasCleaningEvidence) {
    evidenceLabel = "VERIFIED_RECORD";
  } else if (score >= 55 && hasDirectSource) {
    evidenceLabel = "PULLED_RECORD";
  } else if (score >= 45) {
    evidenceLabel = "INFERRED";
  } else {
    evidenceLabel = "LOW_RELEVANCE_NOISE";
  }

  const isRelevant = score >= 55 && evidenceLabel !== "LOW_RELEVANCE_NOISE";

  const withinCapacity =
    conservativeValue === 0 ||
    conservativeValue <= maxDeliverable ||
    routeType === "partner" ||
    routeType === "framework";

  const isAddressable =
    isRelevant &&
    withinCapacity &&
    routeType !== "ignore" &&
    evidenceLabel !== "LOW_RELEVANCE_NOISE";

  if (isRelevant && !withinCapacity) {
    riskReasons.push(
      `Value may exceed current direct capacity: ${formatMoney(
        conservativeValue,
      )} vs max deliverable ${formatMoney(maxDeliverable)}`,
    );
  }

  return {
    ...record,
    title,
    description,
    buyerName,
    supplierName,
    sourceUrl,
    region,
    status,
    cpvCodes,
    relevanceScore: score,
    evidenceLabel,
    routeType,
    sectorMatchReasons: sectorMatchReasons.length
      ? sectorMatchReasons
      : ["No strong sector match found"],
    riskReasons,
    conservativeValue,
    displayValue: conservativeValue ? formatMoney(conservativeValue) : "Unknown",
    isRelevant,
    isAddressable,
    isOpenOpportunity,
    isAwardSignal,
  };
}

function classifyRouteType(text: string): RouteType {
  if (text.includes("dynamic purchasing system") || text.includes(" dps ")) return "dps";
  if (text.includes("framework")) return "framework";
  if (text.includes("subcontract") || text.includes("partner")) return "partner";

  const ignoreSignals = [
    "road traffic survey",
    "condition survey",
    "building surveying",
    "legal services",
    "children partnership",
  ];

  if (ignoreSignals.some((signal) => text.includes(signal))) return "ignore";

  return "direct_bid";
}

function calculateValueSummary(input: {
  pulledRecords: ScoredRecord[];
  relevantRecords: ScoredRecord[];
  openRelevantRecords: ScoredRecord[];
  awardSignalRecords: ScoredRecord[];
}): ValueSummary {
  const totalPulledRecordValue = sumValues(input.pulledRecords);
  const relevantPulledRecordValue = sumValues(input.relevantRecords);

  const addressableOpportunityValue = sumValues(
    input.openRelevantRecords.filter((record) => record.isAddressable),
  );

  const verifiedOpenOpportunityValue = sumValues(
    input.openRelevantRecords.filter(
      (record) =>
        record.isAddressable &&
        ["VERIFIED_RECORD", "PULLED_RECORD"].includes(record.evidenceLabel),
    ),
  );

  const awardBenchmarkValue = sumValues(
    input.awardSignalRecords.filter((record) =>
      ["VERIFIED_RECORD", "PULLED_RECORD"].includes(record.evidenceLabel),
    ),
  );

  return {
    totalPulledRecordValue,
    relevantPulledRecordValue,
    addressableOpportunityValue,
    verifiedOpenOpportunityValue,
    awardBenchmarkValue,
    largestPulledRecordValue: maxValue(input.pulledRecords),
    largestRelevantRecordValue: maxValue(input.relevantRecords),
  };
}

function buildBuyerWatchlist(input: {
  intake: CompanyIntake;
  sectorConfig: SectorConfig;
  relevantRecords: ScoredRecord[];
}): BuyerWatchItem[] {
  const grouped = new Map<string, ScoredRecord[]>();

  for (const record of input.relevantRecords) {
    if (!record.buyerName) continue;
    const key = record.buyerName;
    const existing = grouped.get(key) ?? [];
    existing.push(record);
    grouped.set(key, existing);
  }

  const fromRecords: BuyerWatchItem[] = Array.from(grouped.entries()).map(
    ([buyer, records]) => {
      const best = records.sort(sortByScoreThenValue)[0];
      const urls = uniqueCleanArray(records.map((record) => record.sourceUrl ?? ""));

      return {
        buyer,
        whyTheyMayBuy: best.sectorMatchReasons[0] ?? "Relevant procurement signal found.",
        serviceToPitch: chooseServiceToPitch(input.intake, best),
        bestRoute: humanRoute(best.routeType),
        sourceConfidence: best.evidenceLabel,
        evidenceUrls: urls,
        nextAction: nextActionForBuyer(buyer, best),
        score: best.relevanceScore,
      };
    },
  );

  const strategicTargets: BuyerWatchItem[] = input.sectorConfig.buyerTypes
    .filter(
      (target) =>
        !fromRecords.some((item) =>
          normalise(item.buyer).includes(normalise(target.buyerType)),
        ),
    )
    .slice(0, 5)
    .map((target) => ({
      buyer: target.buyerType,
      whyTheyMayBuy: target.spendLogic,
      serviceToPitch: chooseDefaultService(input.intake),
      bestRoute: target.bestEntryRoute,
      sourceConfidence: "STRATEGIC_TARGET",
      evidenceUrls: [],
      nextAction: `Build a named target list for ${target.buyerType} in ${input.intake.regions.join(
        ", ",
      )}.`,
      score: target.priority === "High" ? 76 : target.priority === "Medium" ? 62 : 45,
    }));

  return [...fromRecords, ...strategicTargets]
    .sort((a, b) => b.score - a.score)
    .slice(0, 12);
}

function buildOpportunityMap(input: {
  intake: CompanyIntake;
  sectorConfig: SectorConfig;
  relevantRecords: ScoredRecord[];
  openRelevantRecords: ScoredRecord[];
  awardSignalRecords: ScoredRecord[];
}): OpportunityItem[] {
  const opportunities: OpportunityItem[] = [];

  for (const route of input.sectorConfig.routeTemplates) {
    const routeRecords = input.relevantRecords.filter((record) => {
      const text = normalise(
        [record.title, record.description, record.buyerName, record.source].join(" "),
      );

      if (route.route.toLowerCase().includes("nhs")) {
        return text.includes("nhs") || text.includes("hospital") || text.includes("healthcare");
      }

      if (route.route.toLowerCase().includes("education")) {
        return text.includes("school") || text.includes("academy") || text.includes("education");
      }

      if (route.route.toLowerCase().includes("local authority")) {
        return text.includes("council") || text.includes("local authority");
      }

      if (route.route.toLowerCase().includes("framework")) {
        return record.routeType === "framework" || record.routeType === "dps";
      }

      if (route.route.toLowerCase().includes("reactive")) {
        return text.includes("reactive") || text.includes("emergency");
      }

      return false;
    });

    const bestRecord = routeRecords.sort(sortByScoreThenValue)[0];
    const evidenceUrls = bestRecord
      ? uniqueCleanArray(routeRecords.map((record) => record.sourceUrl ?? "")).slice(0, 4)
      : [];

    const evidenceLabel: EvidenceLabel = bestRecord
      ? bestRecord.evidenceLabel
      : "STRATEGIC_TARGET";

    const evidenceBoost = bestRecord ? Math.min(12, Math.round(bestRecord.relevanceScore / 10)) : 0;
    const actionabilityScore = clamp(route.baseScore + evidenceBoost, 0, 100);

    opportunities.push({
      opportunity: route.route,
      likelyBuyer: inferLikelyBuyer(route.route),
      sourceConfidence: evidenceLabel,
      whyTheySpend: route.whyMoneyExists,
      route: route.routeType,
      actionabilityScore,
      evidenceNeeded: bestRecord
        ? "Confirm renewal timing, eligibility criteria, buyer contact and exact cleaning scope."
        : "Find source-backed notices, buyer pages or framework lots before treating this as a live opportunity.",
      nextActionThisWeek: route.bestActionThisWeek,
      evidenceUrls,
    });
  }

  return opportunities.sort((a, b) => b.actionabilityScore - a.actionabilityScore);
}

function buildEvidenceGaps(intake: CompanyIntake): ReportModel["evidenceGaps"] {
  return [
    {
      asset: "Team size and delivery capacity",
      status: intake.currentTeamSize ? "Green" : "Red",
      sourceConfidence: intake.currentTeamSize ? "NOT_CONFIRMED" : "NOT_CONFIRMED",
      whyItMatters:
        "Public buyers need confidence that the supplier can mobilise staff, manage sites and sustain service levels.",
      fixThisWeek:
        "Confirm current headcount, supervisor structure, subcontractor access and maximum simultaneous site capacity.",
    },
    {
      asset: "Insurance limits and accreditations",
      status:
        intake.insuranceConfirmed && (intake.accreditations?.length ?? 0) > 0
          ? "Green"
          : "Amber",
      sourceConfidence: "NOT_CONFIRMED",
      whyItMatters:
        "Many public-sector cleaning contracts require documented insurance, health and safety, COSHH and compliance evidence.",
      fixThisWeek:
        "Collect insurance schedule, H&S policy, COSHH documents, risk assessments, training matrix and accreditation certificates.",
    },
    {
      asset: "TUPE readiness",
      status: intake.tupeReady ? "Green" : "Red",
      sourceConfidence: "NOT_CONFIRMED",
      whyItMatters:
        "Cleaning contracts often involve incumbent staff transfer risk, especially in schools, councils and facilities management.",
      fixThisWeek:
        "Confirm whether the company can handle TUPE due diligence, staffing transfer and employment-cost modelling.",
    },
    {
      asset: "Mobilisation capability",
      status: intake.mobilisationReady ? "Green" : "Amber",
      sourceConfidence: "NOT_CONFIRMED",
      whyItMatters:
        "Reactive cleaning, healthcare cleaning and multi-site contracts require fast mobilisation and documented operating procedures.",
      fixThisWeek:
        "Prepare mobilisation plan, escalation route, equipment list, response times and named operations owner.",
    },
    {
      asset: "Case studies and buyer proof",
      status: intake.caseStudiesConfirmed ? "Green" : "Amber",
      sourceConfidence: "NOT_CONFIRMED",
      whyItMatters:
        "Good public-sector bids need proof, not claims. Relevant case studies make recommendations more credible.",
      fixThisWeek:
        "Create two one-page case studies with problem, site type, service delivered, risk controlled and outcome.",
    },
  ];
}

function buildDataQualitySummary(input: {
  pulledRecords: ScoredRecord[];
  relevantRecords: ScoredRecord[];
  noiseRecords: ScoredRecord[];
  openRelevantRecords: ScoredRecord[];
  awardSignalRecords: ScoredRecord[];
}): DataQualitySummary {
  const averageRelevanceScore =
    input.pulledRecords.length === 0
      ? 0
      : Math.round(
          input.pulledRecords.reduce((sum, record) => sum + record.relevanceScore, 0) /
            input.pulledRecords.length,
        );

  const distinctBuyers = new Set(
    input.pulledRecords.map((record) => normalise(record.buyerName ?? "")).filter(Boolean),
  ).size;

  const namedSuppliers = new Set(
    input.pulledRecords.map((record) => normalise(record.supplierName ?? "")).filter(Boolean),
  ).size;

  const relevanceRatio =
    input.pulledRecords.length === 0
      ? 0
      : input.relevantRecords.length / input.pulledRecords.length;

  const warnings: string[] = [];

  if (input.pulledRecords.length === 0) {
    warnings.push("No procurement records were pulled.");
  }

  if (relevanceRatio < 0.35 && input.pulledRecords.length > 0) {
    warnings.push(
      "Large share of pulled records were low relevance. Treat the scan as directionally useful until keywords are refined.",
    );
  }

  if (input.openRelevantRecords.length === 0) {
    warnings.push(
      "No source-backed open opportunities were found. Use award signals and buyer strategy, not live opportunity claims.",
    );
  }

  let level: DataQualitySummary["level"] = "Moderate";

  if (input.pulledRecords.length === 0) level = "Unsafe";
  else if (averageRelevanceScore >= 70 && relevanceRatio >= 0.55) level = "Strong";
  else if (averageRelevanceScore >= 50 && relevanceRatio >= 0.35) level = "Moderate";
  else level = "Weak";

  return {
    level,
    averageRelevanceScore,
    pulledRecords: input.pulledRecords.length,
    relevantRecords: input.relevantRecords.length,
    quarantinedNoiseRecords: input.noiseRecords.length,
    openRecords: input.openRelevantRecords.length,
    awardSignals: input.awardSignalRecords.length,
    distinctBuyers,
    namedSuppliers,
    warnings,
  };
}

function buildThirtyDayPlan(input: {
  intake: CompanyIntake;
  sectorConfig: SectorConfig;
  opportunities: OpportunityItem[];
  evidenceGaps: ReportModel["evidenceGaps"];
}): ReportModel["thirtyDayPlan"] {
  const topOpportunity = input.opportunities[0];
  const urgentGaps = input.evidenceGaps.filter((gap) => gap.status !== "Green").slice(0, 3);

  return [
    {
      week: "Week 1",
      focus: "Trust and bid-readiness evidence",
      actions: [
        "Confirm team size, insurance, accreditations, TUPE readiness and mobilisation limits.",
        ...urgentGaps.map((gap) => gap.fixThisWeek),
      ].slice(0, 5),
      output: "Verified supplier evidence pack and red/amber/green readiness view.",
    },
    {
      week: "Week 2",
      focus: "Source-backed buyer mapping",
      actions: [
        `Build a named buyer list across ${input.intake.regions.join(", ")}.`,
        "Separate buyers into verified records, inferred buyers and strategic targets.",
        "Collect source URLs, procurement portals and renewal clues for each priority buyer.",
      ],
      output: "Buyer watchlist with source confidence labels and next contact action.",
    },
    {
      week: "Week 3",
      focus: topOpportunity
        ? `Opportunity activation: ${topOpportunity.opportunity}`
        : "Opportunity activation",
      actions: [
        topOpportunity?.nextActionThisWeek ??
          "Identify one high-fit route and collect evidence before outreach.",
        "Prepare a one-page capability statement tailored to the top buyer type.",
        "Prepare a public-buyer intro email and partner/subcontractor message.",
      ],
      output: "Route-specific outreach pack and target-buyer messaging.",
    },
    {
      week: "Week 4",
      focus: "Bid / framework / partner decision",
      actions: [
        "Decide which records to bid, partner, monitor or ignore.",
        "Prioritise open opportunities within realistic contract capacity.",
        "Move low-relevance records to monitoring only; do not let them drive the commercial recommendation.",
      ],
      output: "30-day route-to-revenue decision sheet.",
    },
  ];
}

function runQualityGate(
  model: Omit<ReportModel, "qa">,
  html: string,
): QaResult {
  const errors: string[] = [];
  const warnings: string[] = [];

  const htmlLeaks = findTemplateLeaks(html);
  const modelLeaks = findTemplateLeaks(JSON.stringify(model));

  if (htmlLeaks.length || modelLeaks.length) {
    errors.push(
      `Template leakage detected before PDF export: ${uniqueCleanArray([
        ...htmlLeaks,
        ...modelLeaks,
      ]).join(", ")}`,
    );
  }

  const queryText = normalise(model.searchQueries.join(" "));
  const forbidden = model.sectorConfig.forbiddenCarryoverKeywords.filter((keyword) =>
    queryText.includes(normalise(keyword)),
  );

  if (forbidden.length) {
    errors.push(
      `Wrong-sector search keywords detected for ${model.sectorConfig.key}: ${forbidden.join(
        ", ",
      )}`,
    );
  }

  if (model.sectorConfig.key === "cleaning" && !queryText.includes("clean")) {
    errors.push("Cleaning sector scan does not include cleaning-specific search terms.");
  }

  if (model.relevantRecords.some((record) => record.evidenceLabel === "LOW_RELEVANCE_NOISE")) {
    errors.push("Low-relevance noise leaked into relevant records.");
  }

  for (const opportunity of model.opportunities.slice(0, 5)) {
    const isSourceBacked = opportunity.evidenceUrls.length > 0;
    const isStrategic = opportunity.sourceConfidence === "STRATEGIC_TARGET";

    if (!isSourceBacked && !isStrategic) {
      errors.push(
        `Major opportunity lacks source URL and is not marked strategic: ${opportunity.opportunity}`,
      );
    }
  }

  for (const buyer of model.buyerWatchlist.slice(0, 8)) {
    const isSourceBacked = buyer.evidenceUrls.length > 0;
    const isStrategic = buyer.sourceConfidence === "STRATEGIC_TARGET";

    if (!isSourceBacked && !isStrategic) {
      errors.push(`Buyer lacks evidence URL and is not marked strategic: ${buyer.buyer}`);
    }
  }

  if (
    model.valueSummary.totalPulledRecordValue > 0 &&
    model.valueSummary.addressableOpportunityValue >
      model.valueSummary.totalPulledRecordValue
  ) {
    errors.push("Addressable opportunity value cannot exceed total pulled-record value.");
  }

  if (model.valueSummary.totalPulledRecordValue > 0 && model.valueSummary.relevantPulledRecordValue === 0) {
    warnings.push(
      "Records were pulled, but no relevant value was found. Report must avoid revenue claims.",
    );
  }

  if (model.dataQuality.level === "Unsafe") {
    errors.push("Data quality is unsafe. Do not ship a client report.");
  }

  if (model.dataQuality.level === "Weak") {
    warnings.push(
      "Data quality is weak. Report can be generated only with visible caveats and source confidence labels.",
    );
  }

  if (!html.includes("Addressable opportunity value")) {
    errors.push("Report must show addressable opportunity value.");
  }

  if (!html.toLowerCase().includes("low-relevance noise quarantine")) {
    errors.push("Report must include a low-relevance noise quarantine section.");
  }

  if (!html.includes("Who to contact") || !html.includes("What to send")) {
    errors.push("Report must end with sharper action guidance: who to contact and what to send.");
  }

  return {
    passed: errors.length === 0,
    errors,
    warnings,
  };
}

function renderPremiumReportHtml(model: Omit<ReportModel, "qa">): string {
  const intake = model.intake;
  const sectorLens = intake.sectorLens || model.sectorConfig.sectorLens;

  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>${escapeHtml(intake.companyName)} — GovRevenue Scan</title>
<style>
  :root {
    --ink: #1f1512;
    --muted: #6f625d;
    --line: #dfd1bd;
    --paper: #fffaf1;
    --card: #ffffff;
    --accent: #a8752a;
    --accent-soft: #f2e5cf;
    --danger: #8d2c23;
    --amber: #a8752a;
    --green: #2e6f4e;
    --dark: #24130f;
  }

  * { box-sizing: border-box; }

  body {
    margin: 0;
    background: #f6efe4;
    color: var(--ink);
    font-family: Inter, Arial, Helvetica, sans-serif;
    line-height: 1.45;
  }

  .page {
    width: 1120px;
    margin: 0 auto;
    padding: 54px;
    background:
      radial-gradient(circle at top right, rgba(168,117,42,.12), transparent 34%),
      var(--paper);
  }

  .cover {
    border: 1px solid var(--line);
    padding: 48px;
    background: rgba(255,255,255,.58);
  }

  h1, h2, h3 {
    font-family: Georgia, 'Times New Roman', serif;
    color: var(--ink);
    margin: 0;
  }

  h1 {
    font-size: 58px;
    line-height: 1;
    letter-spacing: -1.4px;
    margin-bottom: 20px;
  }

  h2 {
    font-size: 34px;
    margin: 48px 0 18px;
    border-top: 1px solid var(--line);
    padding-top: 28px;
  }

  h3 {
    font-size: 22px;
    margin: 24px 0 10px;
  }

  p {
    font-size: 16px;
    color: var(--muted);
    margin: 0 0 12px;
  }

  .lede {
    font-size: 24px;
    max-width: 900px;
    color: var(--muted);
  }

  .meta {
    margin-top: 34px;
    padding: 22px 26px;
    border-left: 7px solid var(--accent);
    background: #fff;
  }

  .grid {
    display: grid;
    gap: 18px;
  }

  .grid-2 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .grid-3 { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .grid-4 { grid-template-columns: repeat(4, minmax(0, 1fr)); }

  .card {
    background: var(--card);
    border: 1px solid var(--line);
    padding: 20px;
    min-height: 112px;
  }

  .metric-label {
    text-transform: uppercase;
    letter-spacing: .1em;
    color: var(--muted);
    font-size: 12px;
    font-weight: 800;
  }

  .metric-value {
    display: block;
    margin-top: 8px;
    font-family: Georgia, 'Times New Roman', serif;
    font-size: 30px;
    font-weight: 800;
    color: var(--ink);
  }

  .metric-note {
    display: block;
    margin-top: 4px;
    color: var(--muted);
    font-size: 13px;
  }

  .badge {
    display: inline-block;
    border-radius: 999px;
    padding: 5px 10px;
    font-size: 11px;
    font-weight: 800;
    letter-spacing: .04em;
    text-transform: uppercase;
    border: 1px solid var(--line);
    background: #fff;
    white-space: nowrap;
  }

  .badge-verified { color: var(--green); border-color: rgba(46,111,78,.35); }
  .badge-pulled { color: #315b82; border-color: rgba(49,91,130,.35); }
  .badge-inferred { color: var(--amber); border-color: rgba(168,117,42,.35); }
  .badge-strategic { color: #6c4a91; border-color: rgba(108,74,145,.35); }
  .badge-noise { color: var(--danger); border-color: rgba(141,44,35,.35); }

  .warning {
    border: 1px solid rgba(141,44,35,.28);
    background: #fff4f1;
    color: var(--danger);
    padding: 16px 18px;
    margin: 16px 0;
    font-weight: 650;
  }

  .good {
    border: 1px solid rgba(46,111,78,.25);
    background: #f1faf5;
    color: var(--green);
    padding: 16px 18px;
    margin: 16px 0;
    font-weight: 650;
  }

  .table-wrap {
    width: 100%;
    overflow: hidden;
    border: 1px solid var(--line);
    background: #fff;
    margin: 16px 0 28px;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    table-layout: fixed;
  }

  th {
    background: var(--dark);
    color: #fff;
    text-align: left;
    padding: 12px;
    font-size: 12px;
    line-height: 1.25;
    vertical-align: top;
  }

  td {
    padding: 12px;
    border-top: 1px solid var(--line);
    vertical-align: top;
    font-size: 12.5px;
    line-height: 1.35;
    color: #2d2421;
    word-break: normal;
    overflow-wrap: break-word;
    hyphens: none;
  }

  .url {
    font-size: 10.5px;
    color: #315b82;
    word-break: break-all;
    overflow-wrap: anywhere;
  }

  .chart {
    background: #fff;
    border: 1px solid var(--line);
    padding: 20px;
    margin: 16px 0;
  }

  .bar-row {
    display: grid;
    grid-template-columns: 240px 1fr 82px;
    gap: 12px;
    align-items: center;
    margin: 12px 0;
  }

  .bar-label {
    font-size: 13px;
    font-weight: 700;
    color: var(--ink);
  }

  .bar-track {
    height: 14px;
    background: var(--accent-soft);
    border-radius: 999px;
    overflow: hidden;
  }

  .bar-fill {
    height: 100%;
    background: var(--accent);
    border-radius: 999px;
  }

  .bar-value {
    font-size: 12px;
    font-weight: 800;
    color: var(--muted);
    text-align: right;
  }

  .action-card {
    background: #fff;
    border: 1px solid var(--line);
    padding: 20px;
  }

  .action-card ul {
    margin: 12px 0 0 18px;
    padding: 0;
  }

  .action-card li {
    margin: 0 0 8px;
    color: #2d2421;
    font-size: 14px;
  }

  .small {
    color: var(--muted);
    font-size: 12px;
  }

  .footer {
    margin-top: 48px;
    padding-top: 20px;
    border-top: 1px solid var(--line);
    color: var(--muted);
    font-size: 11px;
  }

  @media print {
    body { background: #fff; }
    .page { width: auto; padding: 36px; }
    h2 { break-after: avoid; }
    .card, .table-wrap, .chart, .action-card { break-inside: avoid; }
  }
</style>
</head>

<body>
<main class="page">
  <section class="cover">
    <h1>GovRevenue Scan</h1>
    <p class="lede">
      Commercial public-sector opportunity scan for <strong>${escapeHtml(
        intake.companyName,
      )}</strong>.
      Built from intake data, procurement records, source confidence labels, relevance scoring and human-verification rules.
    </p>

    <div class="grid grid-4" style="margin-top:34px;">
      ${metricCard("Status", "QA gated", "Template, source and relevance checks applied")}
      ${metricCard("Pulled records", NUMBER.format(model.dataQuality.pulledRecords), "All raw procurement matches")}
      ${metricCard("Relevant records", NUMBER.format(model.dataQuality.relevantRecords), "Records above sector relevance threshold")}
      ${metricCard("Route", model.opportunities[0]?.opportunity ?? "Manual review", "Recommended first route")}
    </div>

    <div class="meta">
      <p><strong>Sector lens:</strong> ${escapeHtml(sectorLens)}</p>
      <p><strong>Regions searched:</strong> ${escapeHtml(intake.regions.join(", "))}</p>
      <p><strong>Keywords searched:</strong> ${escapeHtml(model.searchQueries.slice(0, 12).join("; "))}</p>
      <p><strong>Generated:</strong> ${escapeHtml(formatDate(model.generatedAt))}</p>
      <p><strong>Data quality:</strong> ${escapeHtml(model.dataQuality.level)} — ${escapeHtml(
        model.dataQuality.warnings[0] ??
          "Recommendations are tied to pulled records, verified sources, or clearly marked strategic targets.",
      )}</p>
    </div>
  </section>

  <h2>Executive Evidence Dashboard</h2>
  <p>
    This dashboard separates raw data volume from realistic commercial opportunity.
    Pulled-record value is not treated as addressable revenue.
  </p>

  <div class="grid grid-3">
    ${metricCard(
      "Total pulled-record value",
      formatMoney(model.valueSummary.totalPulledRecordValue),
      "Raw value before relevance filtering",
    )}
    ${metricCard(
      "Relevant pulled-record value",
      formatMoney(model.valueSummary.relevantPulledRecordValue),
      "Sector-relevant records only",
    )}
    ${metricCard(
      "Addressable opportunity value",
      formatMoney(model.valueSummary.addressableOpportunityValue),
      "Open, relevant and realistically chaseable",
    )}
    ${metricCard(
      "Verified open opportunity value",
      formatMoney(model.valueSummary.verifiedOpenOpportunityValue),
      "Open source-backed opportunity value",
    )}
    ${metricCard(
      "Award benchmark value",
      formatMoney(model.valueSummary.awardBenchmarkValue),
      "Historical awards useful for market proof",
    )}
    ${metricCard(
      "Noise quarantined",
      NUMBER.format(model.dataQuality.quarantinedNoiseRecords),
      "Low-relevance records excluded from recommendations",
    )}
  </div>

  ${renderWarnings(model)}

  <h2>Money Route Ranking</h2>
  <p>
    Ranked by evidence strength, buyer fit, actionability and realistic route-to-revenue.
  </p>
  ${renderRouteChart(model.opportunities)}
  ${renderOpportunityTable(model.opportunities)}

  <h2>Buyer Fit Matrix</h2>
  <p>
    Buyer types are separated from named buyer records. Strategic targets are useful, but they are not presented as verified opportunities.
  </p>
  ${renderBuyerTypeTable(model.sectorConfig)}

  <h2>Named Buyer Watchlist</h2>
  <p>
    Named buyers below are labelled by evidence confidence. Buyers without source URLs are treated as strategic targets, not confirmed opportunities.
  </p>
  ${renderBuyerWatchlistTable(model.buyerWatchlist)}

  <h2>Source-Backed Contract and Award Signals</h2>
  <p>
    These are the strongest pulled records after sector relevance scoring. Low-relevance pulled records are quarantined separately.
  </p>
  ${renderRecordTable(model.relevantRecords.slice(0, 12))}

  <h2>Low-Relevance Noise Quarantine</h2>
  <p>
    These records were pulled but excluded from the main recommendation because they do not sufficiently match the sector lens.
  </p>
  ${renderNoiseTable(model.noiseRecords.slice(0, 12))}

  <h2>Evidence Gap Checklist</h2>
  <p>
    These items must be verified before aggressive bid decisions. A GovRevenue scan should never pretend unknown capacity is confirmed.
  </p>
  ${renderEvidenceGapTable(model.evidenceGaps)}

  <h2>30-Day Activation Plan</h2>
  <p>
    The report ends with commercial action: who to contact, what to send, and what evidence to prepare.
  </p>
  ${renderThirtyDayPlan(model.thirtyDayPlan)}

  <h2>Who to contact / What to send / What to prepare</h2>
  <div class="grid grid-3">
    <div class="action-card">
      <h3>Who to contact</h3>
      <ul>
        ${model.buyerWatchlist
          .slice(0, 5)
          .map((buyer) => `<li>${escapeHtml(buyer.buyer)} — ${escapeHtml(buyer.nextAction)}</li>`)
          .join("")}
      </ul>
    </div>
    <div class="action-card">
      <h3>What to send</h3>
      <ul>
        <li>One-page capability statement tailored to the highest-fit buyer type.</li>
        <li>Two short case studies with site type, risk, service delivered and outcome.</li>
        <li>Insurance, accreditations, COSHH, H&S and mobilisation summary.</li>
      </ul>
    </div>
    <div class="action-card">
      <h3>What to prepare</h3>
      <ul>
        <li>Bid/no-bid rule for contract size, location, TUPE risk and mobilisation capacity.</li>
        <li>Framework eligibility checklist.</li>
        <li>Verified source list with record IDs and URLs.</li>
      </ul>
    </div>
  </div>

  <h2>Source Appendix</h2>
  ${renderSourceAppendix(model.relevantRecords)}

  <div class="footer">
    GovRevenue is commercial intelligence, not legal, procurement or financial advice.
    Human verification is required before bid decisions. No outcome is guaranteed.
  </div>
</main>
</body>
</html>`;
}

function metricCard(label: string, value: string, note: string): string {
  return `<div class="card">
    <span class="metric-label">${escapeHtml(label)}</span>
    <span class="metric-value">${escapeHtml(value)}</span>
    <span class="metric-note">${escapeHtml(note)}</span>
  </div>`;
}

function renderWarnings(model: Omit<ReportModel, "qa">): string {
  if (!model.dataQuality.warnings.length) {
    return `<div class="good">Data quality passed the minimum commercial trust checks. Continue to human verification before bid action.</div>`;
  }

  return model.dataQuality.warnings
    .map((warning) => `<div class="warning">${escapeHtml(warning)}</div>`)
    .join("");
}

function renderRouteChart(opportunities: OpportunityItem[]): string {
  const rows = opportunities.slice(0, 8).map((item) => {
    const width = clamp(item.actionabilityScore, 0, 100);
    return `<div class="bar-row">
      <div class="bar-label">${escapeHtml(item.opportunity)}</div>
      <div class="bar-track"><div class="bar-fill" style="width:${width}%"></div></div>
      <div class="bar-value">${width}/100</div>
    </div>`;
  });

  return `<div class="chart">${rows.join("")}</div>`;
}

function renderOpportunityTable(items: OpportunityItem[]): string {
  return `<div class="table-wrap">
    <table>
      <colgroup>
        <col style="width:18%">
        <col style="width:14%">
        <col style="width:13%">
        <col style="width:18%">
        <col style="width:10%">
        <col style="width:9%">
        <col style="width:18%">
      </colgroup>
      <thead>
        <tr>
          <th>Opportunity</th>
          <th>Likely buyer</th>
          <th>Confidence</th>
          <th>Why they spend</th>
          <th>Route</th>
          <th>Score</th>
          <th>Next action this week</th>
        </tr>
      </thead>
      <tbody>
        ${items
          .map(
            (item) => `<tr>
              <td>${escapeHtml(item.opportunity)}</td>
              <td>${escapeHtml(item.likelyBuyer)}</td>
              <td>${renderBadge(item.sourceConfidence)}</td>
              <td>${escapeHtml(item.whyTheySpend)}</td>
              <td>${escapeHtml(humanRoute(item.route))}</td>
              <td>${item.actionabilityScore}/100</td>
              <td>${escapeHtml(item.nextActionThisWeek)}</td>
            </tr>`,
          )
          .join("")}
      </tbody>
    </table>
  </div>`;
}

function renderBuyerTypeTable(config: SectorConfig): string {
  return `<div class="table-wrap">
    <table>
      <colgroup>
        <col style="width:18%">
        <col style="width:10%">
        <col style="width:34%">
        <col style="width:26%">
        <col style="width:12%">
      </colgroup>
      <thead>
        <tr>
          <th>Buyer type</th>
          <th>Fit</th>
          <th>Spend logic</th>
          <th>Best entry route</th>
          <th>Priority</th>
        </tr>
      </thead>
      <tbody>
        ${config.buyerTypes
          .map(
            (buyer) => `<tr>
              <td>${escapeHtml(buyer.buyerType)}</td>
              <td>${escapeHtml(buyer.fit)}</td>
              <td>${escapeHtml(buyer.spendLogic)}</td>
              <td>${escapeHtml(buyer.bestEntryRoute)}</td>
              <td>${escapeHtml(buyer.priority)}</td>
            </tr>`,
          )
          .join("")}
      </tbody>
    </table>
  </div>`;
}

function renderBuyerWatchlistTable(items: BuyerWatchItem[]): string {
  return `<div class="table-wrap">
    <table>
      <colgroup>
        <col style="width:17%">
        <col style="width:20%">
        <col style="width:16%">
        <col style="width:13%">
        <col style="width:12%">
        <col style="width:22%">
      </colgroup>
      <thead>
        <tr>
          <th>Buyer</th>
          <th>Why they may buy</th>
          <th>Service to pitch</th>
          <th>Best route</th>
          <th>Confidence</th>
          <th>Next action</th>
        </tr>
      </thead>
      <tbody>
        ${items
          .map(
            (item) => `<tr>
              <td>${escapeHtml(item.buyer)}</td>
              <td>${escapeHtml(item.whyTheyMayBuy)}</td>
              <td>${escapeHtml(item.serviceToPitch)}</td>
              <td>${escapeHtml(item.bestRoute)}</td>
              <td>${renderBadge(item.sourceConfidence)}</td>
              <td>${escapeHtml(item.nextAction)}</td>
            </tr>`,
          )
          .join("")}
      </tbody>
    </table>
  </div>`;
}

function renderRecordTable(records: ScoredRecord[]): string {
  if (!records.length) {
    return `<div class="warning">No source-backed relevant records were found. Do not present live opportunity claims.</div>`;
  }

  return `<div class="table-wrap">
    <table>
      <colgroup>
        <col style="width:18%">
        <col style="width:14%">
        <col style="width:12%">
        <col style="width:10%">
        <col style="width:10%">
        <col style="width:16%">
        <col style="width:20%">
      </colgroup>
      <thead>
        <tr>
          <th>Record</th>
          <th>Buyer</th>
          <th>Value</th>
          <th>Status</th>
          <th>Score</th>
          <th>Evidence</th>
          <th>Source</th>
        </tr>
      </thead>
      <tbody>
        ${records
          .map(
            (record) => `<tr>
              <td>${escapeHtml(record.title)}</td>
              <td>${escapeHtml(record.buyerName || "Unknown")}</td>
              <td>${escapeHtml(record.displayValue)}</td>
              <td>${escapeHtml(record.status || "unknown")}</td>
              <td>${record.relevanceScore}/100</td>
              <td>${renderBadge(record.evidenceLabel)}<br><span class="small">${escapeHtml(
                record.sectorMatchReasons.slice(0, 2).join(" | "),
              )}</span></td>
              <td class="url">${escapeHtml(record.sourceUrl || record.id || "No source URL")}</td>
            </tr>`,
          )
          .join("")}
      </tbody>
    </table>
  </div>`;
}

function renderNoiseTable(records: ScoredRecord[]): string {
  if (!records.length) {
    return `<div class="good">No low-relevance records were quarantined.</div>`;
  }

  return `<div class="table-wrap">
    <table>
      <colgroup>
        <col style="width:24%">
        <col style="width:18%">
        <col style="width:10%">
        <col style="width:28%">
        <col style="width:20%">
      </colgroup>
      <thead>
        <tr>
          <th>Record</th>
          <th>Buyer</th>
          <th>Score</th>
          <th>Why quarantined</th>
          <th>Source</th>
        </tr>
      </thead>
      <tbody>
        ${records
          .map(
            (record) => `<tr>
              <td>${escapeHtml(record.title)}</td>
              <td>${escapeHtml(record.buyerName || "Unknown")}</td>
              <td>${record.relevanceScore}/100</td>
              <td>${escapeHtml(
                record.riskReasons.join(" | ") || "Below relevance threshold for the selected sector lens.",
              )}</td>
              <td class="url">${escapeHtml(record.sourceUrl || record.id || "No source URL")}</td>
            </tr>`,
          )
          .join("")}
      </tbody>
    </table>
  </div>`;
}

function renderEvidenceGapTable(gaps: ReportModel["evidenceGaps"]): string {
  return `<div class="table-wrap">
    <table>
      <colgroup>
        <col style="width:20%">
        <col style="width:10%">
        <col style="width:16%">
        <col style="width:28%">
        <col style="width:26%">
      </colgroup>
      <thead>
        <tr>
          <th>Asset</th>
          <th>Status</th>
          <th>Confidence</th>
          <th>Why it matters</th>
          <th>Fix this week</th>
        </tr>
      </thead>
      <tbody>
        ${gaps
          .map(
            (gap) => `<tr>
              <td>${escapeHtml(gap.asset)}</td>
              <td>${escapeHtml(gap.status)}</td>
              <td>${renderBadge(gap.sourceConfidence)}</td>
              <td>${escapeHtml(gap.whyItMatters)}</td>
              <td>${escapeHtml(gap.fixThisWeek)}</td>
            </tr>`,
          )
          .join("")}
      </tbody>
    </table>
  </div>`;
}

function renderThirtyDayPlan(plan: ReportModel["thirtyDayPlan"]): string {
  return `<div class="grid grid-2">
    ${plan
      .map(
        (week) => `<div class="action-card">
          <h3>${escapeHtml(week.week)} — ${escapeHtml(week.focus)}</h3>
          <ul>
            ${week.actions.map((action) => `<li>${escapeHtml(action)}</li>`).join("")}
          </ul>
          <p><strong>Output:</strong> ${escapeHtml(week.output)}</p>
        </div>`,
      )
      .join("")}
  </div>`;
}

function renderSourceAppendix(records: ScoredRecord[]): string {
  const sourceRows = records
    .filter((record) => record.sourceUrl || record.id)
    .slice(0, 30);

  if (!sourceRows.length) {
    return `<div class="warning">No source appendix available. This report should not be sold as source-backed.</div>`;
  }

  return `<div class="table-wrap">
    <table>
      <colgroup>
        <col style="width:24%">
        <col style="width:34%">
        <col style="width:16%">
        <col style="width:26%">
      </colgroup>
      <thead>
        <tr>
          <th>Record ID</th>
          <th>URL</th>
          <th>Confidence</th>
          <th>Use in report</th>
        </tr>
      </thead>
      <tbody>
        ${sourceRows
          .map(
            (record) => `<tr>
              <td>${escapeHtml(record.id)}</td>
              <td class="url">${escapeHtml(record.sourceUrl || "")}</td>
              <td>${renderBadge(record.evidenceLabel)}</td>
              <td>${escapeHtml(record.sectorMatchReasons.slice(0, 2).join(" | "))}</td>
            </tr>`,
          )
          .join("")}
      </tbody>
    </table>
  </div>`;
}

function renderBadge(label: EvidenceLabel): string {
  const cls =
    label === "VERIFIED_RECORD"
      ? "badge-verified"
      : label === "PULLED_RECORD"
        ? "badge-pulled"
        : label === "INFERRED"
          ? "badge-inferred"
          : label === "STRATEGIC_TARGET"
            ? "badge-strategic"
            : label === "LOW_RELEVANCE_NOISE"
              ? "badge-noise"
              : "badge-inferred";

  return `<span class="badge ${cls}">${escapeHtml(label.replaceAll("_", " "))}</span>`;
}

function sortByScoreThenValue(a: ScoredRecord, b: ScoredRecord): number {
  if (b.relevanceScore !== a.relevanceScore) return b.relevanceScore - a.relevanceScore;
  return b.conservativeValue - a.conservativeValue;
}

function sortByLowestScore(a: ScoredRecord, b: ScoredRecord): number {
  return a.relevanceScore - b.relevanceScore;
}

function matchedTerms(text: string, terms: string[]): string[] {
  return uniqueCleanArray(terms).filter((term) => {
    const normalisedTerm = normalise(term);
    if (!normalisedTerm) return false;
    return text.includes(normalisedTerm);
  });
}

function normaliseStatus(status?: string): ProcurementStatus {
  const text = normalise(status ?? "");

  if (text.includes("open") || text.includes("active")) return "open";
  if (text.includes("award") || text.includes("awarded")) return "award";
  if (text.includes("closed") || text.includes("complete")) return "closed";
  if (text.includes("planned") || text.includes("pipeline")) return "planned";

  return "unknown";
}

function hasFutureDeadline(deadline?: string): boolean {
  if (!deadline) return false;
  const date = new Date(deadline);
  if (Number.isNaN(date.getTime())) return false;
  return date.getTime() > Date.now();
}

function getConservativeValue(record: ProcurementRecord): number {
  const candidates = [record.value, record.valueLow, record.valueHigh]
    .filter((value): value is number => typeof value === "number" && Number.isFinite(value))
    .filter((value) => value > 0);

  if (!candidates.length) return 0;

  return Math.min(...candidates);
}

function sumValues(records: ScoredRecord[]): number {
  return records.reduce((sum, record) => sum + (record.conservativeValue || 0), 0);
}

function maxValue(records: ScoredRecord[]): number {
  return records.reduce(
    (max, record) => Math.max(max, record.conservativeValue || 0),
    0,
  );
}

function chooseServiceToPitch(intake: CompanyIntake, record: ScoredRecord): string {
  const text = normalise([record.title, record.description].join(" "));

  const specialist = [
    "healthcare deep cleaning",
    "clinical cleaning",
    "school cleaning",
    "kitchen deep cleaning",
    "reactive cleaning",
    "emergency cleaning",
    "office cleaning",
    "communal cleaning",
  ];

  const match = specialist.find((service) => text.includes(service));
  if (match) return titleCase(match);

  return chooseDefaultService(intake);
}

function chooseDefaultService(intake: CompanyIntake): string {
  return intake.services[0] || "Relevant public-sector service";
}

function nextActionForBuyer(buyer: string, record: ScoredRecord): string {
  if (record.isOpenOpportunity) {
    return `Check eligibility and prepare bid/no-bid decision for ${buyer}.`;
  }

  if (record.isAwardSignal) {
    return `Use award record as market proof and check renewal timing for ${buyer}.`;
  }

  return `Verify procurement route and find named procurement contact for ${buyer}.`;
}

function inferLikelyBuyer(route: string): string {
  const text = normalise(route);

  if (text.includes("nhs") || text.includes("healthcare")) return "NHS trusts";
  if (text.includes("education") || text.includes("academy")) return "Academy trusts and schools";
  if (text.includes("local authority")) return "Local authorities";
  if (text.includes("framework")) return "Framework operators";
  if (text.includes("reactive")) return "NHS trusts and local authorities";

  return "Public-sector buyers";
}

function humanRoute(route: RouteType): string {
  switch (route) {
    case "direct_bid":
      return "Direct bid";
    case "framework":
      return "Framework";
    case "dps":
      return "DPS";
    case "partner":
      return "Partner / subcontract";
    case "monitor_only":
      return "Monitor only";
    case "ignore":
      return "Ignore";
    default:
      return "Unknown";
  }
}

function findTemplateLeaks(value: string): string[] {
  const leaks = value.match(/\$\{[^}]+\}/g) ?? [];
  return uniqueCleanArray(leaks);
}

function escapeHtml(value: unknown): string {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function cleanText(value: unknown): string {
  if (value === undefined || value === null) return "";

  return String(value)
    .replace(/\s+/g, " ")
    .replace(/[\u0000-\u001F\u007F]/g, "")
    .trim();
}

function requiredCleanText(value: unknown, fieldName: string): string {
  const cleaned = cleanText(value);

  if (!cleaned) {
    throw new Error(`Missing required field: ${fieldName}`);
  }

  return cleaned;
}

function uniqueCleanArray(values: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];

  for (const value of values) {
    const cleaned = cleanText(value);
    const key = normalise(cleaned);

    if (!cleaned || seen.has(key)) continue;

    seen.add(key);
    out.push(cleaned);
  }

  return out;
}

function normalise(value: unknown): string {
  return cleanText(value)
    .toLowerCase()
    .replace(/[^\p{L}\p{N}£$.\-/%\s]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function formatMoney(value: number): string {
  if (!value || !Number.isFinite(value)) return "£0";
  return GBP.format(value);
}

function formatDate(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;

  return new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  }).format(date);
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function titleCase(value: string): string {
  return value.replace(/\w\S*/g, (word) => word.charAt(0).toUpperCase() + word.slice(1));
}
