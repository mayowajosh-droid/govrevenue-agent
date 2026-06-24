import type { EarlySignal } from "./types.js";

const GOVUK_SEARCH = "https://www.gov.uk/api/search.json";
const TIMEOUT_MS = 15_000;

type GovukSearchResult = {
  title: string;
  link: string;
  description?: string;
  public_timestamp?: string;
  document_type?: string;
  organisations?: Array<{ title: string; slug: string }>;
};

type GovukSearchResponse = {
  total: number;
  results: GovukSearchResult[];
};

function govukAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

async function searchGovuk(
  query: string,
  filter_document_type?: string,
  count = 20
): Promise<GovukSearchResult[]> {
  const url = new URL(GOVUK_SEARCH);
  url.searchParams.set("q", query);
  url.searchParams.set("count", String(count));
  url.searchParams.set("order", "-public_timestamp");
  if (filter_document_type) {
    url.searchParams.set("filter_document_type", filter_document_type);
  }

  const ac = govukAbort();
  const res = await fetch(url.toString(), { signal: ac.signal });
  if (!res.ok) return [];
  const data = (await res.json()) as GovukSearchResponse;
  return Array.isArray(data.results) ? data.results : [];
}

export type GovukSnapshot = {
  procurementPolicyCount: number;
  spendingReviewCount: number;
  consultationCount: number;
  strategyCount: number;
  recentItems: GovukSearchResult[];
  fetchedAt: string;
};

const THIRTY_DAYS_AGO = () => {
  const d = new Date();
  d.setDate(d.getDate() - 30);
  return d.toISOString().slice(0, 10);
};

export async function fetchGovukSnapshot(): Promise<GovukSnapshot> {
  const fetchedAt = new Date().toISOString();
  const cutoff = THIRTY_DAYS_AGO();

  const [policies, reviews, consultations, strategies] = await Promise.allSettled([
    searchGovuk("procurement public sector spending", "policy"),
    searchGovuk("spending review public services budget"),
    searchGovuk("public services consultation procurement", "consultation"),
    searchGovuk("government strategy infrastructure public sector"),
  ]);

  const filterRecent = (items: GovukSearchResult[]) =>
    items.filter(r => !r.public_timestamp || r.public_timestamp.slice(0, 10) >= cutoff);

  const recentPolicies = policies.status === "fulfilled" ? filterRecent(policies.value) : [];
  const recentReviews = reviews.status === "fulfilled" ? filterRecent(reviews.value) : [];
  const recentConsultations = consultations.status === "fulfilled" ? filterRecent(consultations.value) : [];
  const recentStrategies = strategies.status === "fulfilled" ? filterRecent(strategies.value) : [];

  const allRecent = [...recentPolicies, ...recentReviews, ...recentConsultations]
    .sort((a, b) => (b.public_timestamp || "").localeCompare(a.public_timestamp || ""))
    .slice(0, 10);

  return {
    procurementPolicyCount: recentPolicies.length,
    spendingReviewCount: recentReviews.length,
    consultationCount: recentConsultations.length,
    strategyCount: recentStrategies.length,
    recentItems: allRecent,
    fetchedAt,
  };
}

const GOVUK_DESK_MAP: Record<string, string[]> = {
  procurement: ["construction", "facilities", "digital", "energy", "health", "education"],
  "spending review": ["construction", "facilities", "digital", "energy"],
  consultation: ["construction", "digital", "planning", "health"],
  strategy: ["digital", "energy", "health", "consulting"],
};

function mapGovukToDeskCategories(snapshot: GovukSnapshot): string[] {
  const desks = new Set<string>();
  if (snapshot.procurementPolicyCount > 0) GOVUK_DESK_MAP["procurement"].forEach(d => desks.add(d));
  if (snapshot.spendingReviewCount > 0) GOVUK_DESK_MAP["spending review"].forEach(d => desks.add(d));
  if (snapshot.consultationCount > 0) GOVUK_DESK_MAP["consultation"].forEach(d => desks.add(d));
  if (snapshot.strategyCount > 0) GOVUK_DESK_MAP["strategy"].forEach(d => desks.add(d));
  return [...desks];
}

export function buildGovukSignals(snapshot: GovukSnapshot): EarlySignal[] {
  const signals: EarlySignal[] = [];
  const now = snapshot.fetchedAt;
  const period = new Date().toISOString().slice(0, 7);
  const total = snapshot.procurementPolicyCount + snapshot.spendingReviewCount + snapshot.consultationCount;

  if (total > 0) {
    signals.push({
      id: `govuk-policy-${period}`,
      source: "planning",
      indicator: "govuk_policy_activity",
      region: "UK",
      period,
      current_value: total,
      previous_value: null,
      change_pct: null,
      significance: total >= 10 ? "high" : total >= 4 ? "medium" : "low",
      desk_categories: mapGovukToDeskCategories(snapshot),
      narrative: `${total} UK government publications in the last 30 days: ${snapshot.procurementPolicyCount} procurement policies, ${snapshot.spendingReviewCount} spending/budget documents, ${snapshot.consultationCount} consultations. Watch for procurement rule changes and new framework signals.`,
      fetched_at: now,
    });
  }

  if (snapshot.consultationCount >= 3) {
    signals.push({
      id: `govuk-consultations-${period}`,
      source: "planning",
      indicator: "govuk_consultations_open",
      region: "UK",
      period,
      current_value: snapshot.consultationCount,
      previous_value: null,
      change_pct: null,
      significance: "medium",
      desk_categories: ["digital", "health", "construction", "energy"],
      narrative: `${snapshot.consultationCount} open government consultations signal upcoming policy shifts. Suppliers who respond to consultations often gain early positioning advantage on related contracts.`,
      fetched_at: now,
    });
  }

  return signals;
}
