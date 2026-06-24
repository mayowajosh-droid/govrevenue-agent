import type { EarlySignal } from "./types.js";

const PLANNING_BASE = "https://www.planning.data.gov.uk/api/v1";
const TIMEOUT_MS = 20_000;

type PlanningEntity = {
  reference: string;
  name?: string;
  dataset: string;
  organisation?: string;
  "entry-date"?: string;
  "start-date"?: string;
  typology?: string;
  point?: string;
};

type PlanningApiResponse = {
  count: number;
  entities: PlanningEntity[];
};

function planningAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

async function fetchPlanningDataset(
  dataset: string,
  params: Record<string, string> = {}
): Promise<PlanningApiResponse> {
  const url = new URL(`${PLANNING_BASE}/entity.json`);
  url.searchParams.set("dataset", dataset);
  url.searchParams.set("limit", "100");
  for (const [k, v] of Object.entries(params)) {
    url.searchParams.set(k, v);
  }

  const ac = planningAbort();
  const res = await fetch(url.toString(), { signal: ac.signal });
  if (!res.ok) throw new Error(`Planning Data API ${res.status}: ${dataset}`);
  return res.json() as Promise<PlanningApiResponse>;
}

function recentMonthStart(monthsBack: number): string {
  const d = new Date();
  d.setMonth(d.getMonth() - monthsBack);
  return d.toISOString().slice(0, 10);
}

export type PlanningSnapshot = {
  brownfieldCount: number;
  brownfieldCountPrior: number;
  conservationAreaCount: number;
  articleFourCount: number;
  fetchedAt: string;
};

export async function fetchPlanningSnapshot(): Promise<PlanningSnapshot> {
  const fetchedAt = new Date().toISOString();
  const monthStart = recentMonthStart(3);
  const priorMonthStart = recentMonthStart(6);

  const [brownfieldRecent, brownfieldPrior, conservationAreas, articleFour] =
    await Promise.allSettled([
      fetchPlanningDataset("brownfield-land", { "entry-date-minimum": monthStart }),
      fetchPlanningDataset("brownfield-land", {
        "entry-date-minimum": priorMonthStart,
        "entry-date-maximum": monthStart,
      }),
      fetchPlanningDataset("conservation-area"),
      fetchPlanningDataset("article-4-direction-area"),
    ]);

  return {
    brownfieldCount:
      brownfieldRecent.status === "fulfilled" ? brownfieldRecent.value.count : 0,
    brownfieldCountPrior:
      brownfieldPrior.status === "fulfilled" ? brownfieldPrior.value.count : 0,
    conservationAreaCount:
      conservationAreas.status === "fulfilled" ? conservationAreas.value.count : 0,
    articleFourCount:
      articleFour.status === "fulfilled" ? articleFour.value.count : 0,
    fetchedAt,
  };
}

function significance(changePct: number): "high" | "medium" | "low" {
  const abs = Math.abs(changePct);
  if (abs > 20) return "high";
  if (abs >= 8) return "medium";
  return "low";
}

export function buildPlanningSignals(snapshot: PlanningSnapshot): EarlySignal[] {
  const signals: EarlySignal[] = [];
  const now = snapshot.fetchedAt;
  const period = new Date().toISOString().slice(0, 7);

  if (snapshot.brownfieldCountPrior > 0) {
    const changePct =
      ((snapshot.brownfieldCount - snapshot.brownfieldCountPrior) /
        snapshot.brownfieldCountPrior) *
      100;

    const sig: EarlySignal = {
      id: `planning-brownfield-${period}`,
      source: "planning",
      indicator: "brownfield_registrations",
      region: "England",
      period,
      current_value: snapshot.brownfieldCount,
      previous_value: snapshot.brownfieldCountPrior,
      change_pct: Math.round(changePct * 10) / 10,
      significance: significance(changePct),
      desk_categories: ["construction", "housing-support", "planning"],
      narrative: "",
      fetched_at: now,
    };
    sig.narrative =
      changePct > 0
        ? `Brownfield land registrations up ${Math.abs(sig.change_pct ?? 0).toFixed(1)}% vs prior quarter (${snapshot.brownfieldCount} vs ${snapshot.brownfieldCountPrior}) — signals growing remediation and housing development pipeline.`
        : `Brownfield land registrations down ${Math.abs(sig.change_pct ?? 0).toFixed(1)}% vs prior quarter — monitor for pipeline slowdown in construction and housing sectors.`;
    signals.push(sig);
  }

  if (snapshot.conservationAreaCount > 0) {
    signals.push({
      id: `planning-conservation-${period}`,
      source: "planning",
      indicator: "conservation_areas",
      region: "England",
      period,
      current_value: snapshot.conservationAreaCount,
      previous_value: null,
      change_pct: null,
      significance: "low",
      desk_categories: ["construction", "planning"],
      narrative: `${snapshot.conservationAreaCount} conservation areas indexed — any construction procurement in these zones requires heritage-compliant specifications.`,
      fetched_at: now,
    });
  }

  if (snapshot.articleFourCount > 0) {
    signals.push({
      id: `planning-article4-${period}`,
      source: "planning",
      indicator: "article_4_directions",
      region: "England",
      period,
      current_value: snapshot.articleFourCount,
      previous_value: null,
      change_pct: null,
      significance: "low",
      desk_categories: ["construction", "digital"],
      narrative: `${snapshot.articleFourCount} Article 4 Direction areas restrict permitted development — relevant for construction and digital infrastructure bids in controlled zones.`,
      fetched_at: now,
    });
  }

  return signals;
}
