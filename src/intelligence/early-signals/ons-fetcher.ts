import type { OnsDataPoint } from "./types.js";

const ONS_BETA = "https://api.beta.ons.gov.uk/v1";
const TIMEOUT_MS = 15_000;

const CONSTRUCTION_DATASET = "output-in-the-construction-industry";
const CONSTRUCTION_EDITION = "time-series";

const BUSINESS_DATASET = "uk-business-by-enterprises-and-local-units";

function onsAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

function recentMonthCodes(count: number): string[] {
  const months = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"];
  const codes: string[] = [];
  const now = new Date();
  for (let i = count; i >= 1; i--) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    codes.push(`${d.getFullYear()}-${months[d.getMonth()]}`);
  }
  return codes;
}

async function fetchLatestVersion(): Promise<string> {
  const ac = onsAbort();
  const res = await fetch(
    `${ONS_BETA}/datasets/${CONSTRUCTION_DATASET}/editions/${CONSTRUCTION_EDITION}/versions?limit=1`,
    { signal: ac.signal }
  );
  if (!res.ok) return "54";
  const json = await res.json() as { items?: Array<{ version?: number }> };
  return String(json.items?.[0]?.version ?? 54);
}

async function fetchConstructionObservation(version: string, timeCode: string): Promise<OnsDataPoint | null> {
  try {
    const ac = onsAbort();
    const url = `${ONS_BETA}/datasets/${CONSTRUCTION_DATASET}/editions/${CONSTRUCTION_EDITION}/versions/${version}/observations?geography=K03000001&seasonaladjustment=seasonal-adjustment&seriestype=index-numbers&typeofwork=1&time=${timeCode}`;
    const res = await fetch(url, { signal: ac.signal });
    if (!res.ok) return null;

    const json = await res.json() as {
      observations?: Array<{ observation: string }> | null;
      total_observations?: number;
    };

    if (!json.observations || json.observations.length === 0) return null;

    const val = parseFloat(json.observations[0].observation);
    if (isNaN(val)) return null;

    return { date: timeCode, value: val, label: timeCode };
  } catch {
    return null;
  }
}

export async function fetchConstructionOutput(): Promise<OnsDataPoint[]> {
  try {
    const version = await fetchLatestVersion();
    const codes = recentMonthCodes(12);

    const results = await Promise.allSettled(
      codes.map(code => fetchConstructionObservation(version, code))
    );

    const points: OnsDataPoint[] = [];
    for (const r of results) {
      if (r.status === "fulfilled" && r.value) {
        points.push(r.value);
      }
    }

    return points;
  } catch {
    return [];
  }
}

export async function fetchBusinessDemography(): Promise<OnsDataPoint[]> {
  try {
    const points: OnsDataPoint[] = [];
    const editions = ["2021", "2022"];

    for (const edition of editions) {
      try {
        const ac = onsAbort();
        const url = `${ONS_BETA}/datasets/${BUSINESS_DATASET}/editions/${edition}/versions/1/observations?geography=K02000001&enterprisesandlocalunits=enterprises&unofficialstandardindustrialclassification=total&time=${edition}`;
        const res = await fetch(url, { signal: ac.signal });
        if (!res.ok) continue;

        const json = await res.json() as {
          observations?: Array<{ observation: string }> | null;
        };

        if (!json.observations || json.observations.length === 0) continue;

        const val = parseFloat(json.observations[0].observation);
        if (!isNaN(val)) {
          points.push({ date: edition, value: val, label: `${edition} enterprises` });
        }
      } catch {
        continue;
      }
    }

    return points;
  } catch {
    return [];
  }
}
