/**
 * PlanIt planning-applications fetcher.
 *
 * UK planning applications have no single national portal — each council runs its
 * own. PlanIt (planit.org.uk) aggregates them into one free JSON API, which is the
 * practical national feed. We pull recent applications and write them into the
 * `planning_applications` table so the Atlas demand map's planning layer + the
 * "new approvals = construction/roofing demand" signal become real.
 *
 * API: https://www.planit.org.uk/api/applics/json
 *   params: pg_sz (page size), page (1-based), recent (last N days),
 *           start_date / end_date (YYYY-MM-DD), bbox, app_size, app_state …
 */

import { upsertPlanningApplication } from "./db.js";

const PLANIT_BASE = "https://www.planit.org.uk/api/applics/json";

interface PlanItRecord {
  name?: string;            // reference, e.g. "Aberdeen/260692/CLP"
  description?: string;
  area_name?: string;       // council / local authority
  app_type?: string;        // Full / Outline / Conditions …
  app_state?: string;       // Undecided / Permitted / Rejected …
  app_size?: string;        // Small / Medium / Large
  start_date?: string;      // received
  decided_date?: string | null;
  location_x?: number;      // longitude
  location_y?: number;      // latitude
  postcode?: string;
  address?: string;
}

interface PlanItResponse {
  records?: PlanItRecord[];
  total?: number;
  from?: string;
  to?: string;
}

// app_size → a rough indicative value band (£) so the map can weight demand.
// Deliberately conservative and clearly indicative, not a quote.
function indicativeValue(size?: string): number | null {
  switch ((size || "").toLowerCase()) {
    case "large": return 2_000_000;
    case "medium": return 250_000;
    case "small": return 25_000;
    default: return null;
  }
}

async function fetchPage(params: Record<string, string | number>, signal?: AbortSignal): Promise<PlanItResponse> {
  const qs = new URLSearchParams(
    Object.entries(params).map(([k, v]) => [k, String(v)])
  ).toString();
  const resp = await fetch(`${PLANIT_BASE}?${qs}`, {
    headers: { "User-Agent": "AtlasRevenue/1.0 (planning demand intelligence)", Accept: "application/json" },
    signal,
  });
  if (!resp.ok) throw new Error(`PlanIt ${resp.status} ${resp.statusText}`);
  return (await resp.json()) as PlanItResponse;
}

export interface PlanItIngestResult {
  recordsIngested: number;
  pagesFetched: number;
  total: number;
  error: string | null;
}

/**
 * Pull recent UK planning applications into planning_applications.
 * @param opts.recentDays how many days back to pull (default 30)
 * @param opts.maxRecords cap on records to ingest (default 4000)
 * @param opts.pageSize per-page size (default 100; PlanIt rejects larger pages)
 */
export async function ingestPlanningApplications(opts: {
  recentDays?: number;
  maxRecords?: number;
  pageSize?: number;
} = {}): Promise<PlanItIngestResult> {
  const recentDays = opts.recentDays ?? 30;
  const maxRecords = opts.maxRecords ?? 4000;
  const pageSize = Math.min(opts.pageSize ?? 100, 100);

  let ingested = 0;
  let page = 1;
  let pagesFetched = 0;
  let total = 0;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 120_000);

  try {
    while (ingested < maxRecords) {
      const data = await fetchPage(
        { pg_sz: pageSize, page, recent: recentDays },
        controller.signal,
      );
      pagesFetched++;
      total = data.total ?? total;
      const records = data.records ?? [];
      if (records.length === 0) break;

      for (const r of records) {
        if (ingested >= maxRecords) break;
        if (!r.name) continue;
        const lat = typeof r.location_y === "number" ? r.location_y : null;
        const lon = typeof r.location_x === "number" ? r.location_x : null;
        await upsertPlanningApplication({
          reference: r.name,
          description: r.description ?? null,
          status: r.app_state ?? null,
          decision: r.app_state && /permit|grant|approv/i.test(r.app_state) ? "Approved"
            : r.app_state && /reject|refus/i.test(r.app_state) ? "Refused" : null,
          application_type: r.app_type ?? null,
          applicant_name: null, // PlanIt does not expose applicant in the list API
          address: r.address ?? null,
          postcode: r.postcode ?? null,
          local_authority: r.area_name ?? null,
          lat, lon,
          received_date: r.start_date ?? null,
          decided_date: r.decided_date ?? null,
          estimated_value: indicativeValue(r.app_size),
          source: "planit",
        });
        ingested++;
      }

      // Stop if we've drained the result set.
      if (records.length < pageSize) break;
      page++;
    }
    return { recordsIngested: ingested, pagesFetched, total, error: null };
  } catch (err: any) {
    return { recordsIngested: ingested, pagesFetched, total, error: err?.message || String(err) };
  } finally {
    clearTimeout(timeout);
  }
}
