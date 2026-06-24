import type { EarlySignal } from "./types.js";
import { pool } from "../../config.js";

export type ScoredSignal = EarlySignal & {
  relevanceScore: number;
  recencyScore: number;
  compositeScore: number;
  dedupKey: string;
};

// ── Recency weighting ────────────────────────────────────────────────────────

function recencyScore(fetchedAt: string): number {
  const ageMs = Date.now() - new Date(fetchedAt).getTime();
  const ageDays = ageMs / (1000 * 60 * 60 * 24);
  // Exponential decay: 100 → ~37 at 1 day, ~14 at 2 days, ~2 at 4 days
  return Math.max(0, Math.round(100 * Math.exp(-0.5 * ageDays)));
}

// ── Relevance scoring ────────────────────────────────────────────────────────

const SIGNIFICANCE_WEIGHT: Record<string, number> = {
  high: 100,
  medium: 60,
  low: 30,
};

const CHANGE_BONUS_THRESHOLD = 5; // % change that adds a bonus

function relevanceScore(signal: EarlySignal): number {
  let score = SIGNIFICANCE_WEIGHT[signal.significance] ?? 30;

  // Boost for large absolute change
  if (signal.change_pct != null) {
    const absPct = Math.abs(signal.change_pct);
    if (absPct >= 20) score += 30;
    else if (absPct >= CHANGE_BONUS_THRESHOLD) score += 15;
  }

  // Boost for multi-desk relevance
  const deskCount = signal.desk_categories?.length ?? 0;
  if (deskCount >= 3) score += 20;
  else if (deskCount >= 2) score += 10;

  return Math.min(100, score);
}

// ── Deduplication key ────────────────────────────────────────────────────────

function dedupKey(signal: EarlySignal): string {
  return `${signal.source}:${signal.indicator}:${signal.region}:${signal.period}`;
}

// ── Main aggregation function ────────────────────────────────────────────────

export function scoreAndDeduplicate(signals: EarlySignal[]): ScoredSignal[] {
  // Deduplicate: keep the most recent per key
  const seen = new Map<string, EarlySignal>();
  for (const s of signals) {
    const key = dedupKey(s);
    const existing = seen.get(key);
    if (!existing || new Date(s.fetched_at) > new Date(existing.fetched_at)) {
      seen.set(key, s);
    }
  }

  // Score each unique signal
  return [...seen.values()].map(s => {
    const rel = relevanceScore(s);
    const rec = recencyScore(s.fetched_at);
    // Composite: 60% relevance, 40% recency
    const composite = Math.round(rel * 0.6 + rec * 0.4);
    return {
      ...s,
      relevanceScore: rel,
      recencyScore: rec,
      compositeScore: composite,
      dedupKey: dedupKey(s),
    };
  }).sort((a, b) => b.compositeScore - a.compositeScore);
}

// ── Desk-filtered aggregation ─────────────────────────────────────────────────

export function aggregateSignalsForDesk(signals: EarlySignal[], deskSlug: string): ScoredSignal[] {
  const relevant = signals.filter(s =>
    s.desk_categories?.includes(deskSlug) ||
    s.desk_categories?.includes("all")
  );
  return scoreAndDeduplicate(relevant);
}

// ── DB-backed aggregation (reads from canonical_ingest for extra sources) ────

export async function getAggregatedSignals(opts: {
  deskSlug?: string;
  limit?: number;
  minScore?: number;
}): Promise<ScoredSignal[]> {
  const { deskSlug, limit = 50, minScore = 0 } = opts;
  if (!pool) return [];

  // Pull recent early signals from DB
  const r = await pool.query<EarlySignal>(
    `SELECT * FROM early_signals
     WHERE fetched_at > NOW() - INTERVAL '30 days'
     ${deskSlug ? `AND $1 = ANY(desk_categories)` : ""}
     ORDER BY fetched_at DESC
     LIMIT 500`,
    deskSlug ? [deskSlug] : []
  );

  const scored = scoreAndDeduplicate(r.rows);
  return scored.filter(s => s.compositeScore >= minScore).slice(0, limit);
}

// ── Ingest-derived signal generation ─────────────────────────────────────────
// Scans canonical_ingest for recent records and derives signals for each source type

export async function deriveSignalsFromIngest(): Promise<number> {
  if (!pool) return 0;

  // Pull recent unprocessed ingest records grouped by source
  const r = await pool.query<{ source: string; count: string; latest: string }>(
    `SELECT source, COUNT(*) as count, MAX(fetched_at) as latest
     FROM canonical_ingest
     WHERE status = 'pending' AND fetched_at > NOW() - INTERVAL '7 days'
     GROUP BY source
     HAVING COUNT(*) > 0`
  );

  let derived = 0;
  for (const row of r.rows) {
    const count = parseInt(row.count);
    const source = row.source;

    // Map ingest source to signal desk categories
    const deskMap: Record<string, string[]> = {
      sell2wales:            ["construction", "facilities", "digital"],
      nhsbsa_pipeline:       ["health", "digital"],
      gazette_procurement:   ["all"],
      gazette_insolvency:    ["all"],
      nomis_demography:      ["construction", "recruitment", "finance"],
      uk_parliament:         ["all"],
      local_authority_spending: ["facilities", "construction"],
      data_gov_uk_procurement:  ["all"],
      tfl:                   ["transport", "construction"],
      bus_open_data:         ["transport"],
      nhs_content:           ["health"],
      three_sixty_giving:    ["social_care", "education"],
      environment_agency_floods: ["construction", "facilities", "energy"],
      defra_datasets:        ["energy", "waste"],
      octopus_energy:        ["energy"],
      bbc_business:          ["all"],
      bbc_politics:          ["all"],
      hansard_procurement:   ["all"],
    };

    const desks = deskMap[source] ?? [];
    if (desks.length === 0) continue;

    // Synthesise a signal from the batch volume
    try {
      await pool.query(
        `INSERT INTO early_signals (source, indicator, region, period, current_value, previous_value, change_pct, significance, desk_categories, narrative, fetched_at)
         VALUES ($1, $2, $3, $4, $5, NULL, NULL, $6, $7, $8, NOW())
         ON CONFLICT DO NOTHING`,
        [
          "govuk",
          `${source}_activity`,
          "UK",
          new Date().toISOString().slice(0, 7),
          count,
          count > 50 ? "high" : count > 10 ? "medium" : "low",
          JSON.stringify(desks),
          `${count} new records ingested from ${source} in the last 7 days.`,
        ]
      );
      derived++;
    } catch {}
  }

  // Mark those ingest records as processed
  await pool.query(
    `UPDATE canonical_ingest SET status = 'processed', processed_at = NOW()
     WHERE status = 'pending' AND fetched_at > NOW() - INTERVAL '7 days'`
  );

  return derived;
}
