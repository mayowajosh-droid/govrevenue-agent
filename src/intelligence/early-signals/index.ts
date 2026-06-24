export type { EarlySignal, OnsDataPoint, LandRegistryTransaction } from "./types.js";
export {
  initEarlySignalsTables,
  upsertEarlySignal,
  getLatestEarlySignals,
  getEarlySignalsByDesk,
  upsertOnsCache,
  upsertLandRegistryCache,
} from "./db.js";
export { fetchConstructionOutput, fetchBusinessDemography } from "./ons-fetcher.js";
export { fetchRecentTransactions, fetchDistrictSummary } from "./land-registry-fetcher.js";
export { correlateSignals, generateNarrative } from "./signal-correlator.js";
export { fetchPlanningSnapshot, buildPlanningSignals } from "./planning-data-fetcher.js";
export { fetchGovukSnapshot, buildGovukSignals } from "./govuk-fetcher.js";
export { scoreAndDeduplicate, aggregateSignalsForDesk, getAggregatedSignals, deriveSignalsFromIngest } from "./aggregation-engine.js";

import { fetchConstructionOutput, fetchBusinessDemography } from "./ons-fetcher.js";
import { fetchPlanningSnapshot, buildPlanningSignals } from "./planning-data-fetcher.js";
import { fetchGovukSnapshot, buildGovukSignals } from "./govuk-fetcher.js";
import { correlateSignals } from "./signal-correlator.js";
import { upsertEarlySignal } from "./db.js";
import type { EarlySignal } from "./types.js";

/**
 * Full refresh: fetch ONS + Planning Data + GOV.UK, correlate, persist.
 * Land Registry is skipped here (requires district input) — call separately.
 */
export async function refreshEarlySignals(): Promise<EarlySignal[]> {
  const [constructionOutput, businessDemography, planningSnapshot, govukSnapshot] = await Promise.all([
    fetchConstructionOutput(),
    fetchBusinessDemography(),
    fetchPlanningSnapshot().catch(() => null),
    fetchGovukSnapshot().catch(() => null),
  ]);

  const onsSignals = correlateSignals({
    constructionOutput,
    businessDemography,
    landRegistryByDistrict: new Map(),
  });

  const planningSignals = planningSnapshot ? buildPlanningSignals(planningSnapshot) : [];
  const govukSignals = govukSnapshot ? buildGovukSignals(govukSnapshot) : [];
  const signals = [...onsSignals, ...planningSignals, ...govukSignals];

  await Promise.all(signals.map((s) => upsertEarlySignal(s)));

  return signals;
}
