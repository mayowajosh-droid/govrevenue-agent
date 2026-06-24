import { pool } from "../../config.js";
import { DATA_SOURCES } from "./source-registry.js";

// ── Procurement & Contracts ──────────────────────────────────────────────────
import { fetchSell2WalesNotices } from "../../fetchers/sell2wales.js";
import { fetchNhsbsaPipelineData } from "../../fetchers/nhsbsa-pipeline.js";
import { fetchRecentProcurementNotices, fetchRecentInsolvencyNotices } from "../../fetchers/gazette.js";

// ── Government & Public Data ─────────────────────────────────────────────────
import { fetchBusinessDemographyByArea } from "../../fetchers/nomis.js";
import { searchCharities } from "../../fetchers/charity-commission.js";
import { searchParliamentaryQuestions } from "../../fetchers/uk-parliament.js";
import { searchLocalAuthoritySpending } from "../../fetchers/local-authority-spending.js";
import { fetchProcurementDatasets, fetchSpendingDatasets } from "../../fetchers/data-gov-uk.js";

// ── Transport & Infrastructure ───────────────────────────────────────────────
import { fetchLineStatuses } from "../../fetchers/tfl.js";
import { fetchBusDatasets } from "../../fetchers/bus-open-data.js";
import { fetchNetworkRailAssets } from "../../fetchers/networkrail.js";
import { lookupPostcode } from "../../fetchers/postcodes-io.js";

// ── Health, Education & Social ───────────────────────────────────────────────
import { searchNhsOrganisations } from "../../fetchers/nhs-content.js";
import { searchGrants } from "../../fetchers/three-sixty-giving.js";
import { searchCharity } from "../../fetchers/find-that-charity.js";
import { searchSchools } from "../../fetchers/explore-education.js";

// ── Environment & Energy ─────────────────────────────────────────────────────
import { fetchFloodWarnings, fetchActiveFloodWarningCount } from "../../fetchers/environment-agency.js";
import { fetchForecast } from "../../fetchers/met-office.js";
import { searchDefraDatasets } from "../../fetchers/defra.js";
import { fetchGreenEnergyProducts } from "../../fetchers/octopus-energy.js";
import { searchNrwDatasets } from "../../fetchers/natural-resources-wales.js";

// ── News & Media ─────────────────────────────────────────────────────────────
import { fetchBbcBusinessNews, fetchBbcPoliticsNews } from "../../fetchers/bbc-news.js";

// ── Crime & Justice ──────────────────────────────────────────────────────────
import { fetchPoliceForces } from "../../fetchers/uk-police.js";
import { searchCourts } from "../../fetchers/courts-tribunals.js";

// ── Other ────────────────────────────────────────────────────────────────────
import { fetchRecentProcurementDebates } from "../../fetchers/hansard.js";
import { fetchEnergyEfficiencyData } from "../../fetchers/energy-tech-list.js";
import { fetchPcsNotices } from "../../fetchers/public-contracts-scotland.js";
import { fetchNiProcurementNotices } from "../../fetchers/esourcing-ni.js";
import { fetchCqcProviders } from "../../fetchers/cqc.js";
import { fetchOfstedInspectionDatasets } from "../../fetchers/ofsted.js";

// ── Early Signals (already integrated) ───────────────────────────────────────
import { fetchConstructionOutput, fetchBusinessDemography } from "../../intelligence/early-signals/ons-fetcher.js";
import { fetchPlanningSnapshot } from "../../intelligence/early-signals/planning-data-fetcher.js";
import { fetchGovukSnapshot } from "../../intelligence/early-signals/govuk-fetcher.js";
import { fetchRecentTransactions } from "../../intelligence/early-signals/land-registry-fetcher.js";
import { upsertMetadataCatalog } from "../../governance/index.js";

export type IngestResult = {
  source: string;
  recordsIngested: number;
  durationMs: number;
  error: string | null;
};

async function ingestRecords(source: string, records: unknown[]): Promise<number> {
  if (!pool || records.length === 0) return 0;
  const batchId = `${source}_${Date.now()}`;
  let count = 0;
  for (const record of records) {
    try {
      await pool.query(
        `INSERT INTO canonical_ingest (source, raw_payload, ingestion_batch_id, status)
         VALUES ($1, $2, $3, 'pending')
         ON CONFLICT DO NOTHING`,
        [source, JSON.stringify(record), batchId]
      );
      count++;
    } catch {}
  }
  return count;
}

async function runSource(sourceId: string, fn: () => Promise<unknown[]>): Promise<IngestResult> {
  const start = Date.now();
  try {
    const records = await fn();
    const ingested = await ingestRecords(sourceId, records);
    const durationMs = Date.now() - start;

    // Update metadata catalog with latest fetch stats
    if (pool) {
      const src = DATA_SOURCES.find(s => s.id === sourceId);
      if (src) {
        await upsertMetadataCatalog(pool, {
          sourceId: src.id, sourceName: src.name, category: src.category,
          isLive: src.live, cadence: src.cadence,
          lastFetchedAt: new Date().toISOString(),
          lastRecordCount: ingested,
          avgFetchMs: durationMs,
          qualityStatus: ingested > 0 ? "pass" : "warn",
          qualityScore: ingested > 0 ? 1.0 : 0.5,
        }).catch(() => null);
      }
    }

    return { source: sourceId, recordsIngested: ingested, durationMs, error: null };
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    const durationMs = Date.now() - start;

    if (pool) {
      const src = DATA_SOURCES.find(s => s.id === sourceId);
      if (src) {
        await upsertMetadataCatalog(pool, {
          sourceId: src.id, sourceName: src.name, category: src.category,
          isLive: src.live, cadence: src.cadence,
          lastFetchedAt: new Date().toISOString(),
          lastRecordCount: 0, avgFetchMs: durationMs,
          qualityStatus: "fail", qualityScore: 0,
        }).catch(() => null);
      }
    }

    return { source: sourceId, recordsIngested: 0, durationMs, error: msg };
  }
}

/**
 * Full 43-source ingest pass. Safe to call repeatedly — all inserts are idempotent.
 * Sources run in parallel within each category batch.
 */
export async function runFullIngest(): Promise<IngestResult[]> {
  console.log("[ingest] Starting full 43-source ingest pass");

  // Run all source batches in parallel within each group
  const [
    procurementResults,
    govResults,
    transportResults,
    healthResults,
    envResults,
    newsResults,
    crimeResults,
    otherResults,
    signalResults,
  ] = await Promise.all([

    // PROCUREMENT & CONTRACTS
    Promise.all([
      runSource("sell2wales", () => fetchSell2WalesNotices()),
      runSource("nhsbsa_pipeline", () => fetchNhsbsaPipelineData()),
      runSource("gazette_procurement", () => fetchRecentProcurementNotices()),
      runSource("gazette_insolvency", () => fetchRecentInsolvencyNotices()),
      runSource("public_contracts_scotland", () => fetchPcsNotices()),
      runSource("esourcing_ni", () => fetchNiProcurementNotices()),
    ]),

    // GOVERNMENT & PUBLIC DATA
    Promise.all([
      runSource("nomis_demography", () => fetchBusinessDemographyByArea()),
      runSource("charity_commission", () => searchCharities("public").catch(() => [])),
      runSource("uk_parliament", () => searchParliamentaryQuestions("procurement")),
      runSource("local_authority_spending", () => searchLocalAuthoritySpending()),
      runSource("data_gov_uk_procurement", () => fetchProcurementDatasets()),
      runSource("data_gov_uk_spending", () => fetchSpendingDatasets()),
    ]),

    // TRANSPORT & INFRASTRUCTURE
    Promise.all([
      runSource("tfl", () => fetchLineStatuses()),
      runSource("bus_open_data", () => fetchBusDatasets()),
      runSource("networkrail", () => fetchNetworkRailAssets()),
    ]),

    // HEALTH, EDUCATION & SOCIAL
    Promise.all([
      runSource("nhs_content", () => searchNhsOrganisations("NHS Trust")),
      runSource("three_sixty_giving", () => searchGrants("public sector")),
      runSource("three_sixty_giving_local", () => searchGrants("local authority")),
      runSource("explore_education", () => searchSchools("academy")),
      runSource("cqc", () => fetchCqcProviders()),
      runSource("ofsted", () => fetchOfstedInspectionDatasets()),
    ]),

    // ENVIRONMENT & ENERGY
    Promise.all([
      runSource("environment_agency_floods", () => fetchFloodWarnings()),
      runSource("defra_datasets", () => searchDefraDatasets()),
      runSource("octopus_energy", () => fetchGreenEnergyProducts()),
      runSource("natural_resources_wales", () => searchNrwDatasets()),
    ]),

    // NEWS & MEDIA
    Promise.all([
      runSource("bbc_business", () => fetchBbcBusinessNews()),
      runSource("bbc_politics", () => fetchBbcPoliticsNews()),
    ]),

    // CRIME & JUSTICE
    Promise.all([
      runSource("uk_police", () => fetchPoliceForces()),
      runSource("courts_tribunals", () => searchCourts("procurement")),
    ]),

    // OTHER
    Promise.all([
      runSource("hansard_procurement", () => fetchRecentProcurementDebates()),
      runSource("energy_tech_list", () => fetchEnergyEfficiencyData()),
    ]),

    // EXISTING EARLY SIGNAL SOURCES (already have their own pipeline, also feed canonical_ingest)
    Promise.all([
      runSource("ons_construction", () => fetchConstructionOutput().then(d => [d]).catch(() => [])),
      runSource("ons_business_demography", () => fetchBusinessDemography().then(d => [d]).catch(() => [])),
      runSource("planning_data", () => fetchPlanningSnapshot().then(d => d ? [d] : []).catch(() => [])),
      runSource("govuk_content", () => fetchGovukSnapshot().then(d => d ? [d] : []).catch(() => [])),
      runSource("land_registry", () => fetchRecentTransactions("London").catch(() => [])),
    ]),
  ]);

  const results = [
    ...procurementResults,
    ...govResults,
    ...transportResults,
    ...healthResults,
    ...envResults,
    ...newsResults,
    ...crimeResults,
    ...otherResults,
    ...signalResults,
  ];

  const total = results.reduce((s, r) => s + r.recordsIngested, 0);
  const errors = results.filter(r => r.error).length;
  console.log(`[ingest] Complete: ${total} records across ${results.length} sources (${errors} errors)`);

  return results;
}

/**
 * Run a single named source ingest by source ID.
 */
export async function runSourceIngest(sourceId: string): Promise<IngestResult> {
  const sourceMap: Record<string, () => Promise<unknown[]>> = {
    sell2wales: () => fetchSell2WalesNotices(),
    nhsbsa_pipeline: () => fetchNhsbsaPipelineData(),
    gazette_procurement: () => fetchRecentProcurementNotices(),
    gazette_insolvency: () => fetchRecentInsolvencyNotices(),
    public_contracts_scotland: () => fetchPcsNotices(),
    esourcing_ni: () => fetchNiProcurementNotices(),
    nomis_demography: () => fetchBusinessDemographyByArea(),
    uk_parliament: () => searchParliamentaryQuestions("procurement"),
    local_authority_spending: () => searchLocalAuthoritySpending(),
    data_gov_uk: () => fetchProcurementDatasets(),
    tfl: () => fetchLineStatuses(),
    bus_open_data: () => fetchBusDatasets(),
    networkrail: () => fetchNetworkRailAssets(),
    nhs_content: () => searchNhsOrganisations("NHS Trust"),
    three_sixty_giving: () => searchGrants("public sector"),
    explore_education: () => searchSchools("academy"),
    cqc: () => fetchCqcProviders(),
    ofsted: () => fetchOfstedInspectionDatasets(),
    environment_agency_floods: () => fetchFloodWarnings(),
    defra_datasets: () => searchDefraDatasets(),
    octopus_energy: () => fetchGreenEnergyProducts(),
    natural_resources_wales: () => searchNrwDatasets(),
    bbc_business: () => fetchBbcBusinessNews(),
    bbc_politics: () => fetchBbcPoliticsNews(),
    uk_police: () => fetchPoliceForces(),
    courts_tribunals: () => searchCourts("procurement"),
    hansard_procurement: () => fetchRecentProcurementDebates(),
    energy_tech_list: () => fetchEnergyEfficiencyData(),
    ons_construction: () => fetchConstructionOutput().then(d => [d]).catch(() => []),
    ons: () => fetchBusinessDemography().then(d => [d]).catch(() => []),
    planning_data: () => fetchPlanningSnapshot().then(d => d ? [d] : []).catch(() => []),
    govuk_content: () => fetchGovukSnapshot().then(d => d ? [d] : []).catch(() => []),
    land_registry: () => fetchRecentTransactions("London").catch(() => []),
  };

  const fn = sourceMap[sourceId];
  if (!fn) {
    return { source: sourceId, recordsIngested: 0, durationMs: 0, error: `Unknown source: ${sourceId}` };
  }
  return runSource(sourceId, fn);
}
