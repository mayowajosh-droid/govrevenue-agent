import { Pool } from "pg";

export async function initGovernanceTables(pool: Pool): Promise<void> {
  // Metadata catalog — one row per data source, updated after each fetch
  await pool.query(`
    CREATE TABLE IF NOT EXISTS metadata_catalog (
      id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      source_id           TEXT NOT NULL UNIQUE,
      source_name         TEXT NOT NULL,
      category            TEXT NOT NULL,
      description         TEXT,
      base_url            TEXT,
      requires_key        BOOLEAN NOT NULL DEFAULT FALSE,
      key_env_var         TEXT,
      is_live             BOOLEAN NOT NULL DEFAULT TRUE,
      cadence             TEXT NOT NULL DEFAULT 'daily',
      last_fetched_at     TIMESTAMPTZ,
      last_record_count   INTEGER NOT NULL DEFAULT 0,
      total_records_all_time BIGINT NOT NULL DEFAULT 0,
      avg_fetch_ms        INTEGER,
      quality_status      TEXT NOT NULL DEFAULT 'unknown',
      quality_score       DOUBLE PRECISION NOT NULL DEFAULT 0,
      retention_tier      TEXT NOT NULL DEFAULT 'raw',
      retention_days      INTEGER NOT NULL DEFAULT 730,
      owner               TEXT NOT NULL DEFAULT 'system',
      tags                TEXT[] NOT NULL DEFAULT '{}',
      updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_metadata_catalog_source ON metadata_catalog (source_id);
    CREATE INDEX IF NOT EXISTS idx_metadata_catalog_category ON metadata_catalog (category);
  `);

  // Data lineage nodes
  await pool.query(`
    CREATE TABLE IF NOT EXISTS lineage_nodes (
      id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      node_type   TEXT NOT NULL,
      name        TEXT NOT NULL UNIQUE,
      system      TEXT NOT NULL,
      description TEXT,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_lineage_nodes_type ON lineage_nodes (node_type);
  `);

  // Data lineage edges (directed graph)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS lineage_edges (
      id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      from_node_id          UUID NOT NULL REFERENCES lineage_nodes(id) ON DELETE CASCADE,
      to_node_id            UUID NOT NULL REFERENCES lineage_nodes(id) ON DELETE CASCADE,
      transform_description TEXT,
      records_per_run       INTEGER,
      lag_ms                INTEGER,
      created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(from_node_id, to_node_id)
    );
    CREATE INDEX IF NOT EXISTS idx_lineage_edges_from ON lineage_edges (from_node_id);
    CREATE INDEX IF NOT EXISTS idx_lineage_edges_to   ON lineage_edges (to_node_id);
  `);

  // Data dictionary
  await pool.query(`
    CREATE TABLE IF NOT EXISTS data_dictionary (
      id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      table_name          TEXT NOT NULL,
      column_name         TEXT NOT NULL,
      data_type           TEXT NOT NULL,
      description         TEXT NOT NULL DEFAULT '',
      business_term       TEXT,
      example_value       TEXT,
      is_pii              BOOLEAN NOT NULL DEFAULT FALSE,
      is_required         BOOLEAN NOT NULL DEFAULT FALSE,
      source_system_field TEXT,
      updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(table_name, column_name)
    );
    CREATE INDEX IF NOT EXISTS idx_data_dict_table ON data_dictionary (table_name);
    CREATE INDEX IF NOT EXISTS idx_data_dict_pii   ON data_dictionary (is_pii) WHERE is_pii = TRUE;
  `);

  // Quality rules
  await pool.query(`
    CREATE TABLE IF NOT EXISTS quality_rules (
      id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      source_id       TEXT,
      table_name      TEXT,
      rule_name       TEXT NOT NULL UNIQUE,
      rule_type       TEXT NOT NULL,
      rule_expression TEXT NOT NULL,
      severity        TEXT NOT NULL DEFAULT 'warn',
      enabled         BOOLEAN NOT NULL DEFAULT TRUE,
      created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_quality_rules_source ON quality_rules (source_id);
    CREATE INDEX IF NOT EXISTS idx_quality_rules_table  ON quality_rules (table_name);
  `);

  // Quality rule results (history)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS quality_rule_results (
      id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      rule_id         UUID NOT NULL REFERENCES quality_rules(id) ON DELETE CASCADE,
      run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
      status          TEXT NOT NULL,
      records_checked INTEGER NOT NULL DEFAULT 0,
      records_failed  INTEGER NOT NULL DEFAULT 0,
      failure_rate    DOUBLE PRECISION NOT NULL DEFAULT 0,
      sample          TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_quality_results_rule ON quality_rule_results (rule_id);
    CREATE INDEX IF NOT EXISTS idx_quality_results_run  ON quality_rule_results (run_at DESC);
  `);

  // Retention schedule (tracks which records are due for archive/deletion)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS retention_schedule (
      id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      table_name    TEXT NOT NULL,
      tier          TEXT NOT NULL,
      retain_days   INTEGER NOT NULL,
      archive_days  INTEGER NOT NULL,
      delete_days   INTEGER NOT NULL,
      last_run_at   TIMESTAMPTZ,
      records_purged INTEGER NOT NULL DEFAULT 0,
      notes         TEXT,
      UNIQUE(table_name)
    );
  `);

  // Audit log — immutable record of system/user actions
  await pool.query(`
    CREATE TABLE IF NOT EXISTS audit_log (
      id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      action       TEXT NOT NULL,
      actor_type   TEXT NOT NULL DEFAULT 'system',
      actor_id     TEXT,
      target_table TEXT,
      target_id    TEXT,
      detail       JSONB NOT NULL DEFAULT '{}',
      created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_audit_log_action ON audit_log (action);
    CREATE INDEX IF NOT EXISTS idx_audit_log_ts     ON audit_log (created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_audit_log_target ON audit_log (target_table, target_id);
  `);

  console.log("[governance] Governance tables ready");

  // Seed retention schedule defaults
  await seedRetentionDefaults(pool);
  // Seed default quality rules
  await seedDefaultQualityRules(pool);
  // Seed data dictionary core fields
  await seedDataDictionary(pool);
}

async function seedRetentionDefaults(pool: Pool): Promise<void> {
  const rows = [
    { table_name: "canonical_ingest",         tier: "raw",       retain_days: 730,  archive_days: 1095, delete_days: 2555 },
    { table_name: "early_signals",            tier: "processed", retain_days: 1825, archive_days: 2555, delete_days: 3650 },
    { table_name: "supplier_entities",        tier: "processed", retain_days: 1825, archive_days: 2555, delete_days: 3650 },
    { table_name: "buyer_procurement_history",tier: "processed", retain_days: 1825, archive_days: 2555, delete_days: 3650 },
    { table_name: "spatial_locations",        tier: "processed", retain_days: 1825, archive_days: 2555, delete_days: 3650 },
    { table_name: "planning_applications",    tier: "raw",       retain_days: 730,  archive_days: 1095, delete_days: 2555 },
    { table_name: "audit_log",               tier: "archived",  retain_days: 2555, archive_days: 3650, delete_days: 3650 },
    { table_name: "quality_rule_results",     tier: "raw",       retain_days: 365,  archive_days: 730,  delete_days: 1095 },
  ];

  for (const r of rows) {
    await pool.query(
      `INSERT INTO retention_schedule (table_name, tier, retain_days, archive_days, delete_days)
       VALUES ($1,$2,$3,$4,$5)
       ON CONFLICT (table_name) DO NOTHING`,
      [r.table_name, r.tier, r.retain_days, r.archive_days, r.delete_days]
    );
  }
}

async function seedDefaultQualityRules(pool: Pool): Promise<void> {
  const rules = [
    {
      rule_name: "ingest_title_not_null",
      table_name: "canonical_ingest",
      rule_type: "not_null",
      rule_expression: "SELECT COUNT(*) FROM canonical_ingest WHERE title IS NULL",
      severity: "warn",
    },
    {
      rule_name: "ingest_freshness_24h",
      table_name: "canonical_ingest",
      rule_type: "freshness",
      rule_expression: "SELECT COUNT(*) FROM canonical_ingest WHERE fetched_at < now() - interval '24 hours' AND source NOT IN ('land_registry')",
      severity: "warn",
    },
    {
      rule_name: "signals_score_range",
      table_name: "early_signals",
      rule_type: "range",
      rule_expression: "SELECT COUNT(*) FROM early_signals WHERE relevance_score < 0 OR relevance_score > 100",
      severity: "critical",
    },
    {
      rule_name: "supplier_name_not_null",
      table_name: "supplier_entities",
      rule_type: "not_null",
      rule_expression: "SELECT COUNT(*) FROM supplier_entities WHERE name IS NULL OR name = ''",
      severity: "critical",
    },
    {
      rule_name: "audit_log_completeness",
      table_name: "audit_log",
      rule_type: "completeness",
      rule_expression: "SELECT COUNT(*) FROM audit_log WHERE action IS NULL OR actor_type IS NULL",
      severity: "critical",
    },
  ];

  for (const r of rules) {
    await pool.query(
      `INSERT INTO quality_rules (source_id, table_name, rule_name, rule_type, rule_expression, severity)
       VALUES (NULL, $1, $2, $3, $4, $5)
       ON CONFLICT (rule_name) DO NOTHING`,
      [r.table_name, r.rule_name, r.rule_type, r.rule_expression, r.severity]
    );
  }
}

async function seedDataDictionary(pool: Pool): Promise<void> {
  const entries = [
    { table_name: "canonical_ingest", column_name: "id",          data_type: "uuid",        description: "Unique ingest record identifier",              is_pii: false, is_required: true  },
    { table_name: "canonical_ingest", column_name: "source",      data_type: "text",        description: "Source system identifier (e.g. contracts_finder)", is_pii: false, is_required: true  },
    { table_name: "canonical_ingest", column_name: "source_id",   data_type: "text",        description: "Original record ID from the source system",     is_pii: false, is_required: false },
    { table_name: "canonical_ingest", column_name: "title",       data_type: "text",        description: "Notice or record title as published",           is_pii: false, is_required: false },
    { table_name: "canonical_ingest", column_name: "buyer",       data_type: "text",        description: "Contracting authority / buyer organisation name",is_pii: false, is_required: false },
    { table_name: "canonical_ingest", column_name: "value",       data_type: "numeric",     description: "Contract or notice value in GBP",              is_pii: false, is_required: false },
    { table_name: "canonical_ingest", column_name: "fetched_at",  data_type: "timestamptz", description: "Timestamp when this record was ingested",       is_pii: false, is_required: true  },
    { table_name: "early_signals",    column_name: "indicator",   data_type: "text",        description: "Short name of the economic or market signal",   is_pii: false, is_required: true  },
    { table_name: "early_signals",    column_name: "value_raw",   data_type: "numeric",     description: "Raw numeric value from the source dataset",     is_pii: false, is_required: false },
    { table_name: "early_signals",    column_name: "relevance_score", data_type: "double precision", description: "Composite relevance score 0-100",      is_pii: false, is_required: true  },
    { table_name: "supplier_entities", column_name: "name",       data_type: "text",        description: "Canonical supplier name after normalisation",   is_pii: false, is_required: true  },
    { table_name: "supplier_entities", column_name: "companies_house_id", data_type: "text", description: "Companies House registration number",        is_pii: false, is_required: false },
    { table_name: "canonical_organisations", column_name: "primary_name", data_type: "text", description: "Primary resolved name for this entity",      is_pii: false, is_required: true  },
    { table_name: "canonical_organisations", column_name: "confidence", data_type: "double precision", description: "Entity resolution confidence 0-1", is_pii: false, is_required: true  },
    { table_name: "audit_log",        column_name: "actor_id",    data_type: "text",        description: "User or system ID performing the action",      is_pii: true,  is_required: false },
    { table_name: "audit_log",        column_name: "detail",      data_type: "jsonb",       description: "Structured action detail payload",             is_pii: false, is_required: true  },
  ];

  for (const e of entries) {
    await pool.query(
      `INSERT INTO data_dictionary (table_name, column_name, data_type, description, is_pii, is_required)
       VALUES ($1,$2,$3,$4,$5,$6)
       ON CONFLICT (table_name, column_name) DO NOTHING`,
      [e.table_name, e.column_name, e.data_type, e.description, e.is_pii, e.is_required]
    );
  }
}

// ── Metadata Catalog ──────────────────────────────────────────────────────────

export async function upsertMetadataCatalog(pool: Pool, entry: {
  sourceId: string;
  sourceName: string;
  category: string;
  description?: string;
  baseUrl?: string | null;
  requiresKey?: boolean;
  keyEnvVar?: string | null;
  isLive?: boolean;
  cadence?: string;
  lastFetchedAt?: string;
  lastRecordCount?: number;
  avgFetchMs?: number | null;
  qualityStatus?: string;
  qualityScore?: number;
  retentionTier?: string;
  retentionDays?: number;
  tags?: string[];
}): Promise<void> {
  await pool.query(
    `INSERT INTO metadata_catalog
       (source_id, source_name, category, description, base_url, requires_key, key_env_var,
        is_live, cadence, last_fetched_at, last_record_count, avg_fetch_ms,
        quality_status, quality_score, retention_tier, retention_days, tags, updated_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,now())
     ON CONFLICT (source_id) DO UPDATE SET
       source_name       = EXCLUDED.source_name,
       category          = EXCLUDED.category,
       description       = COALESCE(EXCLUDED.description, metadata_catalog.description),
       base_url          = COALESCE(EXCLUDED.base_url, metadata_catalog.base_url),
       requires_key      = EXCLUDED.requires_key,
       key_env_var       = EXCLUDED.key_env_var,
       is_live           = EXCLUDED.is_live,
       cadence           = EXCLUDED.cadence,
       last_fetched_at   = COALESCE(EXCLUDED.last_fetched_at, metadata_catalog.last_fetched_at),
       last_record_count = COALESCE(EXCLUDED.last_record_count, metadata_catalog.last_record_count),
       avg_fetch_ms      = COALESCE(EXCLUDED.avg_fetch_ms, metadata_catalog.avg_fetch_ms),
       quality_status    = EXCLUDED.quality_status,
       quality_score     = EXCLUDED.quality_score,
       retention_tier    = EXCLUDED.retention_tier,
       retention_days    = EXCLUDED.retention_days,
       tags              = EXCLUDED.tags,
       total_records_all_time = metadata_catalog.total_records_all_time + COALESCE(EXCLUDED.last_record_count, 0),
       updated_at        = now()`,
    [
      entry.sourceId, entry.sourceName, entry.category,
      entry.description ?? null, entry.baseUrl ?? null,
      entry.requiresKey ?? false, entry.keyEnvVar ?? null,
      entry.isLive ?? true, entry.cadence ?? "daily",
      entry.lastFetchedAt ?? null, entry.lastRecordCount ?? 0,
      entry.avgFetchMs ?? null,
      entry.qualityStatus ?? "unknown", entry.qualityScore ?? 0,
      entry.retentionTier ?? "raw", entry.retentionDays ?? 730,
      entry.tags ?? [],
    ]
  );
}

export async function getMetadataCatalog(pool: Pool): Promise<{
  sourceId: string; sourceName: string; category: string; isLive: boolean;
  cadence: string; lastFetchedAt: string | null; lastRecordCount: number;
  totalRecordsAllTime: number; avgFetchMs: number | null;
  qualityStatus: string; qualityScore: number; requiresKey: boolean;
}[]> {
  const r = await pool.query<{
    source_id: string; source_name: string; category: string; is_live: boolean;
    cadence: string; last_fetched_at: string | null; last_record_count: number;
    total_records_all_time: string; avg_fetch_ms: number | null;
    quality_status: string; quality_score: number; requires_key: boolean;
  }>(`SELECT source_id, source_name, category, is_live, cadence, last_fetched_at,
             last_record_count, total_records_all_time, avg_fetch_ms,
             quality_status, quality_score, requires_key
      FROM metadata_catalog ORDER BY category, source_name`);

  return r.rows.map(row => ({
    sourceId: row.source_id,
    sourceName: row.source_name,
    category: row.category,
    isLive: row.is_live,
    cadence: row.cadence,
    lastFetchedAt: row.last_fetched_at,
    lastRecordCount: row.last_record_count,
    totalRecordsAllTime: parseInt(row.total_records_all_time ?? "0"),
    avgFetchMs: row.avg_fetch_ms,
    qualityStatus: row.quality_status,
    qualityScore: row.quality_score,
    requiresKey: row.requires_key,
  }));
}

// ── Quality Rules Runner ──────────────────────────────────────────────────────

export async function runQualityChecks(pool: Pool): Promise<{
  ruleName: string; severity: string; status: string; failureRate: number; recordsFailed: number;
}[]> {
  const rulesR = await pool.query<{
    id: string; rule_name: string; rule_expression: string; severity: string;
  }>(`SELECT id, rule_name, rule_expression, severity FROM quality_rules WHERE enabled = TRUE`);

  const results: { ruleName: string; severity: string; status: string; failureRate: number; recordsFailed: number }[] = [];

  for (const rule of rulesR.rows) {
    try {
      const r = await pool.query<{ count: string }>(rule.rule_expression);
      const failed = parseInt(r.rows[0]?.count ?? "0");
      const totalR = await pool.query<{ count: string }>(
        `SELECT COUNT(*) FROM ${rule.rule_expression.match(/FROM\s+(\w+)/i)?.[1] ?? "canonical_ingest"}`
      );
      const total = parseInt(totalR.rows[0]?.count ?? "1");
      const failureRate = total > 0 ? failed / total : 0;
      const status: string = failed === 0 ? "pass" : failureRate > 0.1 ? "fail" : "warn";

      await pool.query(
        `INSERT INTO quality_rule_results (rule_id, status, records_checked, records_failed, failure_rate)
         VALUES ($1,$2,$3,$4,$5)`,
        [rule.id, status, total, failed, failureRate]
      );

      results.push({ ruleName: rule.rule_name, severity: rule.severity, status, failureRate, recordsFailed: failed });
    } catch {
      results.push({ ruleName: rule.rule_name, severity: rule.severity, status: "unknown", failureRate: 0, recordsFailed: 0 });
    }
  }

  return results;
}

export async function getLatestQualityResults(pool: Pool): Promise<{
  ruleName: string; severity: string; status: string; failureRate: number; runAt: string;
}[]> {
  const r = await pool.query<{
    rule_name: string; severity: string; status: string; failure_rate: number; run_at: string;
  }>(`
    SELECT DISTINCT ON (qr.id)
           qr.rule_name, qr.severity, res.status, res.failure_rate, res.run_at
    FROM quality_rules qr
    LEFT JOIN quality_rule_results res ON res.rule_id = qr.id
    ORDER BY qr.id, res.run_at DESC NULLS LAST
  `);

  return r.rows.map(row => ({
    ruleName: row.rule_name,
    severity: row.severity,
    status: row.status ?? "unknown",
    failureRate: row.failure_rate ?? 0,
    runAt: row.run_at ?? "",
  }));
}

// ── Audit Log ────────────────────────────────────────────────────────────────

export async function writeAuditLog(pool: Pool, opts: {
  action: string;
  actorType?: "system" | "user" | "scheduler";
  actorId?: string | null;
  targetTable?: string | null;
  targetId?: string | null;
  detail?: Record<string, unknown>;
}): Promise<void> {
  await pool.query(
    `INSERT INTO audit_log (action, actor_type, actor_id, target_table, target_id, detail)
     VALUES ($1,$2,$3,$4,$5,$6)`,
    [
      opts.action,
      opts.actorType ?? "system",
      opts.actorId ?? null,
      opts.targetTable ?? null,
      opts.targetId ?? null,
      JSON.stringify(opts.detail ?? {}),
    ]
  );
}

export async function getAuditLog(pool: Pool, limit = 100, action?: string): Promise<{
  id: string; action: string; actorType: string; actorId: string | null;
  targetTable: string | null; detail: Record<string, unknown>; createdAt: string;
}[]> {
  const r = await pool.query<{
    id: string; action: string; actor_type: string; actor_id: string | null;
    target_table: string | null; detail: Record<string, unknown>; created_at: string;
  }>(
    action
      ? `SELECT id, action, actor_type, actor_id, target_table, detail, created_at
         FROM audit_log WHERE action = $1 ORDER BY created_at DESC LIMIT $2`
      : `SELECT id, action, actor_type, actor_id, target_table, detail, created_at
         FROM audit_log ORDER BY created_at DESC LIMIT $1`,
    action ? [action, limit] : [limit]
  );

  return r.rows.map(row => ({
    id: row.id,
    action: row.action,
    actorType: row.actor_type,
    actorId: row.actor_id,
    targetTable: row.target_table,
    detail: row.detail,
    createdAt: row.created_at,
  }));
}

// ── Data Dictionary ───────────────────────────────────────────────────────────

export async function getDataDictionary(pool: Pool, tableName?: string): Promise<{
  tableName: string; columnName: string; dataType: string; description: string;
  isPii: boolean; isRequired: boolean; businessTerm: string | null;
}[]> {
  const r = await pool.query<{
    table_name: string; column_name: string; data_type: string; description: string;
    is_pii: boolean; is_required: boolean; business_term: string | null;
  }>(
    tableName
      ? `SELECT table_name, column_name, data_type, description, is_pii, is_required, business_term
         FROM data_dictionary WHERE table_name = $1 ORDER BY column_name`
      : `SELECT table_name, column_name, data_type, description, is_pii, is_required, business_term
         FROM data_dictionary ORDER BY table_name, column_name`,
    tableName ? [tableName] : []
  );

  return r.rows.map(row => ({
    tableName: row.table_name,
    columnName: row.column_name,
    dataType: row.data_type,
    description: row.description,
    isPii: row.is_pii,
    isRequired: row.is_required,
    businessTerm: row.business_term,
  }));
}

// ── Retention Schedule ────────────────────────────────────────────────────────

export async function getRetentionSchedule(pool: Pool): Promise<{
  tableName: string; tier: string; retainDays: number; archiveDays: number;
  deleteDays: number; lastRunAt: string | null; recordsPurged: number;
}[]> {
  const r = await pool.query<{
    table_name: string; tier: string; retain_days: number; archive_days: number;
    delete_days: number; last_run_at: string | null; records_purged: number;
  }>(`SELECT table_name, tier, retain_days, archive_days, delete_days, last_run_at, records_purged
      FROM retention_schedule ORDER BY tier, table_name`);

  return r.rows.map(row => ({
    tableName: row.table_name,
    tier: row.tier,
    retainDays: row.retain_days,
    archiveDays: row.archive_days,
    deleteDays: row.delete_days,
    lastRunAt: row.last_run_at,
    recordsPurged: row.records_purged,
  }));
}

export async function runRetentionPurge(pool: Pool): Promise<{ table: string; purged: number }[]> {
  const schedule = await getRetentionSchedule(pool);
  const results: { table: string; purged: number }[] = [];

  for (const s of schedule) {
    try {
      // Only purge tables we own with a created_at / fetched_at column
      let purged = 0;
      if (s.tableName === "canonical_ingest") {
        const r = await pool.query<{ count: string }>(
          `WITH deleted AS (
             DELETE FROM canonical_ingest
             WHERE fetched_at < now() - ($1 || ' days')::interval
             RETURNING id
           ) SELECT COUNT(*) as count FROM deleted`,
          [s.retainDays]
        );
        purged = parseInt(r.rows[0]?.count ?? "0");
      } else if (s.tableName === "quality_rule_results") {
        const r = await pool.query<{ count: string }>(
          `WITH deleted AS (
             DELETE FROM quality_rule_results
             WHERE run_at < now() - ($1 || ' days')::interval
             RETURNING id
           ) SELECT COUNT(*) as count FROM deleted`,
          [s.retainDays]
        );
        purged = parseInt(r.rows[0]?.count ?? "0");
      }

      if (purged > 0) {
        await pool.query(
          `UPDATE retention_schedule SET last_run_at = now(), records_purged = records_purged + $1
           WHERE table_name = $2`,
          [purged, s.tableName]
        );
      }

      results.push({ table: s.tableName, purged });
    } catch {
      results.push({ table: s.tableName, purged: 0 });
    }
  }

  return results;
}

// ── Lineage ──────────────────────────────────────────────────────────────────

export async function seedLineageGraph(pool: Pool): Promise<void> {
  const nodes = [
    { node_type: "source",    name: "contracts_finder",   system: "HMCTS / Crown Commercial Service", description: "UK Government Contracts Finder API" },
    { node_type: "source",    name: "find_a_tender",       system: "Find a Tender Service",            description: "Above-threshold UK tenders (OCDS)" },
    { node_type: "source",    name: "companies_house",     system: "Companies House",                  description: "UK company registration and filing data" },
    { node_type: "source",    name: "ons_api",             system: "Office for National Statistics",   description: "Construction output + business demography" },
    { node_type: "source",    name: "planning_data_api",   system: "DLUHC Planning Data",              description: "England planning applications" },
    { node_type: "source",    name: "land_registry",       system: "HM Land Registry",                description: "Property title and transaction data" },
    { node_type: "transform", name: "canonical_ingest",    system: "AtlasRevenue",                     description: "Raw normalisation landing zone" },
    { node_type: "transform", name: "entity_resolution",   system: "AtlasRevenue",                     description: "Name matching + deduplication layer" },
    { node_type: "transform", name: "signal_aggregation",  system: "AtlasRevenue",                     description: "Score + deduplicate early signals" },
    { node_type: "store",     name: "supplier_graph",      system: "AtlasRevenue DB",                  description: "Resolved supplier entities + relationships" },
    { node_type: "store",     name: "buyer_graph",         system: "AtlasRevenue DB",                  description: "Buyer entities + procurement history" },
    { node_type: "store",     name: "geospatial_layer",    system: "AtlasRevenue DB",                  description: "Spatial locations + planning applications" },
    { node_type: "output",    name: "desk_intelligence",   system: "AtlasRevenue Frontend",            description: "Per-sector intelligence desk pages" },
    { node_type: "output",    name: "report_engine",       system: "AtlasRevenue Reports",             description: "LLM-generated procurement intelligence reports" },
    { node_type: "output",    name: "api_endpoints",       system: "AtlasRevenue API",                 description: "Public REST API for entity + signal data" },
  ];

  const nodeIds: Record<string, string> = {};

  for (const n of nodes) {
    const r = await pool.query<{ id: string }>(
      `INSERT INTO lineage_nodes (node_type, name, system, description)
       VALUES ($1,$2,$3,$4)
       ON CONFLICT (name) DO UPDATE SET description = EXCLUDED.description
       RETURNING id`,
      [n.node_type, n.name, n.system, n.description]
    );
    nodeIds[n.name] = r.rows[0]!.id;
  }

  const edges = [
    ["contracts_finder",  "canonical_ingest",   "REST fetch → normalise"],
    ["find_a_tender",     "canonical_ingest",   "OCDS fetch → normalise"],
    ["ons_api",           "canonical_ingest",   "Dataset fetch → normalise"],
    ["planning_data_api", "canonical_ingest",   "API fetch → normalise"],
    ["land_registry",     "canonical_ingest",   "SPARQL fetch → normalise"],
    ["canonical_ingest",  "entity_resolution",  "Name match + merge"],
    ["canonical_ingest",  "signal_aggregation", "Indicator extraction + scoring"],
    ["companies_house",   "entity_resolution",  "CH lookup during resolution"],
    ["entity_resolution", "supplier_graph",     "Resolved entity upsert"],
    ["entity_resolution", "buyer_graph",        "Resolved buyer upsert"],
    ["canonical_ingest",  "geospatial_layer",   "Postcode → lat/lon geocode"],
    ["signal_aggregation","desk_intelligence",  "Scored signals per sector"],
    ["supplier_graph",    "report_engine",      "Supplier context for LLM"],
    ["buyer_graph",       "report_engine",      "Buyer history for LLM"],
    ["signal_aggregation","api_endpoints",      "Signal API responses"],
    ["supplier_graph",    "api_endpoints",      "Supplier API responses"],
  ];

  for (const [from, to, desc] of edges) {
    const fromId = nodeIds[from];
    const toId = nodeIds[to];
    if (!fromId || !toId) continue;
    await pool.query(
      `INSERT INTO lineage_edges (from_node_id, to_node_id, transform_description)
       VALUES ($1,$2,$3)
       ON CONFLICT (from_node_id, to_node_id) DO NOTHING`,
      [fromId, toId, desc]
    );
  }
}

export async function getLineageGraph(pool: Pool): Promise<{
  nodes: { id: string; nodeType: string; name: string; system: string; description: string | null }[];
  edges: { fromNodeId: string; toNodeId: string; transformDescription: string | null }[];
}> {
  const [nodesR, edgesR] = await Promise.all([
    pool.query<{ id: string; node_type: string; name: string; system: string; description: string | null }>(
      `SELECT id, node_type, name, system, description FROM lineage_nodes ORDER BY node_type, name`
    ),
    pool.query<{ from_node_id: string; to_node_id: string; transform_description: string | null }>(
      `SELECT from_node_id, to_node_id, transform_description FROM lineage_edges`
    ),
  ]);

  return {
    nodes: nodesR.rows.map(r => ({ id: r.id, nodeType: r.node_type, name: r.name, system: r.system, description: r.description })),
    edges: edgesR.rows.map(r => ({ fromNodeId: r.from_node_id, toNodeId: r.to_node_id, transformDescription: r.transform_description })),
  };
}

// ── Governance Summary ────────────────────────────────────────────────────────

export async function getGovernanceSummary(pool: Pool): Promise<{
  totalSources: number;
  liveSources: number;
  totalRecordsIngested: bigint | number;
  qualityPassRate: number;
  criticalRulesFailing: number;
  piiFieldCount: number;
  retentionTableCount: number;
  lastAuditAt: string | null;
}> {
  const [catalog, qr, pii, retention, audit] = await Promise.all([
    pool.query<{ total: string; live: string; total_records: string }>(
      `SELECT COUNT(*) as total, SUM(CASE WHEN is_live THEN 1 ELSE 0 END) as live,
              SUM(total_records_all_time) as total_records
       FROM metadata_catalog`
    ),
    pool.query<{ pass: string; total: string; critical_fail: string }>(
      `SELECT
         SUM(CASE WHEN res.status = 'pass' THEN 1 ELSE 0 END) as pass,
         COUNT(qr.id) as total,
         SUM(CASE WHEN res.status = 'fail' AND qr.severity = 'critical' THEN 1 ELSE 0 END) as critical_fail
       FROM quality_rules qr
       LEFT JOIN LATERAL (
         SELECT status FROM quality_rule_results WHERE rule_id = qr.id ORDER BY run_at DESC LIMIT 1
       ) res ON TRUE`
    ),
    pool.query<{ count: string }>(`SELECT COUNT(*) as count FROM data_dictionary WHERE is_pii = TRUE`),
    pool.query<{ count: string }>(`SELECT COUNT(*) as count FROM retention_schedule`),
    pool.query<{ created_at: string }>(`SELECT created_at FROM audit_log ORDER BY created_at DESC LIMIT 1`),
  ]);

  const total = parseInt(catalog.rows[0]?.total ?? "0");
  const live = parseInt(catalog.rows[0]?.live ?? "0");
  const pass = parseInt(qr.rows[0]?.pass ?? "0");
  const totalRules = parseInt(qr.rows[0]?.total ?? "0");
  const criticalFail = parseInt(qr.rows[0]?.critical_fail ?? "0");

  return {
    totalSources: total,
    liveSources: live,
    totalRecordsIngested: parseInt(catalog.rows[0]?.total_records ?? "0"),
    qualityPassRate: totalRules > 0 ? Math.round((pass / totalRules) * 100) : 0,
    criticalRulesFailing: criticalFail,
    piiFieldCount: parseInt(pii.rows[0]?.count ?? "0"),
    retentionTableCount: parseInt(retention.rows[0]?.count ?? "0"),
    lastAuditAt: audit.rows[0]?.created_at ?? null,
  };
}
