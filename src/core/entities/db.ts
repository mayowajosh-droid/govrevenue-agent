import { Pool } from 'pg';

export async function initCanonicalTables(pool: Pool): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS canonical_ingest (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      source TEXT NOT NULL,
      source_record_id TEXT,
      raw_payload JSONB NOT NULL,
      checksum TEXT,
      ingestion_batch_id TEXT,
      entity_id UUID,
      status TEXT NOT NULL DEFAULT 'pending',
      fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      processed_at TIMESTAMPTZ
    );
    CREATE INDEX IF NOT EXISTS idx_canonical_ingest_source_status
      ON canonical_ingest (source, status, fetched_at DESC);
    CREATE INDEX IF NOT EXISTS idx_canonical_ingest_entity
      ON canonical_ingest (entity_id) WHERE entity_id IS NOT NULL;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS canonical_organisations (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      external_ids JSONB NOT NULL DEFAULT '{}',
      primary_name TEXT NOT NULL,
      aliases JSONB NOT NULL DEFAULT '[]',
      domains JSONB NOT NULL DEFAULT '[]',
      sector TEXT,
      lifecycle_stage TEXT,
      last_seen_at TIMESTAMPTZ DEFAULT NOW(),
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_canonical_orgs_name
      ON canonical_organisations (primary_name);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS canonical_persons (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      organisation_id UUID NOT NULL REFERENCES canonical_organisations(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      role TEXT,
      is_decision_maker BOOLEAN DEFAULT FALSE,
      email_inferences JSONB NOT NULL DEFAULT '[]',
      ch_officer_id TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_canonical_persons_org
      ON canonical_persons (organisation_id);
  `);

  console.log('[Canonical] Tables initialized');
}
