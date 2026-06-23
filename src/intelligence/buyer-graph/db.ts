import { pool } from "../../config.js";
import type { BuyerEntity, BuyerOfficer, BuyerProcurementHistory } from "./types.js";

export async function initBuyerGraphTables() {
  if (!pool) return;

  await pool.query(`
    CREATE TABLE IF NOT EXISTS buyer_entities (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      normalised_name TEXT NOT NULL,
      company_number TEXT,
      company_status TEXT,
      company_type TEXT,
      address TEXT,
      sic_codes TEXT[] NOT NULL DEFAULT '{}',
      website TEXT,
      buyer_type TEXT NOT NULL DEFAULT 'unknown',
      total_awards INTEGER NOT NULL DEFAULT 0,
      total_award_value BIGINT NOT NULL DEFAULT 0,
      first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
      last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_buyer_entities_normalised ON buyer_entities (normalised_name);
    CREATE INDEX IF NOT EXISTS idx_buyer_entities_company_number ON buyer_entities (company_number) WHERE company_number IS NOT NULL;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS buyer_officers (
      id TEXT PRIMARY KEY,
      buyer_entity_id TEXT NOT NULL REFERENCES buyer_entities(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      role TEXT NOT NULL,
      appointed_on DATE,
      resigned_on DATE,
      nationality TEXT,
      occupation TEXT,
      source TEXT NOT NULL DEFAULT 'officer',
      fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_buyer_officers_entity ON buyer_officers (buyer_entity_id);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS buyer_procurement_history (
      id TEXT PRIMARY KEY,
      buyer_entity_id TEXT NOT NULL REFERENCES buyer_entities(id) ON DELETE CASCADE,
      notice_id TEXT NOT NULL,
      title TEXT NOT NULL,
      category TEXT,
      status TEXT NOT NULL,
      value_low BIGINT,
      value_high BIGINT,
      awarded_value BIGINT,
      awarded_supplier TEXT,
      published_date TIMESTAMPTZ,
      deadline_date TIMESTAMPTZ,
      awarded_date TIMESTAMPTZ,
      source TEXT NOT NULL,
      source_url TEXT NOT NULL,
      UNIQUE(buyer_entity_id, notice_id)
    );
    CREATE INDEX IF NOT EXISTS idx_buyer_history_entity ON buyer_procurement_history (buyer_entity_id);
  `);

  console.log("[buyer-graph] tables ready");
}

function makeId() { return globalThis.crypto.randomUUID(); }
function nowIso() { return new Date().toISOString(); }

export async function upsertBuyerEntity(entity: Omit<BuyerEntity, "id" | "first_seen" | "last_seen" | "updated_at">): Promise<BuyerEntity> {
  const now = nowIso();
  if (!pool) throw new Error("Database required for buyer graph");

  const r = await pool.query<BuyerEntity>(
    `INSERT INTO buyer_entities (id, name, normalised_name, company_number, company_status, company_type, address, sic_codes, website, buyer_type, total_awards, total_award_value, first_seen, last_seen, updated_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$13,$13)
     ON CONFLICT (normalised_name)
     DO UPDATE SET
       name = CASE WHEN LENGTH(EXCLUDED.name) > LENGTH(buyer_entities.name) THEN EXCLUDED.name ELSE buyer_entities.name END,
       company_number = COALESCE(EXCLUDED.company_number, buyer_entities.company_number),
       company_status = COALESCE(EXCLUDED.company_status, buyer_entities.company_status),
       company_type = COALESCE(EXCLUDED.company_type, buyer_entities.company_type),
       address = COALESCE(EXCLUDED.address, buyer_entities.address),
       sic_codes = CASE WHEN array_length(EXCLUDED.sic_codes, 1) > 0 THEN EXCLUDED.sic_codes ELSE buyer_entities.sic_codes END,
       website = COALESCE(EXCLUDED.website, buyer_entities.website),
       buyer_type = CASE WHEN EXCLUDED.buyer_type != 'unknown' THEN EXCLUDED.buyer_type ELSE buyer_entities.buyer_type END,
       total_awards = buyer_entities.total_awards + EXCLUDED.total_awards,
       total_award_value = buyer_entities.total_award_value + EXCLUDED.total_award_value,
       last_seen = EXCLUDED.last_seen,
       updated_at = EXCLUDED.updated_at
     RETURNING *`,
    [makeId(), entity.name, entity.normalised_name, entity.company_number, entity.company_status,
     entity.company_type, entity.address, entity.sic_codes, entity.website, entity.buyer_type,
     entity.total_awards, entity.total_award_value, now]
  );
  return r.rows[0];
}

export async function getBuyerEntity(normalisedName: string): Promise<BuyerEntity | null> {
  if (!pool) return null;
  const r = await pool.query<BuyerEntity>(
    `SELECT * FROM buyer_entities WHERE normalised_name = $1`, [normalisedName]
  );
  return r.rows[0] || null;
}

export async function getBuyerEntityById(id: string): Promise<BuyerEntity | null> {
  if (!pool) return null;
  const r = await pool.query<BuyerEntity>(
    `SELECT * FROM buyer_entities WHERE id = $1`, [id]
  );
  return r.rows[0] || null;
}

export async function insertBuyerOfficers(officers: Omit<BuyerOfficer, "id" | "fetched_at">[]): Promise<void> {
  if (!pool || officers.length === 0) return;
  const now = nowIso();
  for (const o of officers) {
    await pool.query(
      `INSERT INTO buyer_officers (id, buyer_entity_id, name, role, appointed_on, resigned_on, nationality, occupation, source, fetched_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
       ON CONFLICT DO NOTHING`,
      [makeId(), o.buyer_entity_id, o.name, o.role, o.appointed_on, o.resigned_on,
       o.nationality, o.occupation, o.source, now]
    );
  }
}

export async function getBuyerOfficers(entityId: string): Promise<BuyerOfficer[]> {
  if (!pool) return [];
  const r = await pool.query<BuyerOfficer>(
    `SELECT * FROM buyer_officers WHERE buyer_entity_id = $1 AND resigned_on IS NULL ORDER BY appointed_on DESC`, [entityId]
  );
  return r.rows;
}

export async function upsertProcurementHistory(records: Omit<BuyerProcurementHistory, "id">[]): Promise<void> {
  if (!pool || records.length === 0) return;
  for (const rec of records) {
    await pool.query(
      `INSERT INTO buyer_procurement_history (id, buyer_entity_id, notice_id, title, category, status, value_low, value_high, awarded_value, awarded_supplier, published_date, deadline_date, awarded_date, source, source_url)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
       ON CONFLICT (buyer_entity_id, notice_id) DO UPDATE SET
         status = EXCLUDED.status,
         awarded_value = COALESCE(EXCLUDED.awarded_value, buyer_procurement_history.awarded_value),
         awarded_supplier = COALESCE(EXCLUDED.awarded_supplier, buyer_procurement_history.awarded_supplier),
         awarded_date = COALESCE(EXCLUDED.awarded_date, buyer_procurement_history.awarded_date)`,
      [makeId(), rec.buyer_entity_id, rec.notice_id, rec.title, rec.category, rec.status,
       rec.value_low, rec.value_high, rec.awarded_value, rec.awarded_supplier,
       rec.published_date, rec.deadline_date, rec.awarded_date, rec.source, rec.source_url]
    );
  }
}

export async function getBuyerHistory(entityId: string, limit = 50): Promise<BuyerProcurementHistory[]> {
  if (!pool) return [];
  const r = await pool.query<BuyerProcurementHistory>(
    `SELECT * FROM buyer_procurement_history WHERE buyer_entity_id = $1 ORDER BY COALESCE(published_date, awarded_date) DESC NULLS LAST LIMIT $2`,
    [entityId, limit]
  );
  return r.rows;
}

export async function getTopBuyerEntities(limit = 20): Promise<BuyerEntity[]> {
  if (!pool) return [];
  const r = await pool.query<BuyerEntity>(
    `SELECT * FROM buyer_entities ORDER BY total_awards DESC, total_award_value DESC LIMIT $1`, [limit]
  );
  return r.rows;
}

export async function searchBuyerEntities(query: string, limit = 10): Promise<BuyerEntity[]> {
  if (!pool) return [];
  const r = await pool.query<BuyerEntity>(
    `SELECT * FROM buyer_entities WHERE normalised_name LIKE $1 OR name ILIKE $2 ORDER BY total_awards DESC LIMIT $3`,
    [`%${normaliseName(query)}%`, `%${query}%`, limit]
  );
  return r.rows;
}

function normaliseName(name: string): string {
  return name.toLowerCase()
    .replace(/\b(ltd|limited|plc|llp|uk|group|company|co|council|borough|district|county|city of|royal borough of|london borough of)\b/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}
