import { pool } from "../../config.js";
import type { SupplierEntity, SupplierRelationship, SupplierProfile } from "./types.js";

export async function initSupplierGraphTables() {
  if (!pool) return;

  await pool.query(`
    CREATE TABLE IF NOT EXISTS supplier_entities (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      normalised_name TEXT NOT NULL,
      company_number TEXT,
      company_status TEXT,
      company_type TEXT,
      address TEXT,
      sic_codes TEXT[] NOT NULL DEFAULT '{}',
      website TEXT,
      total_wins INTEGER NOT NULL DEFAULT 0,
      total_win_value BIGINT NOT NULL DEFAULT 0,
      first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
      last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_supplier_entities_normalised ON supplier_entities (normalised_name);
    CREATE INDEX IF NOT EXISTS idx_supplier_entities_company_number ON supplier_entities (company_number) WHERE company_number IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_supplier_entities_total_wins ON supplier_entities (total_wins DESC);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS supplier_relationships (
      id TEXT PRIMARY KEY,
      supplier_entity_id TEXT NOT NULL REFERENCES supplier_entities(id) ON DELETE CASCADE,
      buyer_entity_id TEXT,
      buyer_name TEXT NOT NULL,
      notice_id TEXT NOT NULL,
      title TEXT NOT NULL,
      category TEXT,
      awarded_value BIGINT,
      awarded_date TIMESTAMPTZ,
      source TEXT NOT NULL,
      source_url TEXT NOT NULL,
      UNIQUE(supplier_entity_id, notice_id)
    );
    CREATE INDEX IF NOT EXISTS idx_supplier_rel_supplier ON supplier_relationships (supplier_entity_id);
    CREATE INDEX IF NOT EXISTS idx_supplier_rel_buyer ON supplier_relationships (buyer_entity_id) WHERE buyer_entity_id IS NOT NULL;
  `);

  console.log("[supplier-graph] tables ready");
}

function makeId() { return globalThis.crypto.randomUUID(); }
function nowIso() { return new Date().toISOString(); }

export async function upsertSupplierEntity(
  entity: Omit<SupplierEntity, "id" | "first_seen" | "last_seen" | "updated_at">
): Promise<SupplierEntity> {
  if (!pool) throw new Error("Database required for supplier graph");
  const now = nowIso();

  const r = await pool.query<SupplierEntity>(
    `INSERT INTO supplier_entities (id, name, normalised_name, company_number, company_status, company_type, address, sic_codes, website, total_wins, total_win_value, first_seen, last_seen, updated_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$12,$12)
     ON CONFLICT (normalised_name) DO UPDATE SET
       name = EXCLUDED.name,
       company_number = COALESCE(EXCLUDED.company_number, supplier_entities.company_number),
       company_status = COALESCE(EXCLUDED.company_status, supplier_entities.company_status),
       company_type   = COALESCE(EXCLUDED.company_type,   supplier_entities.company_type),
       address        = COALESCE(EXCLUDED.address,        supplier_entities.address),
       sic_codes      = CASE WHEN array_length(EXCLUDED.sic_codes, 1) > 0 THEN EXCLUDED.sic_codes ELSE supplier_entities.sic_codes END,
       website        = COALESCE(EXCLUDED.website,        supplier_entities.website),
       last_seen      = $12,
       updated_at     = $12
     RETURNING *`,
    [makeId(), entity.name, entity.normalised_name, entity.company_number, entity.company_status,
     entity.company_type, entity.address, entity.sic_codes, entity.website,
     entity.total_wins, entity.total_win_value, now]
  );
  return r.rows[0];
}

export async function getSupplierEntity(normalisedName: string): Promise<SupplierEntity | null> {
  if (!pool) return null;
  const r = await pool.query<SupplierEntity>(
    `SELECT * FROM supplier_entities WHERE normalised_name = $1`,
    [normalisedName]
  );
  return r.rows[0] ?? null;
}

export async function getSupplierEntityById(id: string): Promise<SupplierEntity | null> {
  if (!pool) return null;
  const r = await pool.query<SupplierEntity>(
    `SELECT * FROM supplier_entities WHERE id = $1`,
    [id]
  );
  return r.rows[0] ?? null;
}

export async function upsertSupplierRelationship(
  rel: Omit<SupplierRelationship, "id">
): Promise<void> {
  if (!pool) return;
  await pool.query(
    `INSERT INTO supplier_relationships (id, supplier_entity_id, buyer_entity_id, buyer_name, notice_id, title, category, awarded_value, awarded_date, source, source_url)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
     ON CONFLICT (supplier_entity_id, notice_id) DO UPDATE SET
       buyer_entity_id = COALESCE(EXCLUDED.buyer_entity_id, supplier_relationships.buyer_entity_id),
       awarded_value   = COALESCE(EXCLUDED.awarded_value, supplier_relationships.awarded_value),
       awarded_date    = COALESCE(EXCLUDED.awarded_date, supplier_relationships.awarded_date)`,
    [makeId(), rel.supplier_entity_id, rel.buyer_entity_id, rel.buyer_name, rel.notice_id,
     rel.title, rel.category, rel.awarded_value, rel.awarded_date, rel.source, rel.source_url]
  );
}

export async function recomputeSupplierTotals(supplierId: string): Promise<void> {
  if (!pool) return;
  await pool.query(
    `UPDATE supplier_entities SET
       total_wins      = (SELECT COUNT(*) FROM supplier_relationships WHERE supplier_entity_id = $1),
       total_win_value = (SELECT COALESCE(SUM(awarded_value), 0) FROM supplier_relationships WHERE supplier_entity_id = $1),
       updated_at      = now()
     WHERE id = $1`,
    [supplierId]
  );
}

export async function listSuppliers(opts: { q?: string; limit?: number; offset?: number }): Promise<SupplierEntity[]> {
  if (!pool) return [];
  const limit = Math.min(opts.limit ?? 50, 200);
  const offset = opts.offset ?? 0;

  if (opts.q) {
    const r = await pool.query<SupplierEntity>(
      `SELECT * FROM supplier_entities
       WHERE normalised_name ILIKE $1 OR name ILIKE $1
       ORDER BY total_wins DESC, total_win_value DESC
       LIMIT $2 OFFSET $3`,
      [`%${opts.q}%`, limit, offset]
    );
    return r.rows;
  }

  const r = await pool.query<SupplierEntity>(
    `SELECT * FROM supplier_entities
     ORDER BY total_wins DESC, total_win_value DESC
     LIMIT $1 OFFSET $2`,
    [limit, offset]
  );
  return r.rows;
}

export async function getSupplierRelationships(supplierId: string): Promise<SupplierRelationship[]> {
  if (!pool) return [];
  const r = await pool.query<SupplierRelationship>(
    `SELECT * FROM supplier_relationships
     WHERE supplier_entity_id = $1
     ORDER BY awarded_date DESC NULLS LAST
     LIMIT 200`,
    [supplierId]
  );
  return r.rows;
}

export async function getSupplierProfile(id: string): Promise<SupplierProfile | null> {
  const entity = await getSupplierEntityById(id);
  if (!entity) return null;

  const relationships = await getSupplierRelationships(id);

  const totalValue = relationships.reduce((s, r) => s + (r.awarded_value ?? 0), 0);
  const avgContractValue = relationships.length > 0 ? Math.round(totalValue / relationships.length) : 0;

  const buyerMap = new Map<string, { count: number; value: number }>();
  for (const r of relationships) {
    const e = buyerMap.get(r.buyer_name) ?? { count: 0, value: 0 };
    e.count++;
    e.value += r.awarded_value ?? 0;
    buyerMap.set(r.buyer_name, e);
  }
  const topBuyers = [...buyerMap.entries()]
    .map(([name, { count, value }]) => ({ name, count, value }))
    .sort((a, b) => b.count - a.count || b.value - a.value)
    .slice(0, 10);

  const catMap = new Map<string, { count: number; value: number }>();
  for (const r of relationships) {
    const cat = r.category ?? "Uncategorised";
    const e = catMap.get(cat) ?? { count: 0, value: 0 };
    e.count++;
    e.value += r.awarded_value ?? 0;
    catMap.set(cat, e);
  }
  const topCategories = [...catMap.entries()]
    .map(([category, { count, value }]) => ({ category, count, value }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 8);

  const lastActivity = relationships[0]?.awarded_date ?? null;

  return {
    entity,
    relationships,
    stats: { totalContracts: relationships.length, totalValue, avgContractValue, topBuyers, topCategories, lastActivity },
  };
}

export async function getTopSuppliersByBuyer(buyerEntityId: string, limit = 10): Promise<{ name: string; count: number; value: number; supplier_id: string | null }[]> {
  if (!pool) return [];
  const r = await pool.query<{ supplier_entity_id: string; buyer_name: string; count: string; value: string }>(
    `SELECT supplier_entity_id, MIN(buyer_name) as buyer_name, COUNT(*) as count, COALESCE(SUM(awarded_value), 0) as value
     FROM supplier_relationships
     WHERE buyer_entity_id = $1
     GROUP BY supplier_entity_id
     ORDER BY count DESC, value DESC
     LIMIT $2`,
    [buyerEntityId, limit]
  );

  // Get supplier names
  const ids = r.rows.map(row => row.supplier_entity_id);
  if (ids.length === 0) return [];

  const names = await pool.query<{ id: string; name: string }>(
    `SELECT id, name FROM supplier_entities WHERE id = ANY($1)`,
    [ids]
  );
  const nameMap = new Map(names.rows.map(n => [n.id, n.name]));

  return r.rows.map(row => ({
    name: nameMap.get(row.supplier_entity_id) ?? row.supplier_entity_id,
    count: parseInt(row.count),
    value: parseInt(row.value),
    supplier_id: row.supplier_entity_id,
  }));
}
