import { pool } from "../../config.js";
import { companiesHouseSearch } from "../../fetchers/companies-house.js";
import {
  upsertSupplierEntity, getSupplierEntity, upsertSupplierRelationship,
  recomputeSupplierTotals,
} from "./db.js";
import type { SupplierEntity } from "./types.js";

function normaliseSupplierName(name: string): string {
  return name.toLowerCase()
    .replace(/\b(ltd|limited|plc|llp|uk|group|company|co|inc|corporation|corp)\b/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export async function resolveSupplier(supplierName: string): Promise<SupplierEntity | null> {
  if (!supplierName || supplierName.length < 3) return null;

  const normalised = normaliseSupplierName(supplierName);
  const existing = await getSupplierEntity(normalised);
  if (existing) return existing;

  let companyNumber: string | null = null;
  let companyStatus: string | null = null;
  let companyType: string | null = null;
  let address: string | null = null;
  let sicCodes: string[] = [];

  try {
    const chResult = await companiesHouseSearch(supplierName);
    const match = chResult.matches.find(m =>
      normaliseSupplierName(m.companyName) === normalised ||
      m.companyName.toLowerCase().includes(supplierName.toLowerCase().slice(0, 20))
    );
    if (match) {
      companyNumber = match.companyNumber;
      companyStatus = match.companyStatus;
      companyType = match.companyType;
      address = match.address;
      sicCodes = match.sicCodes;
    }
  } catch {}

  return await upsertSupplierEntity({
    name: supplierName,
    normalised_name: normalised,
    company_number: companyNumber,
    company_status: companyStatus,
    company_type: companyType,
    address,
    sic_codes: sicCodes,
    website: null,
    total_wins: 0,
    total_win_value: 0,
  });
}

type HistoryRow = {
  id: string;
  buyer_entity_id: string;
  buyer_name: string;
  notice_id: string;
  title: string;
  category: string | null;
  awarded_value: number | null;
  awarded_supplier: string | null;
  awarded_date: string | null;
  source: string;
  source_url: string;
};

/**
 * Reads all awarded contracts from buyer_procurement_history, resolves each
 * unique supplier name, writes supplier_relationships, and recomputes totals.
 * Safe to re-run — all inserts are upserts.
 */
export async function syncSuppliersFromHistory(): Promise<number> {
  if (!pool) return 0;

  const r = await pool.query<HistoryRow>(
    `SELECT bph.id, bph.buyer_entity_id, be.name AS buyer_name, bph.notice_id, bph.title,
            bph.category, bph.awarded_value, bph.awarded_supplier, bph.awarded_date,
            bph.source, bph.source_url
     FROM buyer_procurement_history bph
     JOIN buyer_entities be ON be.id = bph.buyer_entity_id
     WHERE bph.awarded_supplier IS NOT NULL AND bph.awarded_supplier != ''
     ORDER BY bph.awarded_date DESC NULLS LAST`
  );

  const rows = r.rows;
  const uniqueNames = [...new Set(rows.map(row => row.awarded_supplier!))];

  // Resolve all unique suppliers (with CH lookup), batched to avoid rate limits
  const supplierMap = new Map<string, SupplierEntity>();
  for (const name of uniqueNames) {
    try {
      const entity = await resolveSupplier(name);
      if (entity) supplierMap.set(name, entity);
    } catch (err: any) {
      console.warn(`[supplier-graph] failed to resolve "${name}": ${err?.message}`);
    }
    // Gentle rate-limit pacing: 200ms between CH lookups
    await new Promise(r => setTimeout(r, 200));
  }

  // Write relationships
  let written = 0;
  for (const row of rows) {
    const supplier = supplierMap.get(row.awarded_supplier!);
    if (!supplier) continue;
    try {
      await upsertSupplierRelationship({
        supplier_entity_id: supplier.id,
        buyer_entity_id: row.buyer_entity_id,
        buyer_name: row.buyer_name,
        notice_id: row.notice_id,
        title: row.title,
        category: row.category,
        awarded_value: row.awarded_value,
        awarded_date: row.awarded_date,
        source: row.source,
        source_url: row.source_url,
      });
      written++;
    } catch {}
  }

  // Recompute totals for all touched suppliers
  for (const entity of supplierMap.values()) {
    await recomputeSupplierTotals(entity.id);
  }

  return written;
}

/**
 * Called when a single procurement history record is written — resolves
 * the supplier and writes the relationship immediately, without a full sync.
 */
export async function ingestSupplierRelationship(opts: {
  supplierName: string;
  buyerEntityId: string;
  buyerName: string;
  noticeId: string;
  title: string;
  category?: string | null;
  awardedValue?: number | null;
  awardedDate?: string | null;
  source: string;
  sourceUrl: string;
}): Promise<void> {
  const entity = await resolveSupplier(opts.supplierName);
  if (!entity) return;

  await upsertSupplierRelationship({
    supplier_entity_id: entity.id,
    buyer_entity_id: opts.buyerEntityId,
    buyer_name: opts.buyerName,
    notice_id: opts.noticeId,
    title: opts.title,
    category: opts.category ?? null,
    awarded_value: opts.awardedValue ?? null,
    awarded_date: opts.awardedDate ?? null,
    source: opts.source,
    source_url: opts.sourceUrl,
  });

  await recomputeSupplierTotals(entity.id);
}
