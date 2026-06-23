import type { ProcurementNotice } from "../../types.js";
import type { BuyerEntity } from "./types.js";
import { upsertBuyerEntity, getBuyerEntity, upsertProcurementHistory, insertBuyerOfficers } from "./db.js";
import { fetchOfficers, fetchPscs, fetchCompanyProfile } from "./companies-house-officers.js";
import { companiesHouseSearch } from "../../fetchers/companies-house.js";

const GOV_PATTERNS: [RegExp, BuyerEntity["buyer_type"]][] = [
  [/\b(council|borough|district|county|city of|unitary)\b/i, "local_authority"],
  [/\b(nhs|health|hospital|clinical commissioning|icb|trust)\b/i, "nhs"],
  [/\b(department|ministry|cabinet|hm |hmrc|mod |home office|dwp)\b/i, "central_gov"],
  [/\b(housing association|registered provider|homes|habitat)\b/i, "housing"],
  [/\b(university|college|school|academy|education|ofsted)\b/i, "education"],
  [/\b(police|fire|ambulance|constabulary)\b/i, "police_fire"],
];

function classifyBuyerType(name: string): BuyerEntity["buyer_type"] {
  for (const [pattern, type] of GOV_PATTERNS) {
    if (pattern.test(name)) return type;
  }
  return "unknown";
}

function normaliseBuyerName(name: string): string {
  return name.toLowerCase()
    .replace(/\b(ltd|limited|plc|llp|uk|group|company|co)\b/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function inferDomain(name: string, buyerType: BuyerEntity["buyer_type"]): string | null {
  const slug = name.toLowerCase()
    .replace(/\b(council|borough of|city of|county|district|london borough of|royal borough of|metropolitan)\b/g, "")
    .replace(/[^a-z0-9]+/g, "")
    .trim();

  if (!slug) return null;

  if (buyerType === "local_authority") return `${slug}.gov.uk`;
  if (buyerType === "nhs") return `${slug}.nhs.uk`;
  if (buyerType === "police_fire") return `${slug}.police.uk`;
  return null;
}

export async function resolveAndEnrichBuyer(buyerName: string): Promise<BuyerEntity | null> {
  if (!buyerName || buyerName.length < 3) return null;

  const normalised = normaliseBuyerName(buyerName);
  const existing = await getBuyerEntity(normalised);
  if (existing) return existing;

  const buyerType = classifyBuyerType(buyerName);
  const website = inferDomain(buyerName, buyerType);

  let companyNumber: string | null = null;
  let companyStatus: string | null = null;
  let companyType: string | null = null;
  let address: string | null = null;
  let sicCodes: string[] = [];

  if (buyerType === "company" || buyerType === "unknown") {
    const chResult = await companiesHouseSearch(buyerName);
    const match = chResult.matches.find(m =>
      normaliseBuyerName(m.companyName) === normalised ||
      m.companyName.toLowerCase().includes(buyerName.toLowerCase().slice(0, 20))
    );

    if (match) {
      companyNumber = match.companyNumber;
      companyStatus = match.companyStatus;
      companyType = match.companyType;
      address = match.address;
      sicCodes = match.sicCodes;
    }
  }

  const entity = await upsertBuyerEntity({
    name: buyerName,
    normalised_name: normalised,
    company_number: companyNumber,
    company_status: companyStatus,
    company_type: companyType,
    address,
    sic_codes: sicCodes,
    website,
    buyer_type: buyerType,
    total_awards: 0,
    total_award_value: 0,
  });

  if (companyNumber) {
    const [officers, pscs] = await Promise.all([
      fetchOfficers(companyNumber),
      fetchPscs(companyNumber),
    ]);
    const allPeople = [...officers, ...pscs].map(o => ({
      ...o,
      buyer_entity_id: entity.id,
    }));
    await insertBuyerOfficers(allPeople);

    try {
      const { discoverContactsForBuyer } = await import("../email-discovery/index.js");
      await discoverContactsForBuyer(entity);
    } catch {}
  }

  return entity;
}

export async function ingestNoticesForBuyer(
  buyerName: string,
  notices: ProcurementNotice[],
  category?: string
): Promise<BuyerEntity | null> {
  const entity = await resolveAndEnrichBuyer(buyerName);
  if (!entity) return null;

  const historyRecords = notices
    .filter(n => n.buyer === buyerName)
    .map(n => ({
      buyer_entity_id: entity.id,
      notice_id: n.id,
      title: n.title,
      category: category || null,
      status: n.status,
      value_low: n.valueLow,
      value_high: n.valueHigh,
      awarded_value: n.awardedValue,
      awarded_supplier: n.awardedSupplier || null,
      published_date: n.publishedDate,
      deadline_date: n.deadlineDate,
      awarded_date: n.awardedDate,
      source: n.source,
      source_url: n.url,
    }));

  await upsertProcurementHistory(historyRecords);
  return entity;
}

export async function bulkIngestBuyers(notices: ProcurementNotice[], category?: string): Promise<number> {
  const buyerNames = [...new Set(notices.map(n => n.buyer).filter(b => b && b.length >= 3))];
  let resolved = 0;

  for (const name of buyerNames) {
    try {
      const buyerNotices = notices.filter(n => n.buyer === name);
      await ingestNoticesForBuyer(name, buyerNotices, category);
      resolved++;
    } catch (err: any) {
      console.warn(`[buyer-graph] failed to resolve "${name}": ${err?.message}`);
    }
  }

  return resolved;
}
