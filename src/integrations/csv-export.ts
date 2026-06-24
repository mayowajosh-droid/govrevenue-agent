// Minimal CSV serialiser — no external deps

function escCsv(v: unknown): string {
  if (v === null || v === undefined) return "";
  const s = String(v);
  if (s.includes(",") || s.includes('"') || s.includes("\n") || s.includes("\r")) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

export function toCsv(headers: string[], rows: unknown[][]): string {
  const head = headers.map(escCsv).join(",");
  const body = rows.map(row => row.map(escCsv).join(",")).join("\r\n");
  return head + "\r\n" + body;
}

// ── Per-entity serialisers ────────────────────────────────────────────────────

export function exportOpportunitiesCsv(notices: {
  title?: string; buyer?: string; awardedValue?: number | null;
  publishedDate?: string; deadlineDate?: string; awardedDate?: string;
  awardedSupplier?: string; status?: string; url?: string;
}[]): string {
  const headers = ["Title", "Buyer", "Status", "Value (£)", "Published", "Deadline", "Awarded Date", "Winner", "URL"];
  const rows = notices.map(n => [
    n.title ?? "",
    n.buyer ?? "",
    n.status ?? "",
    n.awardedValue ?? "",
    n.publishedDate ?? "",
    n.deadlineDate ?? "",
    n.awardedDate ?? "",
    n.awardedSupplier ?? "",
    n.url ?? "",
  ]);
  return toCsv(headers, rows);
}

export function exportSuppliersCsv(suppliers: {
  name?: string; companiesHouseId?: string; postcode?: string;
  primarySector?: string; totalContracts?: number;
  totalContractValue?: number; topBuyer?: string;
}[]): string {
  const headers = ["Name", "Companies House ID", "Postcode", "Primary Sector", "Total Contracts", "Total Value (£)", "Top Buyer"];
  const rows = suppliers.map(s => [
    s.name ?? "", s.companiesHouseId ?? "", s.postcode ?? "",
    s.primarySector ?? "", s.totalContracts ?? 0, s.totalContractValue ?? 0, s.topBuyer ?? "",
  ]);
  return toCsv(headers, rows);
}

export function exportBuyersCsv(buyers: {
  name?: string; region?: string; sector?: string;
  totalSpend?: number; contractCount?: number; openTenders?: number;
}[]): string {
  const headers = ["Name", "Region", "Sector", "Total Spend (£)", "Contracts", "Open Tenders"];
  const rows = buyers.map(b => [
    b.name ?? "", b.region ?? "", b.sector ?? "",
    b.totalSpend ?? 0, b.contractCount ?? 0, b.openTenders ?? 0,
  ]);
  return toCsv(headers, rows);
}

export function exportSignalsCsv(signals: {
  indicator?: string; source?: string; region?: string;
  valueRaw?: number | null; period?: string; significance?: string;
  relevanceScore?: number; fetchedAt?: string;
}[]): string {
  const headers = ["Indicator", "Source", "Region", "Value", "Period", "Significance", "Relevance Score", "Fetched At"];
  const rows = signals.map(s => [
    s.indicator ?? "", s.source ?? "", s.region ?? "",
    s.valueRaw ?? "", s.period ?? "", s.significance ?? "",
    s.relevanceScore ?? "", s.fetchedAt ?? "",
  ]);
  return toCsv(headers, rows);
}

export function exportIngestCsv(records: {
  source?: string; title?: string; buyer?: string;
  value?: number | null; fetchedAt?: string; status?: string;
}[]): string {
  const headers = ["Source", "Title", "Buyer", "Value (£)", "Fetched At", "Status"];
  const rows = records.map(r => [
    r.source ?? "", r.title ?? "", r.buyer ?? "",
    r.value ?? "", r.fetchedAt ?? "", r.status ?? "",
  ]);
  return toCsv(headers, rows);
}

export function exportCatalogCsv(catalog: {
  sourceId?: string; sourceName?: string; category?: string;
  cadence?: string; isLive?: boolean; lastRecordCount?: number;
  totalRecordsAllTime?: number; qualityStatus?: string; avgFetchMs?: number | null;
}[]): string {
  const headers = ["Source ID", "Source Name", "Category", "Cadence", "Live", "Last Record Count", "Total Records", "Quality", "Avg Fetch (ms)"];
  const rows = catalog.map(c => [
    c.sourceId ?? "", c.sourceName ?? "", c.category ?? "",
    c.cadence ?? "", c.isLive ? "Yes" : "No",
    c.lastRecordCount ?? 0, c.totalRecordsAllTime ?? 0,
    c.qualityStatus ?? "", c.avgFetchMs ?? "",
  ]);
  return toCsv(headers, rows);
}

export function exportPlanningCsv(apps: {
  reference?: string | null; description?: string | null; status?: string | null;
  applicantName?: string | null; address?: string | null; localAuthority?: string | null;
  estimatedValue?: number | null; receivedDate?: string | null;
}[]): string {
  const headers = ["Reference", "Description", "Status", "Applicant", "Address", "Local Authority", "Est Value (£)", "Received Date"];
  const rows = apps.map(a => [
    a.reference ?? "", a.description ?? "", a.status ?? "",
    a.applicantName ?? "", a.address ?? "", a.localAuthority ?? "",
    a.estimatedValue ?? "", a.receivedDate ?? "",
  ]);
  return toCsv(headers, rows);
}
