const BASE = "https://api.charitycommission.gov.uk/register/api";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

function getApiKey(): string | null {
  return process.env.CHARITY_COMMISSION_API_KEY ?? null;
}

export type CharityCommissionRecord = {
  charityNumber: string;
  name: string;
  status: string;
  registrationDate: string | null;
  removalDate: string | null;
  income: number | null;
  spending: number | null;
  activities: string | null;
  areaOfOperation: string[];
  website: string | null;
  address: string | null;
};

function mapSearchResult(raw: Record<string, unknown>): CharityCommissionRecord {
  return {
    charityNumber: String(raw.charity_number ?? raw.charityNumber ?? ""),
    name: String(raw.charity_name ?? raw.name ?? ""),
    status: String(raw.charity_status ?? raw.status ?? ""),
    registrationDate: raw.date_of_registration
      ? String(raw.date_of_registration)
      : raw.registrationDate
        ? String(raw.registrationDate)
        : null,
    removalDate: raw.date_of_removal
      ? String(raw.date_of_removal)
      : raw.removalDate
        ? String(raw.removalDate)
        : null,
    income: raw.latest_income != null ? Number(raw.latest_income) || null : null,
    spending: raw.latest_expenditure != null ? Number(raw.latest_expenditure) || null : null,
    activities: raw.activities ? String(raw.activities) : null,
    areaOfOperation: Array.isArray(raw.area_of_operation)
      ? raw.area_of_operation.map(String)
      : Array.isArray(raw.areaOfOperation)
        ? (raw.areaOfOperation as unknown[]).map(String)
        : [],
    website: raw.web ? String(raw.web) : raw.website ? String(raw.website) : null,
    address: raw.address ? String(raw.address) : null,
  };
}

/**
 * Search charities by name.
 * Requires CHARITY_COMMISSION_API_KEY env var.
 */
export async function searchCharities(name: string): Promise<CharityCommissionRecord[]> {
  const key = getApiKey();
  if (!key) return [];
  try {
    const ac = makeAbort();
    const url =
      `${BASE}/searchcharities` +
      `?searchText=${encodeURIComponent(name)}` +
      `&pageNumber=1&pageSize=20`;
    const res = await fetch(url, {
      headers: {
        Accept: "application/json",
        "Ocp-Apim-Subscription-Key": key,
      },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data: unknown = await res.json();
    const list = Array.isArray(data) ? data : [];
    return list.map((r: Record<string, unknown>) => mapSearchResult(r));
  } catch {
    return [];
  }
}

/**
 * Get full details for a single charity.
 * Requires CHARITY_COMMISSION_API_KEY env var.
 */
export async function getCharityDetails(
  charityNumber: string
): Promise<CharityCommissionRecord | null> {
  const key = getApiKey();
  if (!key) return null;
  try {
    const ac = makeAbort();
    const url = `${BASE}/charityoverview/${encodeURIComponent(charityNumber)}/0`;
    const res = await fetch(url, {
      headers: {
        Accept: "application/json",
        "Ocp-Apim-Subscription-Key": key,
      },
      signal: ac.signal,
    });
    if (!res.ok) return null;
    const raw: unknown = await res.json();
    if (!raw || typeof raw !== "object") return null;
    return mapSearchResult(raw as Record<string, unknown>);
  } catch {
    return null;
  }
}
