const CH_BASE = "https://api.company-information.service.gov.uk";
const TIMEOUT_MS = 20_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type NewBusiness = {
  name: string;
  number: string;
  incorporatedOn: string;
  sicCodes: string[];
  sector: string;
  address: string;
  county: string;
};

export type ChNewBusinessSnapshot = {
  fetchedAt: string;
  totalFound: number;
  periodFrom: string;
  periodTo: string;
  businesses: NewBusiness[];
  topCounties: { county: string; count: number }[];
};

const SIC_SECTOR_LABELS: Record<string, string> = {
  "45": "Automotive",
  "46": "Wholesale Trade",
  "47": "Retail",
  "56": "Food & Beverage",
  "41": "Construction",
  "42": "Civil Engineering",
  "43": "Specialist Construction",
  "62": "Software & Tech",
  "63": "Information Services",
  "69": "Legal & Accounting",
  "70": "Management Consulting",
  "71": "Architecture & Engineering",
  "72": "Scientific Research",
  "74": "Creative & Design",
  "85": "Education",
  "86": "Health",
  "90": "Arts & Entertainment",
  "96": "Personal Services",
};

function sectorFromSic(sicCodes: string[]): string {
  for (const code of sicCodes) {
    const prefix2 = code.slice(0, 2);
    if (SIC_SECTOR_LABELS[prefix2]) return SIC_SECTOR_LABELS[prefix2];
  }
  return "Other";
}

/**
 * Companies House advanced search for recently incorporated businesses.
 * Requires COMPANIES_HOUSE_API_KEY env var (free registration at companieshouse.gov.uk).
 * Returns empty snapshot gracefully without key.
 */
export async function fetchNewBusinessRegistrations(
  daysBack = 30,
  sicCodePrefix?: string,
): Promise<ChNewBusinessSnapshot> {
  const today = new Date();
  const from = new Date(today.getTime() - daysBack * 24 * 60 * 60 * 1000);
  const periodFrom = from.toISOString().slice(0, 10);
  const periodTo = today.toISOString().slice(0, 10);

  const empty: ChNewBusinessSnapshot = {
    fetchedAt: new Date().toISOString(),
    totalFound: 0,
    periodFrom,
    periodTo,
    businesses: [],
    topCounties: [],
  };

  const apiKey = process.env.COMPANIES_HOUSE_API_KEY;
  if (!apiKey) return empty;

  try {
    const ac = makeAbort();
    const params = new URLSearchParams({
      incorporated_from: periodFrom,
      incorporated_to: periodTo,
      size: "100",
      ...(sicCodePrefix ? { sic_codes: sicCodePrefix } : {}),
    });
    const res = await fetch(`${CH_BASE}/advanced-search/companies?${params}`, {
      headers: { Authorization: `Basic ${Buffer.from(`${apiKey}:`).toString("base64")}` },
      signal: ac.signal,
    });
    if (!res.ok) return empty;
    const data = await res.json() as {
      hits?: number;
      items?: {
        company_name?: string;
        company_number?: string;
        date_of_creation?: string;
        sic_codes?: string[];
        registered_office_address?: {
          address_line_1?: string;
          locality?: string;
          region?: string;
          country?: string;
        };
      }[];
    };

    const items = data.items ?? [];
    const countyCounts: Record<string, number> = {};

    const businesses: NewBusiness[] = items.map(c => {
      const addr = c.registered_office_address ?? {};
      const county = addr.region ?? addr.locality ?? addr.country ?? "Unknown";
      countyCounts[county] = (countyCounts[county] ?? 0) + 1;
      const sicCodes = c.sic_codes ?? [];
      return {
        name: c.company_name ?? "",
        number: c.company_number ?? "",
        incorporatedOn: c.date_of_creation ?? "",
        sicCodes,
        sector: sectorFromSic(sicCodes),
        address: [addr.address_line_1, addr.locality].filter(Boolean).join(", "),
        county,
      };
    });

    const topCounties = Object.entries(countyCounts)
      .sort(([, a], [, b]) => b - a)
      .slice(0, 10)
      .map(([county, count]) => ({ county, count }));

    return {
      fetchedAt: new Date().toISOString(),
      totalFound: data.hits ?? items.length,
      periodFrom,
      periodTo,
      businesses,
      topCounties,
    };
  } catch { return empty; }
}
