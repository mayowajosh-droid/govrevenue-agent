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
  "55": "Accommodation",
  "56": "Food & Beverage",
  "41": "Construction",
  "42": "Civil Engineering",
  "43": "Specialist Construction",
  "49": "Transport",
  "52": "Logistics",
  "58": "Publishing",
  "62": "Software & Tech",
  "63": "Information Services",
  "64": "Finance",
  "66": "Financial Services",
  "68": "Real Estate",
  "69": "Legal & Accounting",
  "70": "Management Consulting",
  "71": "Architecture & Engineering",
  "72": "Scientific Research",
  "73": "Advertising & Marketing",
  "74": "Creative & Design",
  "77": "Rental & Leasing",
  "78": "Recruitment",
  "79": "Travel & Tourism",
  "80": "Security",
  "81": "Facilities & Cleaning",
  "82": "Business Support",
  "85": "Education",
  "86": "Health",
  "87": "Social Care",
  "88": "Social Work",
  "90": "Arts & Entertainment",
  "93": "Sports & Fitness",
  "95": "Repair Services",
  "96": "Personal Services",
};

function sectorFromSic(sicCodes: string[]): string {
  for (const code of sicCodes) {
    const prefix2 = code.slice(0, 2);
    if (SIC_SECTOR_LABELS[prefix2]) return SIC_SECTOR_LABELS[prefix2];
  }
  return "Other";
}

// Key SIC prefixes to fetch sector-specific data for.
// Each gets its own API call → 100 businesses per sector → rich county distributions.
const SECTOR_SIC_PREFIXES = [
  "47",  // Retail
  "56",  // Food & Beverage
  "86",  // Health
  "93",  // Sports & Fitness
  "62",  // Software & Tech
  "74",  // Creative & Design
  "41",  // Construction
  "45",  // Automotive
  "69",  // Legal & Accounting
  "85",  // Education
  "96",  // Personal Services
  "73",  // Advertising & Marketing
];

type ChItem = {
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
};

async function fetchPage(
  authHeader: string,
  params: URLSearchParams,
): Promise<{ hits: number; items: ChItem[] }> {
  const ac = makeAbort();
  const res = await fetch(`${CH_BASE}/advanced-search/companies?${params}`, {
    headers: { Authorization: authHeader },
    signal: ac.signal,
  });
  if (!res.ok) return { hits: 0, items: [] };
  const data = await res.json() as { hits?: number; items?: ChItem[] };
  return { hits: data.hits ?? 0, items: data.items ?? [] };
}

/**
 * Companies House advanced search for recently incorporated businesses.
 * Runs sector-specific fetches for key SIC codes to build rich county
 * distributions per sector (not just 100 random businesses).
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

  const authHeader = `Basic ${Buffer.from(`${apiKey}:`).toString("base64")}`;

  try {
    const allItems: ChItem[] = [];
    let totalHits = 0;

    if (sicCodePrefix) {
      // Single-sector mode (explicit SIC filter)
      const params = new URLSearchParams({
        incorporated_from: periodFrom,
        incorporated_to: periodTo,
        size: "100",
        sic_codes: sicCodePrefix,
      });
      const { hits, items } = await fetchPage(authHeader, params);
      totalHits = hits;
      allItems.push(...items);
    } else {
      // Multi-sector mode: fetch 100 businesses per key sector in parallel batches.
      // CH rate limit is 600/5min — 12 concurrent requests is safe.
      const batches: string[][] = [];
      for (let i = 0; i < SECTOR_SIC_PREFIXES.length; i += 4) {
        batches.push(SECTOR_SIC_PREFIXES.slice(i, i + 4));
      }

      for (const batch of batches) {
        const results = await Promise.allSettled(
          batch.map(sic => {
            const params = new URLSearchParams({
              incorporated_from: periodFrom,
              incorporated_to: periodTo,
              size: "100",
              sic_codes: sic,
            });
            return fetchPage(authHeader, params);
          }),
        );
        for (const r of results) {
          if (r.status === "fulfilled") {
            totalHits += r.value.hits;
            allItems.push(...r.value.items);
          }
        }
      }

      // Also fetch 100 without SIC filter to catch uncategorized businesses
      const generalParams = new URLSearchParams({
        incorporated_from: periodFrom,
        incorporated_to: periodTo,
        size: "100",
      });
      const general = await fetchPage(authHeader, generalParams);
      totalHits = Math.max(totalHits, general.hits);
      allItems.push(...general.items);
    }

    if (!allItems.length) return empty;

    // Deduplicate by company number
    const seen = new Set<string>();
    const countyCounts: Record<string, number> = {};
    const businesses: NewBusiness[] = [];

    for (const c of allItems) {
      const num = c.company_number ?? "";
      if (seen.has(num)) continue;
      seen.add(num);

      const addr = c.registered_office_address ?? {};
      const county = addr.region ?? addr.locality ?? addr.country ?? "Unknown";
      countyCounts[county] = (countyCounts[county] ?? 0) + 1;
      const sicCodes = c.sic_codes ?? [];
      businesses.push({
        name: c.company_name ?? "",
        number: num,
        incorporatedOn: c.date_of_creation ?? "",
        sicCodes,
        sector: sectorFromSic(sicCodes),
        address: [addr.address_line_1, addr.locality].filter(Boolean).join(", "),
        county,
      });
    }

    const topCounties = Object.entries(countyCounts)
      .sort(([, a], [, b]) => b - a)
      .slice(0, 40)
      .map(([county, count]) => ({ county, count }));

    return {
      fetchedAt: new Date().toISOString(),
      totalFound: totalHits || businesses.length,
      periodFrom,
      periodTo,
      businesses,
      topCounties,
    };
  } catch { return empty; }
}
