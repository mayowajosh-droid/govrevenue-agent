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
  sectorTotals?: Record<string, number>;
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

const SIC_SECTOR_OVERRIDES: Record<string, string> = {
  "47710": "Fashion & Clothing", "47721": "Fashion & Clothing", "47722": "Fashion & Clothing",
  "47723": "Fashion & Clothing", "47782": "Fashion & Clothing", "47789": "Fashion & Clothing",
};

function sectorFromSic(sicCodes: string[]): string {
  for (const code of sicCodes) {
    if (SIC_SECTOR_OVERRIDES[code]) return SIC_SECTOR_OVERRIDES[code];
  }
  for (const code of sicCodes) {
    const prefix2 = code.slice(0, 2);
    if (SIC_SECTOR_LABELS[prefix2]) return SIC_SECTOR_LABELS[prefix2];
  }
  return "Other";
}

// Full 5-digit SIC codes per sector — the CH API requires exact codes, not prefixes.
// Each sector gets its own API call with comma-separated codes → 100 businesses
// per sector → rich county distributions for the Atlas competitive-landscape layer.
const SECTOR_SIC_CODES: { sector: string; codes: string }[] = [
  { sector: "Retail",               codes: "47110,47190,47210,47290,47750,47760,47770,47780,47790,47910,47990" },
  { sector: "Fashion & Clothing",   codes: "47710,47721,47722,47723,47782,47789" },
  { sector: "Food & Beverage",      codes: "56101,56102,56103,56210,56301,56302" },
  { sector: "Health",               codes: "86101,86210,86220,86230,86900" },
  { sector: "Sports & Fitness",     codes: "93110,93120,93130,93199" },
  { sector: "Software & Tech",      codes: "62011,62012,62020,62030,62090" },
  { sector: "Creative & Design",    codes: "74100,74200,74900" },
  { sector: "Construction",         codes: "41100,41201,41202,43110,43210,43220,43221,43222,43290,43310,43320,43330,43341,43390,43999" },
  { sector: "Facilities Management", codes: "81100,81210,81221,81222,81229,81299" },
  { sector: "Automotive",           codes: "45111,45112,45190,45200,45310,45320,45400" },
  { sector: "Legal & Accounting",   codes: "69101,69102,69109,69201,69202,69203" },
  { sector: "Education",            codes: "85100,85200,85310,85510,85520,85590,85600" },
  { sector: "Personal Services",    codes: "96010,96020,96040,96090" },
  { sector: "Advertising & Marketing", codes: "73110,73120,73200" },
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

// Map common locality/city names to their parent county, so businesses without
// a `region` field still get assigned to the right county for map plotting.
const LOCALITY_TO_COUNTY: Record<string, string> = {
  "london": "Greater London", "westminster": "Greater London", "camden": "Greater London",
  "islington": "Greater London", "hackney": "Greater London", "tower hamlets": "Greater London",
  "southwark": "Greater London", "lambeth": "Greater London", "wandsworth": "Greater London",
  "croydon": "Greater London", "barnet": "Greater London", "ealing": "Greater London",
  "brent": "Greater London", "enfield": "Greater London", "bromley": "Greater London",
  "newham": "Greater London", "hillingdon": "Greater London", "hounslow": "Greater London",
  "greenwich": "Greater London", "lewisham": "Greater London", "walthamstow": "Greater London",
  "stratford": "Greater London", "romford": "Greater London", "ilford": "Greater London",
  "sutton": "Greater London", "kingston upon thames": "Greater London", "richmond": "Greater London",
  "surbiton": "Greater London", "hayes": "Greater London", "edgware": "Greater London",
  "harrow": "Greater London", "wembley": "Greater London", "chiswick": "Greater London",
  "manchester": "Greater Manchester", "salford": "Greater Manchester", "stockport": "Greater Manchester",
  "bolton": "Greater Manchester", "oldham": "Greater Manchester", "rochdale": "Greater Manchester",
  "bury": "Greater Manchester", "wigan": "Greater Manchester", "tameside": "Greater Manchester",
  "birmingham": "West Midlands", "wolverhampton": "West Midlands", "coventry": "West Midlands",
  "solihull": "West Midlands", "dudley": "West Midlands", "walsall": "West Midlands",
  "sandwell": "West Midlands", "west bromwich": "West Midlands",
  "leeds": "West Yorkshire", "bradford": "West Yorkshire", "wakefield": "West Yorkshire",
  "huddersfield": "West Yorkshire", "halifax": "West Yorkshire",
  "sheffield": "South Yorkshire", "rotherham": "South Yorkshire", "doncaster": "South Yorkshire",
  "barnsley": "South Yorkshire",
  "liverpool": "Merseyside", "st helens": "Merseyside", "wirral": "Merseyside",
  "sefton": "Merseyside", "knowsley": "Merseyside", "birkenhead": "Merseyside",
  "newcastle upon tyne": "Tyne and Wear", "sunderland": "Tyne and Wear",
  "gateshead": "Tyne and Wear", "south shields": "Tyne and Wear",
  "nottingham": "Nottinghamshire", "leicester": "Leicestershire", "derby": "Derbyshire",
  "northampton": "Northamptonshire", "lincoln": "Lincolnshire",
  "norwich": "Norfolk", "ipswich": "Suffolk", "cambridge": "Cambridgeshire",
  "peterborough": "Cambridgeshire", "chelmsford": "Essex", "colchester": "Essex",
  "southend-on-sea": "Essex", "basildon": "Essex",
  "brighton": "East Sussex", "brighton and hove": "East Sussex", "eastbourne": "East Sussex",
  "hastings": "East Sussex", "worthing": "West Sussex", "crawley": "West Sussex",
  "chichester": "West Sussex",
  "reading": "Berkshire", "slough": "Berkshire", "maidenhead": "Berkshire",
  "oxford": "Oxfordshire", "milton keynes": "Buckinghamshire", "aylesbury": "Buckinghamshire",
  "luton": "Bedfordshire", "bedford": "Bedfordshire",
  "guildford": "Surrey", "woking": "Surrey", "epsom": "Surrey", "reigate": "Surrey",
  "canterbury": "Kent", "maidstone": "Kent", "tunbridge wells": "Kent", "dartford": "Kent",
  "chatham": "Kent", "gravesend": "Kent",
  "southampton": "Hampshire", "portsmouth": "Hampshire", "winchester": "Hampshire",
  "basingstoke": "Hampshire", "fareham": "Hampshire",
  "bristol": "Bristol", "bath": "Somerset", "taunton": "Somerset", "yeovil": "Somerset",
  "exeter": "Devon", "plymouth": "Devon", "torquay": "Devon",
  "truro": "Cornwall", "falmouth": "Cornwall", "newquay": "Cornwall",
  "gloucester": "Gloucestershire", "cheltenham": "Gloucestershire",
  "salisbury": "Wiltshire", "swindon": "Wiltshire",
  "bournemouth": "Dorset", "poole": "Dorset", "dorchester": "Dorset",
  "chester": "Cheshire", "crewe": "Cheshire", "warrington": "Cheshire",
  "preston": "Lancashire", "blackburn": "Lancashire", "burnley": "Lancashire",
  "lancaster": "Lancashire", "blackpool": "Lancashire",
  "carlisle": "Cumbria", "kendal": "Cumbria", "barrow-in-furness": "Cumbria",
  "stoke-on-trent": "Staffordshire", "stafford": "Staffordshire", "lichfield": "Staffordshire",
  "warwick": "Warwickshire", "stratford-upon-avon": "Warwickshire", "leamington spa": "Warwickshire",
  "worcester": "Worcestershire", "redditch": "Worcestershire",
  "shrewsbury": "Shropshire", "telford": "Shropshire",
  "hereford": "Herefordshire",
  "york": "North Yorkshire", "harrogate": "North Yorkshire", "scarborough": "North Yorkshire",
  "hull": "East Riding of Yorkshire", "kingston upon hull": "East Riding of Yorkshire",
  "middlesbrough": "Cleveland", "darlington": "County Durham", "durham": "County Durham",
  "glasgow": "Strathclyde", "edinburgh": "Lothian", "aberdeen": "Grampian",
  "dundee": "Tayside", "inverness": "Highland", "stirling": "Central",
  "perth": "Tayside", "paisley": "Strathclyde",
  "cardiff": "South Glamorgan", "swansea": "West Glamorgan", "newport": "Gwent",
  "wrexham": "Clwyd", "bangor": "Gwynedd", "aberystwyth": "Dyfed",
  "belfast": "Northern Ireland",
};

function normalizeCounty(region?: string, locality?: string, country?: string): string {
  if (region) return region;
  if (locality) {
    const mapped = LOCALITY_TO_COUNTY[locality.toLowerCase()];
    if (mapped) return mapped;
    return locality;
  }
  return country ?? "Unknown";
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
    let sectorTotals: Record<string, number> = {};

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
      // Also stores total hits per sector so the density function can scale
      // sample counts to estimated actuals (100 sampled from 7,500 → multiply by 75).
      const batches: typeof SECTOR_SIC_CODES[] = [];
      for (let i = 0; i < SECTOR_SIC_CODES.length; i += 4) {
        batches.push(SECTOR_SIC_CODES.slice(i, i + 4));
      }

      for (const batch of batches) {
        const results = await Promise.allSettled(
          batch.map(entry => {
            const params = new URLSearchParams({
              incorporated_from: periodFrom,
              incorporated_to: periodTo,
              size: "100",
              sic_codes: entry.codes,
            });
            return fetchPage(authHeader, params).then(r => ({ ...r, sector: entry.sector }));
          }),
        );
        for (const r of results) {
          if (r.status === "fulfilled") {
            totalHits += r.value.hits;
            sectorTotals[r.value.sector] = r.value.hits;
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
      const county = normalizeCounty(addr.region, addr.locality, addr.country);
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
      ...(Object.keys(sectorTotals ?? {}).length ? { sectorTotals } : {}),
    };
  } catch { return empty; }
}
