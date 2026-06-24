const FTC_BASE = "https://findthatcharity.uk";
const TIMEOUT_MS = 10_000;

export type CharityRecord = {
  id: string;
  name: string;
  charityNumber: string;
  companyNumber: string | null;
  description: string | null;
  url: string | null;
  latestIncome: number | null;
  numEmployees: number | null;
  active: boolean;
  registrationDate: string | null;
  areas: string[];
  categories: string[];
  address: string | null;
};

function ftcAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export async function searchCharity(name: string): Promise<CharityRecord[]> {
  try {
    const url = new URL(`${FTC_BASE}/api/v1/autocomplete`);
    url.searchParams.set("q", name.trim());
    url.searchParams.set("limit", "5");
    url.searchParams.set("filters[active]", "true");

    const ac = ftcAbort();
    const res = await fetch(url.toString(), {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];

    const data = await res.json() as { result?: Array<{ id?: string; name?: string }> };
    const items = Array.isArray(data.result) ? data.result : [];

    return items
      .filter(i => i.id && i.name)
      .map(i => ({
        id: String(i.id),
        name: String(i.name),
        charityNumber: String(i.id).replace("GB-CHC-", "").replace("GB-SC-", ""),
        companyNumber: null,
        description: null,
        url: `${FTC_BASE}/charity/${encodeURIComponent(String(i.id))}`,
        latestIncome: null,
        numEmployees: null,
        active: true,
        registrationDate: null,
        areas: [],
        categories: [],
        address: null,
      }));
  } catch {
    return [];
  }
}

export async function getCharityById(charityId: string): Promise<CharityRecord | null> {
  try {
    const ac = ftcAbort();
    const res = await fetch(`${FTC_BASE}/charity/${encodeURIComponent(charityId)}.json`, {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return null;

    const data = await res.json() as {
      id?: string;
      name?: string;
      charity_number?: string;
      company_number?: string;
      description?: string;
      url?: string;
      latest_income?: number;
      num_employees?: number;
      active?: boolean;
      date_registered?: string;
      areas_of_operation?: Array<{ name: string }>;
      charity_type?: string[];
      address?: { street?: string; city?: string; postcode?: string } | string;
    };

    const addrObj = typeof data.address === "object" && data.address !== null ? data.address : null;
    const addrStr = addrObj
      ? [addrObj.street, addrObj.city, addrObj.postcode].filter(Boolean).join(", ")
      : typeof data.address === "string" ? data.address : null;

    return {
      id: String(data.id || charityId),
      name: String(data.name || ""),
      charityNumber: String(data.charity_number || ""),
      companyNumber: data.company_number ? String(data.company_number) : null,
      description: data.description ? String(data.description).slice(0, 400) : null,
      url: `${FTC_BASE}/charity/${encodeURIComponent(String(data.id || charityId))}`,
      latestIncome: data.latest_income ?? null,
      numEmployees: data.num_employees ?? null,
      active: data.active ?? true,
      registrationDate: data.date_registered ?? null,
      areas: Array.isArray(data.areas_of_operation)
        ? data.areas_of_operation.map(a => a.name).filter(Boolean)
        : [],
      categories: Array.isArray(data.charity_type) ? data.charity_type : [],
      address: addrStr,
    };
  } catch {
    return null;
  }
}
