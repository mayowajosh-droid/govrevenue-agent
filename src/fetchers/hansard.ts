const BASE = "https://hansard-api.parliament.uk";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type HansardDebate = {
  id: string;
  title: string;
  date: string;
  house: string;
  debateType: string;
  url: string;
  membersCount: number;
};

export type HansardContribution = {
  memberId: string | null;
  memberName: string | null;
  text: string;
  debateTitle: string;
  date: string;
  house: string;
};

type RawDebate = {
  Id?: string;
  Title?: string;
  Date?: string;
  House?: string;
  Type?: string;
  DebateType?: string;
  ExternalId?: string;
  MembersCount?: number;
};

function debateUrl(id: string, house: string): string {
  const h = house?.toLowerCase() === "lords" ? "lords" : "commons";
  return `https://hansard.parliament.uk/${h}/debates/${id}`;
}

export async function searchHansardDebates(query: string, limit = 20): Promise<HansardDebate[]> {
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({
      "queryParameters.searchTerm": query,
      "queryParameters.take": String(limit),
      "queryParameters.skip": "0",
    });
    const res = await fetch(`${BASE}/search.json?${params}`, {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data = await res.json() as { Contributions?: RawDebate[]; Results?: RawDebate[] };
    const items = data.Contributions ?? data.Results ?? [];
    return items.map(d => ({
      id: String(d.Id || d.ExternalId || ""),
      title: String(d.Title || ""),
      date: String(d.Date || ""),
      house: String(d.House || "Commons"),
      debateType: String(d.Type || d.DebateType || ""),
      url: debateUrl(String(d.ExternalId || d.Id || ""), String(d.House || "")),
      membersCount: d.MembersCount ?? 0,
    })).filter(d => d.id);
  } catch { return []; }
}

export async function fetchRecentProcurementDebates(): Promise<HansardDebate[]> {
  return searchHansardDebates("procurement contracts public spending", 20);
}

export async function fetchRecentConstructionDebates(): Promise<HansardDebate[]> {
  return searchHansardDebates("construction housing planning", 20);
}

export async function fetchRecentHealthDebates(): Promise<HansardDebate[]> {
  return searchHansardDebates("NHS health social care", 20);
}

export async function searchHansardContributions(query: string, limit = 20): Promise<HansardContribution[]> {
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({
      "queryParameters.searchTerm": query,
      "queryParameters.take": String(limit),
      "queryParameters.searchDebatesFull": "true",
    });
    const res = await fetch(`${BASE}/search/contributions.json?${params}`, {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data = await res.json() as {
      Contributions?: {
        MemberId?: string; AttributedTo?: string;
        Value?: string; Text?: string;
        DebateTitle?: string; Date?: string; House?: string;
      }[];
    };
    return (data.Contributions ?? []).map(c => ({
      memberId: c.MemberId ?? null,
      memberName: c.AttributedTo ?? null,
      text: String(c.Value || c.Text || ""),
      debateTitle: String(c.DebateTitle || ""),
      date: String(c.Date || ""),
      house: String(c.House || "Commons"),
    }));
  } catch { return []; }
}
