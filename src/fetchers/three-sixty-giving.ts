const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type GrantRecord = {
  id: string;
  title: string;
  description: string | null;
  amountAwarded: number;
  currency: string;
  awardDate: string;
  recipientName: string;
  recipientId: string | null;
  fundingOrgName: string;
  fundingOrgId: string | null;
};

function mapGrant(g: any): GrantRecord {
  return {
    id: String(g.id ?? g.identifier ?? ""),
    title: String(g.title ?? ""),
    description: g.description ? String(g.description) : null,
    amountAwarded: Number(g.amountAwarded ?? g.amount_awarded ?? 0),
    currency: String(g.currency ?? "GBP"),
    awardDate: String(g.awardDate ?? g.award_date ?? ""),
    recipientName: String(
      g.recipientOrganization?.[0]?.name ?? g.recipient_name ?? "",
    ),
    recipientId: g.recipientOrganization?.[0]?.id ?? g.recipient_id ?? null,
    fundingOrgName: String(
      g.fundingOrganization?.[0]?.name ?? g.funding_org_name ?? "",
    ),
    fundingOrgId: g.fundingOrganization?.[0]?.id ?? g.funding_org_id ?? null,
  };
}

export async function searchGrants(query: string, limit = 20): Promise<GrantRecord[]> {
  try {
    const ac = makeAbort();
    // Try the search.json endpoint first (GrantNav public search)
    const res = await fetch(
      `https://grantnav.threesixtygiving.org/search.json?query=${encodeURIComponent(query)}&limit=${limit}`,
      { headers: { Accept: "application/json" }, signal: ac.signal },
    );
    if (!res.ok) return [];
    const data: any = await res.json();
    const grants: any[] = Array.isArray(data.grants)
      ? data.grants
      : Array.isArray(data.results)
        ? data.results
        : [];
    return grants.map(mapGrant);
  } catch {
    return [];
  }
}

export async function fetchRecentGrants(limit = 20): Promise<GrantRecord[]> {
  return searchGrants("*", limit);
}
