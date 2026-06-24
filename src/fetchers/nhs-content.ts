const BASE = "https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type NhsOrganisation = {
  orgId: string;
  name: string;
  status: string;
  orgRecordClass: string;
  postCode: string | null;
  lastChangeDate: string | null;
  primaryRoleId: string | null;
  primaryRoleDescription: string | null;
};

function mapOrg(r: any): NhsOrganisation {
  return {
    orgId: String(r.OrgId ?? r.orgId ?? ""),
    name: String(r.Name ?? r.name ?? ""),
    status: String(r.Status ?? r.status ?? ""),
    orgRecordClass: String(r.OrgRecordClass ?? r.orgRecordClass ?? ""),
    postCode: r.PostCode ?? r.postCode ?? null,
    lastChangeDate: r.LastChangeDate ?? r.lastChangeDate ?? null,
    primaryRoleId: r.PrimaryRoleId ?? r.primaryRoleId ?? null,
    primaryRoleDescription: r.PrimaryRoleDescription ?? r.primaryRoleDescription ?? null,
  };
}

export async function searchNhsOrganisations(name: string): Promise<NhsOrganisation[]> {
  try {
    const ac = makeAbort();
    const res = await fetch(
      `${BASE}?Name=${encodeURIComponent(name)}&Limit=20`,
      { headers: { Accept: "application/json" }, signal: ac.signal },
    );
    if (!res.ok) return [];
    const data: any = await res.json();
    const items: any[] = Array.isArray(data.Organisations) ? data.Organisations : [];
    return items.map(mapOrg);
  } catch {
    return [];
  }
}

export async function getNhsOrganisation(orgId: string): Promise<NhsOrganisation | null> {
  try {
    const ac = makeAbort();
    const res = await fetch(`${BASE}/${encodeURIComponent(orgId)}`, {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return null;
    const data: any = await res.json();
    const org = data.Organisation ?? data;
    if (!org) return null;
    return mapOrg(org);
  } catch {
    return null;
  }
}
