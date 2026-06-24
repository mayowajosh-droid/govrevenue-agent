const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type EducationEstablishment = {
  urn: string;
  name: string;
  type: string;
  phase: string;
  status: string;
  localAuthority: string;
  address: string | null;
  postcode: string | null;
  website: string | null;
};

// Uses the GIAS suggest endpoint — returns basic school info for a search term.
// The full GIAS API requires complex auth; this lightweight path covers name search.
export async function searchSchools(name: string): Promise<EducationEstablishment[]> {
  try {
    const ac = makeAbort();
    const res = await fetch(
      `https://www.get-information-schools.service.gov.uk/api/suggest?searchterm=${encodeURIComponent(name)}`,
      { headers: { Accept: "application/json" }, signal: ac.signal },
    );
    if (!res.ok) return [];
    const data: any = await res.json();
    const items: any[] = Array.isArray(data) ? data : Array.isArray(data.results) ? data.results : [];
    return items.map((s: any) => ({
      urn: String(s.Urn ?? s.urn ?? s.id ?? ""),
      name: String(s.Text ?? s.Name ?? s.name ?? ""),
      type: String(s.Type ?? s.type ?? ""),
      phase: String(s.Phase ?? s.phase ?? ""),
      status: String(s.Status ?? s.status ?? ""),
      localAuthority: String(s.LocalAuthority ?? s.localAuthority ?? ""),
      address: s.Address ? String(s.Address) : null,
      postcode: s.Postcode ?? s.postcode ?? null,
      website: s.Website ?? s.website ?? null,
    }));
  } catch {
    return [];
  }
}
