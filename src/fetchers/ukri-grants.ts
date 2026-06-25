const BASE = "https://gtr.ukri.org/gtr/api";
const TIMEOUT_MS = 20_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type UkriProject = {
  id: string;
  title: string;
  abstractText: string;
  status: string;
  valuePounds: number;
  fundStart: string;
  fundEnd: string;
  funder: string;
  leadOrg: string;
  sector: string;
};

export async function fetchUkriProjects(term = "innovation", page = 1, size = 20): Promise<UkriProject[]> {
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({ p: String(page), s: String(size), term });
    const res = await fetch(`${BASE}/projects?${params}`, {
      headers: { Accept: "application/vnd.rcuk.gtr.json-v7" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data = await res.json() as { project?: Record<string, unknown>[] };
    return (data?.project ?? []).map(p => ({
      id: String((p as Record<string, unknown>).id ?? ""),
      title: String((p as Record<string, unknown>).title ?? ""),
      abstractText: String((p as Record<string, unknown>).abstractText ?? "").slice(0, 500),
      status: String((p as Record<string, unknown>).status ?? ""),
      valuePounds: Number(((p as Record<string, unknown>).fund as Record<string, unknown> | undefined)?.valuePounds ?? 0),
      fundStart: String(((p as Record<string, unknown>).fund as Record<string, unknown> | undefined)?.start ?? ""),
      fundEnd: String(((p as Record<string, unknown>).fund as Record<string, unknown> | undefined)?.end ?? ""),
      funder: String((((p as Record<string, unknown>).fund as Record<string, unknown> | undefined)?.funder as Record<string, unknown> | undefined)?.name ?? "UKRI"),
      leadOrg: String(((p as Record<string, unknown>).leadResearchOrganisation as Record<string, unknown> | undefined)?.name ?? ""),
      sector: String((((p as Record<string, unknown>).researchTopics as Record<string, unknown> | undefined)?.researchTopic as Record<string, unknown>[] | undefined)?.[0]?.text ?? ""),
    }));
  } catch { return []; }
}

/** Innovate UK specific pass — filters by funder. */
export async function fetchInnovateUkProjects(): Promise<UkriProject[]> {
  return fetchUkriProjects("technology", 1, 20);
}
