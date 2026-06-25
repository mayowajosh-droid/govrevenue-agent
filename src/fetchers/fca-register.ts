const BASE = "https://register.fca.org.uk/services/V0.1";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type FcaFirm = {
  frn: string;
  name: string;
  status: string;
  businessType: string;
  authorisedFrom: string | null;
};

export type FcaIndividual = {
  irn: string;
  name: string;
  status: string;
};

/** Search FCA Register for firms. Requires FCA_API_EMAIL + FCA_API_KEY env vars (free registration). */
export async function searchFcaFirms(query: string, perPage = 20): Promise<FcaFirm[]> {
  const email = process.env.FCA_API_EMAIL ?? "";
  const key = process.env.FCA_API_KEY ?? "";
  if (!email || !key) return [];
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({ q: query, "per-page": String(perPage), page: "1" });
    const res = await fetch(`${BASE}/Firm?${params}`, {
      headers: {
        "X-AUTH-EMAIL": email,
        "X-AUTH-TOKEN": key,
        Accept: "application/json",
      },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data = await res.json() as { Data?: Record<string, unknown>[] };
    return (data?.Data ?? []).map(f => ({
      frn: String(f["FRN"] ?? ""),
      name: String(f["Organisation_Name"] ?? ""),
      status: String(f["Status"] ?? ""),
      businessType: String(f["Business_Type"] ?? ""),
      authorisedFrom: (f["Authorised_From"] as string | undefined) ?? null,
    }));
  } catch { return []; }
}

/** Fetch a firm by FRN. */
export async function getFcaFirmByFrn(frn: string): Promise<FcaFirm | null> {
  const email = process.env.FCA_API_EMAIL ?? "";
  const key = process.env.FCA_API_KEY ?? "";
  if (!email || !key) return null;
  try {
    const ac = makeAbort();
    const res = await fetch(`${BASE}/Firm/${encodeURIComponent(frn)}`, {
      headers: { "X-AUTH-EMAIL": email, "X-AUTH-TOKEN": key, Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return null;
    const data = await res.json() as { Data?: Record<string, unknown>[] };
    const f = data?.Data?.[0];
    if (!f) return null;
    return {
      frn: String(f["FRN"] ?? ""),
      name: String(f["Organisation_Name"] ?? ""),
      status: String(f["Status"] ?? ""),
      businessType: String(f["Business_Type"] ?? ""),
      authorisedFrom: (f["Authorised_From"] as string | undefined) ?? null,
    };
  } catch { return null; }
}
