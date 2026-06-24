const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type NetworkRailAsset = {
  name: string;
  region: string;
  type: string;
  description: string | null;
};

// Network Rail data feeds require registration at datafeeds.networkrail.co.uk.
// This stub checks for a token and returns [] — wire up when credentials are available.
export async function fetchNetworkRailAssets(): Promise<NetworkRailAsset[]> {
  const token = process.env.NETWORK_RAIL_TOKEN;
  if (!token) return [];
  try {
    const ac = makeAbort();
    const res = await fetch("https://datafeeds.networkrail.co.uk/ntrod/SupportingFileAuthenticate?type=CORPUS", {
      headers: { Accept: "application/json", Authorization: `Bearer ${token}` },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data: any = await res.json();
    const items: any[] = Array.isArray(data) ? data : Array.isArray(data.TIPLOCDATA) ? data.TIPLOCDATA : [];
    return items.map((a: any) => ({
      name: String(a.TIPLOC ?? a.name ?? ""),
      region: String(a.STANOX ?? a.region ?? ""),
      type: String(a.type ?? "TIPLOC"),
      description: a.NLCDESC ? String(a.NLCDESC) : null,
    }));
  } catch {
    return [];
  }
}
