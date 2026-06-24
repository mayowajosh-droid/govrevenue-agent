const BASE = "https://data.bus-data.dft.gov.uk/api/v1";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type BusOperator = {
  id: number;
  name: string;
  nocCode: string;
};

export type BusDataset = {
  id: number;
  operatorName: string;
  name: string;
  description: string | null;
  status: string;
  lastUpdated: string;
};

export async function fetchBusOperators(): Promise<BusOperator[]> {
  const key = process.env.BODS_API_KEY;
  if (!key) return [];
  try {
    const ac = makeAbort();
    const res = await fetch(`${BASE}/operators/`, {
      headers: { Accept: "application/json", Authorization: `Api-Key ${key}` },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data: any = await res.json();
    const items: any[] = Array.isArray(data) ? data : Array.isArray(data.results) ? data.results : [];
    return items.map((op: any) => ({
      id: Number(op.id ?? 0),
      name: String(op.name ?? op.operator_name ?? ""),
      nocCode: String(op.noc ?? op.nocCode ?? ""),
    }));
  } catch {
    return [];
  }
}

export async function fetchBusDatasets(limit = 25): Promise<BusDataset[]> {
  const key = process.env.BODS_API_KEY;
  if (!key) return [];
  try {
    const ac = makeAbort();
    const res = await fetch(`${BASE}/dataset/?limit=${limit}`, {
      headers: { Accept: "application/json", Authorization: `Api-Key ${key}` },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data: any = await res.json();
    const items: any[] = Array.isArray(data) ? data : Array.isArray(data.results) ? data.results : [];
    return items.map((ds: any) => ({
      id: Number(ds.id ?? 0),
      operatorName: String(ds.operatorName ?? ds.operator_name ?? ""),
      name: String(ds.name ?? ""),
      description: ds.description ? String(ds.description) : null,
      status: String(ds.status ?? ""),
      lastUpdated: String(ds.lastUpdated ?? ds.modified ?? ""),
    }));
  } catch {
    return [];
  }
}
