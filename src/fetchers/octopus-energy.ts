const BASE = "https://api.octopus.energy/v1";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type EnergyProduct = {
  code: string;
  name: string;
  description: string | null;
  brand: string;
  isVariable: boolean;
  isGreen: boolean;
  isTracker: boolean;
  availableFrom: string;
  availableTo: string | null;
};

export type EnergyTariff = {
  validFrom: string;
  validTo: string | null;
  valueIncVat: number;
  valueExcVat: number;
};

export async function fetchEnergyProducts(filters?: {
  isVariable?: boolean;
  isGreen?: boolean;
  isTracker?: boolean;
}): Promise<EnergyProduct[]> {
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({ page_size: "100" });
    if (filters?.isVariable != null) params.set("is_variable", String(filters.isVariable));
    if (filters?.isGreen != null) params.set("is_green", String(filters.isGreen));
    if (filters?.isTracker != null) params.set("is_tracker", String(filters.isTracker));
    const res = await fetch(`${BASE}/products/?${params}`, {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data = await res.json() as { results?: {
      code?: string; full_name?: string; description?: string; brand?: string;
      is_variable?: boolean; is_green?: boolean; is_tracker?: boolean;
      available_from?: string; available_to?: string | null;
    }[] };
    return (data.results ?? []).map(p => ({
      code: String(p.code || ""),
      name: String(p.full_name || ""),
      description: p.description ?? null,
      brand: String(p.brand || ""),
      isVariable: p.is_variable ?? false,
      isGreen: p.is_green ?? false,
      isTracker: p.is_tracker ?? false,
      availableFrom: String(p.available_from || ""),
      availableTo: p.available_to ?? null,
    }));
  } catch { return []; }
}

export async function fetchGreenEnergyProducts(): Promise<EnergyProduct[]> {
  return fetchEnergyProducts({ isGreen: true });
}

export async function fetchElectricityTariffs(productCode: string, tariffCode: string, limit = 48): Promise<EnergyTariff[]> {
  try {
    const ac = makeAbort();
    const url = `${BASE}/products/${productCode}/electricity-tariffs/${tariffCode}/standard-unit-rates/?page_size=${limit}`;
    const res = await fetch(url, { headers: { Accept: "application/json" }, signal: ac.signal });
    if (!res.ok) return [];
    const data = await res.json() as { results?: {
      valid_from?: string; valid_to?: string | null;
      value_inc_vat?: number; value_exc_vat?: number;
    }[] };
    return (data.results ?? []).map(t => ({
      validFrom: String(t.valid_from || ""),
      validTo: t.valid_to ?? null,
      valueIncVat: t.value_inc_vat ?? 0,
      valueExcVat: t.value_exc_vat ?? 0,
    }));
  } catch { return []; }
}
