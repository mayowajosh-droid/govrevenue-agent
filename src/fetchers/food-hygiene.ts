const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type FoodHygieneEstablishment = {
  fhrsId: number;
  businessName: string;
  businessType: string;
  ratingValue: string;
  ratingDate: string | null;
  address: string;
  postcode: string | null;
  localAuthority: string;
  longitude: number | null;
  latitude: number | null;
};

export async function searchFoodEstablishments(
  name: string,
  localAuthority?: string,
): Promise<FoodHygieneEstablishment[]> {
  try {
    const ac = makeAbort();
    const laSegment = localAuthority ? encodeURIComponent(localAuthority) : "0";
    const url = `https://ratings.food.gov.uk/enhanced-search/en-GB/%5E/${encodeURIComponent(name)}/${laSegment}/0/10/0/json`;
    const res = await fetch(url, {
      headers: { Accept: "application/json", "x-api-version": "2" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data: any = await res.json();
    const items: any[] = Array.isArray(data.FHRSEstablishment?.EstablishmentCollection)
      ? data.FHRSEstablishment.EstablishmentCollection
      : Array.isArray(data.establishments)
        ? data.establishments
        : [];
    return items.map((e: any) => ({
      fhrsId: Number(e.FHRSID ?? e.fhrsId ?? 0),
      businessName: String(e.BusinessName ?? e.businessName ?? ""),
      businessType: String(e.BusinessType ?? e.businessType ?? ""),
      ratingValue: String(e.RatingValue ?? e.ratingValue ?? ""),
      ratingDate: e.RatingDate ?? e.ratingDate ?? null,
      address: [e.AddressLine1, e.AddressLine2, e.AddressLine3, e.AddressLine4]
        .filter(Boolean)
        .join(", "),
      postcode: e.PostCode ?? e.postcode ?? null,
      localAuthority: String(e.LocalAuthorityName ?? e.localAuthority ?? ""),
      longitude: e.Longitude != null ? Number(e.Longitude) : null,
      latitude: e.Latitude != null ? Number(e.Latitude) : null,
    }));
  } catch {
    return [];
  }
}
