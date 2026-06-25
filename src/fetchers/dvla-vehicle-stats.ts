const GOVUK_API = "https://www.gov.uk/api/content";
const TIMEOUT_MS = 20_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type DvlaVehicleStats = {
  fetchedAt: string;
  referenceYear: string;
  totalLicensedVehicles: number;
  totalLicensedVehiclesMillions: number;
  newFirstRegistrations: number;
  zeroEmissionRegistrations: number;
  zeroEmissionOnRoad: number;
  newFirstRegistrationsChangePercent: number;
  zeroEmissionChangePercent: number;
  licensedVehiclesChangePercent: number;
  sourceUrl: string;
  headline: string;
};

function extractNumber(text: string, pattern: RegExp): number {
  const m = text.match(pattern);
  if (!m) return 0;
  return parseFloat(m[1].replace(/,/g, ""));
}

function extractPercent(text: string, afterPhrase: string): number {
  const idx = text.indexOf(afterPhrase);
  if (idx < 0) return 0;
  const nearby = text.slice(idx, idx + 80);
  const m = nearby.match(/increase of (\d+)%|decrease of (\d+)%/i);
  if (!m) return 0;
  return m[1] ? +m[1] : -(+(m[2] ?? 0));
}

async function fetchStatsPage(year: number): Promise<string | null> {
  try {
    const ac = makeAbort();
    const res = await fetch(
      `${GOVUK_API}/government/statistics/vehicle-licensing-statistics-${year}`,
      { signal: ac.signal },
    );
    if (!res.ok) return null;
    const data = await res.json() as { details?: { body?: string } };
    return data.details?.body ?? null;
  } catch { return null; }
}

function stripHtml(html: string): string {
  return html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
}

/** DVLA vehicle licensing statistics — headline figures from GOV.UK publication. No auth needed. */
export async function fetchDvlaVehicleStats(): Promise<DvlaVehicleStats> {
  const empty: DvlaVehicleStats = {
    fetchedAt: new Date().toISOString(),
    referenceYear: "",
    totalLicensedVehicles: 0,
    totalLicensedVehiclesMillions: 0,
    newFirstRegistrations: 0,
    zeroEmissionRegistrations: 0,
    zeroEmissionOnRoad: 0,
    newFirstRegistrationsChangePercent: 0,
    zeroEmissionChangePercent: 0,
    licensedVehiclesChangePercent: 0,
    sourceUrl: "",
    headline: "",
  };

  // Try current year and fall back to previous year
  const currentYear = new Date().getFullYear();
  let rawHtml: string | null = null;
  let foundYear = currentYear;

  for (const year of [currentYear, currentYear - 1, currentYear - 2]) {
    rawHtml = await fetchStatsPage(year);
    if (rawHtml) { foundYear = year; break; }
  }

  if (!rawHtml) return empty;
  const text = stripHtml(rawHtml);

  const totalM = extractNumber(text, /([\d.]+)\s+million\s+licensed\s+vehicles/i);
  const newReg = extractNumber(text, /([\d,]+)\s+vehicles\s+registered\s+for\s+the\s+first\s+time/i);
  const zeroReg = extractNumber(text, /([\d,]+)\s+zero\s+emission\s+vehicles\s+registered\s+for\s+the\s+first\s+time/i);
  const zeroOnRoad = extractNumber(text, /([\d,]+)\s+licensed\s+zero\s+emission\s+vehicles\s+on\s+the\s+road/i);

  const licPct = extractPercent(text, "licensed vehicles");
  const regPct = extractPercent(text, "registered for the first time");
  const evPct = extractPercent(text, "zero emission vehicles registered for the first time");

  const headline = [
    totalM ? `${totalM}M licensed vehicles in UK` : "",
    newReg ? `${(newReg / 1000).toFixed(0)}k new registrations` : "",
    zeroReg ? `${(zeroReg / 1000).toFixed(0)}k zero-emission new regs` : "",
    zeroOnRoad ? `${(zeroOnRoad / 1000).toFixed(0)}k EVs on road` : "",
  ].filter(Boolean).join(" · ");

  return {
    fetchedAt: new Date().toISOString(),
    referenceYear: String(foundYear),
    totalLicensedVehicles: Math.round(totalM * 1_000_000),
    totalLicensedVehiclesMillions: totalM,
    newFirstRegistrations: newReg,
    zeroEmissionRegistrations: zeroReg,
    zeroEmissionOnRoad: zeroOnRoad,
    newFirstRegistrationsChangePercent: regPct,
    zeroEmissionChangePercent: evPct,
    licensedVehiclesChangePercent: licPct,
    sourceUrl: `https://www.gov.uk/government/statistics/vehicle-licensing-statistics-${foundYear}`,
    headline,
  };
}
