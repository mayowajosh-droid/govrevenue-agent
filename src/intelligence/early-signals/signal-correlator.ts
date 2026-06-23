import { DESK_PROFILES } from "../../data/desk-profiles.js";
import type { EarlySignal, OnsDataPoint, LandRegistryTransaction } from "./types.js";

type RawSignalInput = {
  constructionOutput: OnsDataPoint[];
  businessDemography: OnsDataPoint[];
  landRegistryByDistrict: Map<string, LandRegistryTransaction[]>;
};

const DESK_CORRELATION: Record<string, string[]> = {
  construction_output_rising: ["construction", "facilities", "planning"],
  property_transactions_up: ["construction", "housing-support", "planning"],
  business_births_up: ["digital", "legal", "finance"],
};

function significance(changePct: number): "high" | "medium" | "low" {
  const abs = Math.abs(changePct);
  if (abs > 20) return "high";
  if (abs >= 10) return "medium";
  return "low";
}

function matchingDesks(keys: string[]): string[] {
  const slugs = new Set<string>();
  for (const k of keys) {
    const mapped = DESK_CORRELATION[k];
    if (mapped) mapped.forEach((s) => slugs.add(s));
  }
  // filter to only desks that actually exist in DESK_PROFILES
  const validSlugs = new Set(DESK_PROFILES.map((d) => d.slug));
  return [...slugs].filter((s) => validSlugs.has(s));
}

export function generateNarrative(signal: EarlySignal): string {
  const dir = (signal.change_pct ?? 0) >= 0 ? "up" : "down";
  const pct = Math.abs(signal.change_pct ?? 0).toFixed(1);

  switch (signal.indicator) {
    case "construction_output":
      return `Construction output index ${dir} ${pct}% in ${signal.period} — signals ${dir === "up" ? "growing" : "cooling"} capital works pipeline.`;
    case "property_transactions":
      return `Property transactions in ${signal.region} ${dir} ${pct}% — ${dir === "up" ? "increased development activity likely" : "market slowdown"}.`;
    case "business_demography":
      return `Business births ${dir} ${pct}% — ${dir === "up" ? "expanding SME market for public sector services" : "contraction in new business formation"}.`;
    default:
      return `${signal.indicator} ${dir} ${pct}% in ${signal.region} (${signal.period}).`;
  }
}

export function correlateSignals(input: RawSignalInput): EarlySignal[] {
  const signals: EarlySignal[] = [];
  const now = new Date().toISOString();

  // Construction output — compare last two data points
  if (input.constructionOutput.length >= 2) {
    const curr = input.constructionOutput[input.constructionOutput.length - 1]!;
    const prev = input.constructionOutput[input.constructionOutput.length - 2]!;

    if (prev.value !== 0) {
      const changePct = ((curr.value - prev.value) / prev.value) * 100;

      if (Math.abs(changePct) >= 1) {
        const sig: EarlySignal = {
          id: `ons-construction-${curr.date}`,
          source: "ons",
          indicator: "construction_output",
          region: "UK",
          period: curr.date,
          current_value: curr.value,
          previous_value: prev.value,
          change_pct: Math.round(changePct * 10) / 10,
          significance: significance(changePct),
          desk_categories: matchingDesks(
            changePct > 10 ? ["construction_output_rising"] : []
          ),
          narrative: "",
          fetched_at: now,
        };
        sig.narrative = generateNarrative(sig);
        signals.push(sig);
      }
    }
  }

  // Business demography — compare last two years
  if (input.businessDemography.length >= 2) {
    const curr = input.businessDemography[input.businessDemography.length - 1]!;
    const prev = input.businessDemography[input.businessDemography.length - 2]!;

    if (prev.value !== 0) {
      const changePct = ((curr.value - prev.value) / prev.value) * 100;

      if (Math.abs(changePct) >= 1) {
        const sig: EarlySignal = {
          id: `ons-biz-demog-${curr.date}`,
          source: "ons",
          indicator: "business_demography",
          region: "UK",
          period: curr.date,
          current_value: curr.value,
          previous_value: prev.value,
          change_pct: Math.round(changePct * 10) / 10,
          significance: significance(changePct),
          desk_categories: matchingDesks(
            changePct > 0 ? ["business_births_up"] : []
          ),
          narrative: "",
          fetched_at: now,
        };
        sig.narrative = generateNarrative(sig);
        signals.push(sig);
      }
    }
  }

  // Land Registry — per-district transaction volume changes
  for (const [district, txns] of input.landRegistryByDistrict) {
    if (txns.length === 0) continue;

    const totalPrice = txns.reduce((s, t) => s + t.price, 0);
    const avgPrice = Math.round(totalPrice / txns.length);

    const sig: EarlySignal = {
      id: `lr-${district.toLowerCase().replace(/\s+/g, "-")}-${txns.length}`,
      source: "land_registry",
      indicator: "property_transactions",
      region: district,
      period: "last-6m",
      current_value: txns.length,
      previous_value: null,
      change_pct: null,
      significance: txns.length > 30 ? "high" : txns.length > 15 ? "medium" : "low",
      desk_categories: matchingDesks(
        txns.length > 15 ? ["property_transactions_up"] : []
      ),
      narrative: "",
      fetched_at: now,
    };

    // Override narrative for land registry with avg price context
    sig.narrative = `${txns.length} property transactions in ${district} (avg ${"£"}${avgPrice.toLocaleString()}) — ${txns.length > 15 ? "active market signals development opportunity" : "moderate market activity"}.`;
    signals.push(sig);
  }

  return signals;
}
