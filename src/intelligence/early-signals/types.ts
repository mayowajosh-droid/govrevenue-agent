export type EarlySignal = {
  id: string;
  source: "ons" | "land_registry" | "planning";
  indicator: string;
  region: string;
  period: string;
  current_value: number;
  previous_value: number | null;
  change_pct: number | null;
  significance: "high" | "medium" | "low";
  desk_categories: string[];
  narrative: string;
  fetched_at: string;
};

export type OnsDataPoint = {
  date: string;
  value: number;
  label: string;
};

export type LandRegistryTransaction = {
  id: string;
  price: number;
  date: string;
  postcode: string;
  property_type: string;
  district: string;
  county: string;
};
