export type SupplierEntity = {
  id: string;
  name: string;
  normalised_name: string;
  company_number: string | null;
  company_status: string | null;
  company_type: string | null;
  address: string | null;
  sic_codes: string[];
  website: string | null;
  total_wins: number;
  total_win_value: number;
  first_seen: string;
  last_seen: string;
  updated_at: string;
};

export type SupplierRelationship = {
  id: string;
  supplier_entity_id: string;
  buyer_entity_id: string;
  buyer_name: string;
  notice_id: string;
  title: string;
  category: string | null;
  awarded_value: number | null;
  awarded_date: string | null;
  source: string;
  source_url: string;
};

export type SupplierProfile = {
  entity: SupplierEntity;
  relationships: SupplierRelationship[];
  stats: {
    totalContracts: number;
    totalValue: number;
    avgContractValue: number;
    topBuyers: { name: string; count: number; value: number }[];
    topCategories: { category: string; count: number; value: number }[];
    lastActivity: string | null;
  };
};
