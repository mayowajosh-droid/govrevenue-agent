export type DataQualityStatus = "pass" | "warn" | "fail" | "unknown";
export type RetentionTier = "raw" | "processed" | "archived";
export type LineageNodeType = "source" | "transform" | "store" | "output";

export type MetadataCatalogEntry = {
  id: string;
  sourceId: string;
  sourceName: string;
  category: string;
  description: string;
  baseUrl: string | null;
  requiresKey: boolean;
  keyEnvVar: string | null;
  isLive: boolean;
  cadence: string;
  lastFetchedAt: string | null;
  lastRecordCount: number;
  totalRecordsAllTime: number;
  avgFetchMs: number | null;
  qualityStatus: DataQualityStatus;
  qualityScore: number;
  retentionTier: RetentionTier;
  retentionDays: number;
  owner: string;
  tags: string[];
  updatedAt: string;
};

export type DataLineageNode = {
  id: string;
  nodeType: LineageNodeType;
  name: string;
  system: string;
  description: string | null;
};

export type DataLineageEdge = {
  id: string;
  fromNodeId: string;
  toNodeId: string;
  transformDescription: string | null;
  recordsPerRun: number | null;
  lagMs: number | null;
};

export type DataDictionaryEntry = {
  id: string;
  tableName: string;
  columnName: string;
  dataType: string;
  description: string;
  businessTerm: string | null;
  exampleValue: string | null;
  isPii: boolean;
  isRequired: boolean;
  sourceSystemField: string | null;
};

export type QualityRule = {
  id: string;
  sourceId: string | null;
  tableName: string | null;
  ruleName: string;
  ruleType: "not_null" | "uniqueness" | "range" | "pattern" | "freshness" | "completeness";
  ruleExpression: string;
  severity: "info" | "warn" | "critical";
  enabled: boolean;
};

export type QualityRuleResult = {
  id: string;
  ruleId: string;
  runAt: string;
  status: DataQualityStatus;
  recordsChecked: number;
  recordsFailed: number;
  failureRate: number;
  sample: string | null;
};

export type AuditLog = {
  id: string;
  action: string;
  actorType: "system" | "user" | "scheduler";
  actorId: string | null;
  targetTable: string | null;
  targetId: string | null;
  detail: Record<string, unknown>;
  createdAt: string;
};
