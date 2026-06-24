export type { SpatialLocation, PlanningApplication } from "./db.js";
export {
  initGeospatialTables,
  upsertSpatialLocation,
  upsertPlanningApplication,
  findLocationsNear,
  findPlanningApplicationsInDistrict,
  getGeospatialStats,
} from "./db.js";
