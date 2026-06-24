import { pool } from "../../config.js";

export async function initGeospatialTables(): Promise<void> {
  if (!pool) return;

  // Enable PostGIS if available — gracefully skip if not installed on this Postgres instance
  try {
    await pool.query(`CREATE EXTENSION IF NOT EXISTS postgis`);
    await pool.query(`CREATE EXTENSION IF NOT EXISTS postgis_topology`);
    console.log("[geo] PostGIS extensions enabled");
  } catch (err: any) {
    console.warn("[geo] PostGIS not available — falling back to lat/lon columns:", err?.message?.split("\n")[0]);
    await initFallbackTables();
    return;
  }

  await pool.query(`
    CREATE TABLE IF NOT EXISTS spatial_locations (
      id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      entity_id   TEXT,
      entity_type TEXT NOT NULL DEFAULT 'organisation',
      name        TEXT NOT NULL,
      address     TEXT,
      postcode    TEXT,
      district    TEXT,
      county      TEXT,
      region      TEXT,
      country     TEXT NOT NULL DEFAULT 'England',
      lat         DOUBLE PRECISION,
      lon         DOUBLE PRECISION,
      geom        GEOMETRY(POINT, 4326),
      source      TEXT NOT NULL,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_spatial_locations_entity   ON spatial_locations (entity_id);
    CREATE INDEX IF NOT EXISTS idx_spatial_locations_postcode ON spatial_locations (postcode);
    CREATE INDEX IF NOT EXISTS idx_spatial_locations_district ON spatial_locations (district);
  `);

  // PostGIS spatial index
  try {
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_spatial_locations_geom
        ON spatial_locations USING GIST (geom)
    `);
  } catch {}

  await pool.query(`
    CREATE TABLE IF NOT EXISTS spatial_boundaries (
      id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      boundary_type TEXT NOT NULL,
      code        TEXT NOT NULL,
      name        TEXT NOT NULL,
      geom        GEOMETRY(MULTIPOLYGON, 4326),
      properties  JSONB NOT NULL DEFAULT '{}',
      source      TEXT NOT NULL DEFAULT 'ons',
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(boundary_type, code)
    );
    CREATE INDEX IF NOT EXISTS idx_spatial_boundaries_type ON spatial_boundaries (boundary_type);
  `);

  try {
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_spatial_boundaries_geom
        ON spatial_boundaries USING GIST (geom)
    `);
  } catch {}

  await pool.query(`
    CREATE TABLE IF NOT EXISTS planning_applications (
      id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      reference       TEXT,
      description     TEXT,
      status          TEXT,
      decision        TEXT,
      application_type TEXT,
      applicant_name  TEXT,
      address         TEXT,
      postcode        TEXT,
      local_authority TEXT,
      lat             DOUBLE PRECISION,
      lon             DOUBLE PRECISION,
      geom            GEOMETRY(POINT, 4326),
      received_date   DATE,
      decided_date    DATE,
      estimated_value BIGINT,
      source          TEXT NOT NULL DEFAULT 'planning_data',
      raw             JSONB,
      created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_planning_apps_la     ON planning_applications (local_authority);
    CREATE INDEX IF NOT EXISTS idx_planning_apps_status ON planning_applications (status);
    CREATE INDEX IF NOT EXISTS idx_planning_apps_date   ON planning_applications (received_date DESC NULLS LAST);
  `);

  try {
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_planning_apps_geom
        ON planning_applications USING GIST (geom)
    `);
  } catch {}

  await pool.query(`
    CREATE TABLE IF NOT EXISTS environmental_zones (
      id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      zone_type   TEXT NOT NULL,
      name        TEXT,
      risk_level  TEXT,
      geom        GEOMETRY(MULTIPOLYGON, 4326),
      properties  JSONB NOT NULL DEFAULT '{}',
      source      TEXT NOT NULL DEFAULT 'environment_agency',
      fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_env_zones_type ON environmental_zones (zone_type);
  `);

  try {
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_env_zones_geom
        ON environmental_zones USING GIST (geom)
    `);
  } catch {}

  console.log("[geo] Geospatial tables ready");
}

async function initFallbackTables(): Promise<void> {
  if (!pool) return;

  await pool.query(`
    CREATE TABLE IF NOT EXISTS spatial_locations (
      id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      entity_id   TEXT,
      entity_type TEXT NOT NULL DEFAULT 'organisation',
      name        TEXT NOT NULL,
      address     TEXT,
      postcode    TEXT,
      district    TEXT,
      county      TEXT,
      region      TEXT,
      country     TEXT NOT NULL DEFAULT 'England',
      lat         DOUBLE PRECISION,
      lon         DOUBLE PRECISION,
      source      TEXT NOT NULL,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_spatial_locations_entity   ON spatial_locations (entity_id);
    CREATE INDEX IF NOT EXISTS idx_spatial_locations_postcode ON spatial_locations (postcode);
    CREATE INDEX IF NOT EXISTS idx_spatial_locations_district ON spatial_locations (district);

    CREATE TABLE IF NOT EXISTS planning_applications (
      id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      reference       TEXT,
      description     TEXT,
      status          TEXT,
      decision        TEXT,
      application_type TEXT,
      applicant_name  TEXT,
      address         TEXT,
      postcode        TEXT,
      local_authority TEXT,
      lat             DOUBLE PRECISION,
      lon             DOUBLE PRECISION,
      received_date   DATE,
      decided_date    DATE,
      estimated_value BIGINT,
      source          TEXT NOT NULL DEFAULT 'planning_data',
      raw             JSONB,
      created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_planning_apps_la     ON planning_applications (local_authority);
    CREATE INDEX IF NOT EXISTS idx_planning_apps_status ON planning_applications (status);
    CREATE INDEX IF NOT EXISTS idx_planning_apps_date   ON planning_applications (received_date DESC NULLS LAST);
  `);

  console.log("[geo] Fallback (no-PostGIS) geospatial tables ready");
}

export type SpatialLocation = {
  id: string;
  entity_id: string | null;
  entity_type: string;
  name: string;
  address: string | null;
  postcode: string | null;
  district: string | null;
  county: string | null;
  region: string | null;
  country: string;
  lat: number | null;
  lon: number | null;
  source: string;
};

export type PlanningApplication = {
  id: string;
  reference: string | null;
  description: string | null;
  status: string | null;
  decision: string | null;
  application_type: string | null;
  applicant_name: string | null;
  address: string | null;
  postcode: string | null;
  local_authority: string | null;
  lat: number | null;
  lon: number | null;
  received_date: string | null;
  decided_date: string | null;
  estimated_value: number | null;
  source: string;
};

export async function upsertSpatialLocation(loc: Omit<SpatialLocation, "id">): Promise<void> {
  if (!pool) return;

  const hasGeom = loc.lat != null && loc.lon != null;

  try {
    // Try with PostGIS geom column
    if (hasGeom) {
      await pool.query(
        `INSERT INTO spatial_locations (entity_id, entity_type, name, address, postcode, district, county, region, country, lat, lon, geom, source)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, ST_SetSRID(ST_MakePoint($11, $10), 4326), $12)
         ON CONFLICT DO NOTHING`,
        [loc.entity_id, loc.entity_type, loc.name, loc.address, loc.postcode,
         loc.district, loc.county, loc.region, loc.country, loc.lat, loc.lon, loc.source]
      );
    } else {
      await pool.query(
        `INSERT INTO spatial_locations (entity_id, entity_type, name, address, postcode, district, county, region, country, lat, lon, source)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
         ON CONFLICT DO NOTHING`,
        [loc.entity_id, loc.entity_type, loc.name, loc.address, loc.postcode,
         loc.district, loc.county, loc.region, loc.country, loc.lat, loc.lon, loc.source]
      );
    }
  } catch {
    // Fallback: no geom column
    await pool.query(
      `INSERT INTO spatial_locations (entity_id, entity_type, name, address, postcode, district, county, region, country, lat, lon, source)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
       ON CONFLICT DO NOTHING`,
      [loc.entity_id, loc.entity_type, loc.name, loc.address, loc.postcode,
       loc.district, loc.county, loc.region, loc.country, loc.lat, loc.lon, loc.source]
    );
  }
}

export async function upsertPlanningApplication(app: Omit<PlanningApplication, "id">): Promise<void> {
  if (!pool) return;

  const hasGeom = app.lat != null && app.lon != null;

  try {
    if (hasGeom) {
      await pool.query(
        `INSERT INTO planning_applications (reference, description, status, decision, application_type, applicant_name, address, postcode, local_authority, lat, lon, geom, received_date, decided_date, estimated_value, source)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, ST_SetSRID(ST_MakePoint($11, $10), 4326), $12,$13,$14,$15)
         ON CONFLICT DO NOTHING`,
        [app.reference, app.description, app.status, app.decision, app.application_type,
         app.applicant_name, app.address, app.postcode, app.local_authority,
         app.lat, app.lon, app.received_date, app.decided_date, app.estimated_value, app.source]
      );
    } else {
      await pool.query(
        `INSERT INTO planning_applications (reference, description, status, decision, application_type, applicant_name, address, postcode, local_authority, lat, lon, received_date, decided_date, estimated_value, source)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
         ON CONFLICT DO NOTHING`,
        [app.reference, app.description, app.status, app.decision, app.application_type,
         app.applicant_name, app.address, app.postcode, app.local_authority,
         app.lat, app.lon, app.received_date, app.decided_date, app.estimated_value, app.source]
      );
    }
  } catch {
    await pool.query(
      `INSERT INTO planning_applications (reference, description, status, decision, application_type, applicant_name, address, postcode, local_authority, lat, lon, received_date, decided_date, estimated_value, source)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
       ON CONFLICT DO NOTHING`,
      [app.reference, app.description, app.status, app.decision, app.application_type,
       app.applicant_name, app.address, app.postcode, app.local_authority,
       app.lat, app.lon, app.received_date, app.decided_date, app.estimated_value, app.source]
    );
  }
}

export async function findLocationsNear(lat: number, lon: number, radiusKm: number, limit = 20): Promise<SpatialLocation[]> {
  if (!pool) return [];

  try {
    // Try PostGIS ST_DWithin first
    const r = await pool.query<SpatialLocation>(
      `SELECT id, entity_id, entity_type, name, address, postcode, district, county, region, country, lat, lon, source
       FROM spatial_locations
       WHERE ST_DWithin(geom, ST_SetSRID(ST_MakePoint($2, $1), 4326)::geography, $3 * 1000)
       LIMIT $4`,
      [lat, lon, radiusKm, limit]
    );
    return r.rows;
  } catch {
    // Fallback: bounding box approximation (~1 degree lat ≈ 111km)
    const degLat = radiusKm / 111;
    const degLon = radiusKm / (111 * Math.cos(lat * Math.PI / 180));
    const r = await pool.query<SpatialLocation>(
      `SELECT id, entity_id, entity_type, name, address, postcode, district, county, region, country, lat, lon, source
       FROM spatial_locations
       WHERE lat BETWEEN $1 AND $2 AND lon BETWEEN $3 AND $4
       LIMIT $5`,
      [lat - degLat, lat + degLat, lon - degLon, lon + degLon, limit]
    );
    return r.rows;
  }
}

export async function findPlanningApplicationsInDistrict(district: string, limit = 50): Promise<PlanningApplication[]> {
  if (!pool) return [];
  const r = await pool.query<PlanningApplication>(
    `SELECT id, reference, description, status, decision, application_type, applicant_name,
            address, postcode, local_authority, lat, lon, received_date, decided_date, estimated_value, source
     FROM planning_applications
     WHERE local_authority ILIKE $1
     ORDER BY received_date DESC NULLS LAST
     LIMIT $2`,
    [`%${district}%`, limit]
  );
  return r.rows;
}

export async function getGeospatialStats(): Promise<{
  totalLocations: number;
  totalPlanningApps: number;
  byDistrict: { district: string; count: number }[];
}> {
  if (!pool) return { totalLocations: 0, totalPlanningApps: 0, byDistrict: [] };

  const [loc, plan, dist] = await Promise.all([
    pool.query<{ count: string }>(`SELECT COUNT(*) as count FROM spatial_locations`),
    pool.query<{ count: string }>(`SELECT COUNT(*) as count FROM planning_applications`),
    pool.query<{ district: string; count: string }>(
      `SELECT local_authority as district, COUNT(*) as count FROM planning_applications
       WHERE local_authority IS NOT NULL GROUP BY local_authority ORDER BY count DESC LIMIT 20`
    ),
  ]);

  return {
    totalLocations: parseInt(loc.rows[0]?.count ?? "0"),
    totalPlanningApps: parseInt(plan.rows[0]?.count ?? "0"),
    byDistrict: dist.rows.map(r => ({ district: r.district, count: parseInt(r.count) })),
  };
}
