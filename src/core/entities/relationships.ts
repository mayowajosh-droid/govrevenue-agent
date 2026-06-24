import { Pool } from "pg";

export async function initRelationshipTables(pool: Pool): Promise<void> {
  // Alias & Cross-References
  await pool.query(`
    CREATE TABLE IF NOT EXISTS entity_aliases (
      id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      organisation_id UUID NOT NULL REFERENCES canonical_organisations(id) ON DELETE CASCADE,
      alias           TEXT NOT NULL,
      source          TEXT NOT NULL,
      confidence      DOUBLE PRECISION NOT NULL DEFAULT 0.8,
      created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_aliases_unique ON entity_aliases (organisation_id, alias);
    CREATE INDEX IF NOT EXISTS idx_entity_aliases_alias ON entity_aliases (alias);
  `);

  // Source cross-references (links an entity to its IDs in external systems)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS source_references (
      id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      organisation_id UUID NOT NULL REFERENCES canonical_organisations(id) ON DELETE CASCADE,
      source_system   TEXT NOT NULL,
      source_id       TEXT NOT NULL,
      confidence      DOUBLE PRECISION NOT NULL DEFAULT 1.0,
      verified        BOOLEAN NOT NULL DEFAULT FALSE,
      created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(source_system, source_id)
    );
    CREATE INDEX IF NOT EXISTS idx_source_refs_org ON source_references (organisation_id);
    CREATE INDEX IF NOT EXISTS idx_source_refs_system ON source_references (source_system);
  `);

  // Relationship Graph — parent/subsidiary, ownership
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ownership_links (
      id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      parent_id       UUID NOT NULL REFERENCES canonical_organisations(id) ON DELETE CASCADE,
      child_id        UUID NOT NULL REFERENCES canonical_organisations(id) ON DELETE CASCADE,
      relationship    TEXT NOT NULL DEFAULT 'subsidiary',
      ownership_pct   DOUBLE PRECISION,
      source          TEXT NOT NULL DEFAULT 'companies_house',
      verified        BOOLEAN NOT NULL DEFAULT FALSE,
      created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(parent_id, child_id, relationship)
    );
    CREATE INDEX IF NOT EXISTS idx_ownership_parent ON ownership_links (parent_id);
    CREATE INDEX IF NOT EXISTS idx_ownership_child  ON ownership_links (child_id);
  `);

  // Director/officer roles spanning multiple organisations (shared director detection)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS director_roles (
      id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      person_name     TEXT NOT NULL,
      organisation_id UUID NOT NULL REFERENCES canonical_organisations(id) ON DELETE CASCADE,
      role            TEXT NOT NULL,
      appointed_on    DATE,
      resigned_on     DATE,
      ch_officer_id   TEXT,
      source          TEXT NOT NULL DEFAULT 'companies_house',
      created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(ch_officer_id, organisation_id)
    );
    CREATE INDEX IF NOT EXISTS idx_director_roles_person ON director_roles (person_name);
    CREATE INDEX IF NOT EXISTS idx_director_roles_org    ON director_roles (organisation_id);
    CREATE INDEX IF NOT EXISTS idx_director_roles_ch     ON director_roles (ch_officer_id) WHERE ch_officer_id IS NOT NULL;
  `);

  console.log("[entity-resolution] relationship tables ready");
}

export type SharedDirector = {
  personName: string;
  chOfficerId: string | null;
  organisations: { id: string; name: string; role: string }[];
};

export async function findSharedDirectors(pool: Pool, minOrgs = 2): Promise<SharedDirector[]> {
  const r = await pool.query<{
    person_name: string;
    ch_officer_id: string | null;
    org_ids: string[];
    org_names: string[];
    roles: string[];
  }>(
    `SELECT
       dr.person_name,
       dr.ch_officer_id,
       ARRAY_AGG(DISTINCT co.id) AS org_ids,
       ARRAY_AGG(DISTINCT co.primary_name) AS org_names,
       ARRAY_AGG(DISTINCT dr.role) AS roles
     FROM director_roles dr
     JOIN canonical_organisations co ON co.id = dr.organisation_id
     WHERE dr.resigned_on IS NULL
     GROUP BY dr.person_name, dr.ch_officer_id
     HAVING COUNT(DISTINCT dr.organisation_id) >= $1
     ORDER BY COUNT(DISTINCT dr.organisation_id) DESC
     LIMIT 200`,
    [minOrgs]
  );

  return r.rows.map(row => ({
    personName: row.person_name,
    chOfficerId: row.ch_officer_id,
    organisations: (row.org_ids || []).map((id, i) => ({
      id,
      name: (row.org_names || [])[i] ?? "",
      role: (row.roles || [])[0] ?? "",
    })),
  }));
}

export async function upsertAlias(pool: Pool, organisationId: string, alias: string, source: string, confidence = 0.8): Promise<void> {
  await pool.query(
    `INSERT INTO entity_aliases (organisation_id, alias, source, confidence)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (organisation_id, alias) DO UPDATE SET confidence = GREATEST(entity_aliases.confidence, EXCLUDED.confidence)`,
    [organisationId, alias.trim(), source, confidence]
  );
}

export async function upsertSourceReference(
  pool: Pool,
  organisationId: string,
  sourceSystem: string,
  sourceId: string,
  verified = false
): Promise<void> {
  await pool.query(
    `INSERT INTO source_references (organisation_id, source_system, source_id, verified)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (source_system, source_id) DO UPDATE SET
       organisation_id = EXCLUDED.organisation_id,
       verified = EXCLUDED.verified OR source_references.verified`,
    [organisationId, sourceSystem, sourceId, verified]
  );
}

export async function upsertOwnershipLink(
  pool: Pool,
  parentId: string,
  childId: string,
  relationship: string,
  ownershipPct?: number,
  source = "companies_house"
): Promise<void> {
  await pool.query(
    `INSERT INTO ownership_links (parent_id, child_id, relationship, ownership_pct, source)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (parent_id, child_id, relationship) DO UPDATE SET
       ownership_pct = COALESCE(EXCLUDED.ownership_pct, ownership_links.ownership_pct),
       verified = TRUE`,
    [parentId, childId, relationship, ownershipPct ?? null, source]
  );
}

export async function upsertDirectorRole(
  pool: Pool,
  opts: {
    personName: string;
    organisationId: string;
    role: string;
    appointedOn?: string | null;
    resignedOn?: string | null;
    chOfficerId?: string | null;
    source?: string;
  }
): Promise<void> {
  const safeKey = opts.chOfficerId
    ? opts.chOfficerId
    : null;

  if (safeKey) {
    await pool.query(
      `INSERT INTO director_roles (person_name, organisation_id, role, appointed_on, resigned_on, ch_officer_id, source)
       VALUES ($1,$2,$3,$4,$5,$6,$7)
       ON CONFLICT (ch_officer_id, organisation_id) DO UPDATE SET
         resigned_on = EXCLUDED.resigned_on,
         role = EXCLUDED.role`,
      [opts.personName, opts.organisationId, opts.role, opts.appointedOn ?? null,
       opts.resignedOn ?? null, safeKey, opts.source ?? "companies_house"]
    );
  } else {
    // No CH officer ID — just insert, allow duplicates only by name+org
    await pool.query(
      `INSERT INTO director_roles (person_name, organisation_id, role, appointed_on, resigned_on, source)
       VALUES ($1,$2,$3,$4,$5,$6)
       ON CONFLICT DO NOTHING`,
      [opts.personName, opts.organisationId, opts.role, opts.appointedOn ?? null,
       opts.resignedOn ?? null, opts.source ?? "companies_house"]
    );
  }
}

export async function getOrganisationRelationships(
  pool: Pool,
  organisationId: string
): Promise<{
  aliases: { alias: string; source: string; confidence: number }[];
  sourceRefs: { system: string; id: string; verified: boolean }[];
  subsidiaries: { id: string; name: string; relationship: string }[];
  parents: { id: string; name: string; relationship: string }[];
  directors: { name: string; role: string; chOfficerId: string | null }[];
}> {
  const [aliases, refs, subs, parents, dirs] = await Promise.all([
    pool.query<{ alias: string; source: string; confidence: number }>(
      `SELECT alias, source, confidence FROM entity_aliases WHERE organisation_id = $1`,
      [organisationId]
    ),
    pool.query<{ source_system: string; source_id: string; verified: boolean }>(
      `SELECT source_system, source_id, verified FROM source_references WHERE organisation_id = $1`,
      [organisationId]
    ),
    pool.query<{ id: string; primary_name: string; relationship: string }>(
      `SELECT co.id, co.primary_name, ol.relationship
       FROM ownership_links ol JOIN canonical_organisations co ON co.id = ol.child_id
       WHERE ol.parent_id = $1`,
      [organisationId]
    ),
    pool.query<{ id: string; primary_name: string; relationship: string }>(
      `SELECT co.id, co.primary_name, ol.relationship
       FROM ownership_links ol JOIN canonical_organisations co ON co.id = ol.parent_id
       WHERE ol.child_id = $1`,
      [organisationId]
    ),
    pool.query<{ person_name: string; role: string; ch_officer_id: string | null }>(
      `SELECT person_name, role, ch_officer_id FROM director_roles
       WHERE organisation_id = $1 AND resigned_on IS NULL`,
      [organisationId]
    ),
  ]);

  return {
    aliases: aliases.rows,
    sourceRefs: refs.rows.map(r => ({ system: r.source_system, id: r.source_id, verified: r.verified })),
    subsidiaries: subs.rows.map(r => ({ id: r.id, name: r.primary_name, relationship: r.relationship })),
    parents: parents.rows.map(r => ({ id: r.id, name: r.primary_name, relationship: r.relationship })),
    directors: dirs.rows.map(r => ({ name: r.person_name, role: r.role, chOfficerId: r.ch_officer_id })),
  };
}
