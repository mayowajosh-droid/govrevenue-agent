// One-off: pre-rebrand scan reports still say "GovRevenue" in stored text.
// Usage: railway run -- node scripts/rebrand-stored-reports.mjs [--apply]
import pg from "pg";

const conn = process.env.DATABASE_PUBLIC_URL || process.env.DATABASE_URL;
if (!conn) { console.error("No DATABASE_URL in environment"); process.exit(1); }
const apply = process.argv.includes("--apply");
const pool = new pg.Pool({ connectionString: conn, ssl: { rejectUnauthorized: false }, max: 2 });

const targets = [
  ["scans", "report_markdown"],
  ["scans", "capability_statement"],
  ["scans", "outreach_emails"],
  ["scans", "frameworks_assessment"],
  ["articles", "body_md"],
  ["articles", "title"],
  ["comments", "body"],
];

for (const [table, col] of targets) {
  try {
    const { rows } = await pool.query(
      `SELECT COUNT(*) AS n FROM ${table} WHERE ${col} ILIKE '%govrevenue%'`
    );
    const n = parseInt(rows[0].n, 10);
    if (!n) { console.log(`${table}.${col}: clean`); continue; }
    if (!apply) { console.log(`${table}.${col}: ${n} rows contain GovRevenue (dry run)`); continue; }
    const res = await pool.query(
      `UPDATE ${table} SET ${col} =
         REPLACE(REPLACE(REPLACE(${col}, 'GovRevenue', 'AtlasRevenue'), 'govrevenue', 'atlasrevenue'), 'GOVREVENUE', 'ATLASREVENUE')
       WHERE ${col} ILIKE '%govrevenue%'`
    );
    console.log(`${table}.${col}: ${res.rowCount} rows updated`);
  } catch (err) {
    console.log(`${table}.${col}: skipped (${err.message.split("\n")[0]})`);
  }
}
await pool.end();
