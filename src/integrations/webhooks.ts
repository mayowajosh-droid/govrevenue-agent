import { Pool } from "pg";

export type WebhookEvent =
  | "signal.new"
  | "signal.high_score"
  | "opportunity.open"
  | "opportunity.closing_soon"
  | "ingest.complete"
  | "quality.fail"
  | "supplier.new";

export type WebhookRecord = {
  id: string;
  url: string;
  secret: string | null;
  events: WebhookEvent[];
  enabled: boolean;
  label: string | null;
  failCount: number;
  lastFiredAt: string | null;
  lastStatusCode: number | null;
  createdAt: string;
};

export async function initWebhookTables(pool: Pool): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS webhooks (
      id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      url             TEXT NOT NULL,
      secret          TEXT,
      events          TEXT[] NOT NULL DEFAULT '{}',
      enabled         BOOLEAN NOT NULL DEFAULT TRUE,
      label           TEXT,
      fail_count      INTEGER NOT NULL DEFAULT 0,
      last_fired_at   TIMESTAMPTZ,
      last_status_code INTEGER,
      created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(url)
    );
    CREATE INDEX IF NOT EXISTS idx_webhooks_enabled ON webhooks (enabled) WHERE enabled = TRUE;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS webhook_deliveries (
      id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      webhook_id  UUID NOT NULL REFERENCES webhooks(id) ON DELETE CASCADE,
      event       TEXT NOT NULL,
      payload     JSONB NOT NULL DEFAULT '{}',
      status_code INTEGER,
      response_ms INTEGER,
      error       TEXT,
      fired_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_wh  ON webhook_deliveries (webhook_id);
    CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_evt ON webhook_deliveries (event);
    CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_ts  ON webhook_deliveries (fired_at DESC);
  `);

  console.log("[webhooks] Webhook tables ready");
}

export async function createWebhook(pool: Pool, opts: {
  url: string;
  secret?: string | null;
  events?: WebhookEvent[];
  label?: string | null;
}): Promise<WebhookRecord> {
  const r = await pool.query<WebhookRecord>(
    `INSERT INTO webhooks (url, secret, events, label)
     VALUES ($1,$2,$3,$4)
     ON CONFLICT (url) DO UPDATE SET
       events = EXCLUDED.events,
       label = EXCLUDED.label,
       enabled = TRUE
     RETURNING id, url, secret, events, enabled, label, fail_count AS "failCount",
               last_fired_at AS "lastFiredAt", last_status_code AS "lastStatusCode",
               created_at AS "createdAt"`,
    [opts.url, opts.secret ?? null, opts.events ?? ["signal.high_score", "opportunity.closing_soon"], opts.label ?? null]
  );
  return r.rows[0]!;
}

export async function listWebhooks(pool: Pool): Promise<WebhookRecord[]> {
  const r = await pool.query<{
    id: string; url: string; secret: string | null; events: string[];
    enabled: boolean; label: string | null; fail_count: number;
    last_fired_at: string | null; last_status_code: number | null; created_at: string;
  }>(`SELECT id, url, secret, events, enabled, label, fail_count, last_fired_at, last_status_code, created_at
      FROM webhooks ORDER BY created_at DESC`);

  return r.rows.map(row => ({
    id: row.id, url: row.url, secret: row.secret,
    events: row.events as WebhookEvent[], enabled: row.enabled, label: row.label,
    failCount: row.fail_count, lastFiredAt: row.last_fired_at,
    lastStatusCode: row.last_status_code, createdAt: row.created_at,
  }));
}

export async function deleteWebhook(pool: Pool, id: string): Promise<void> {
  await pool.query(`DELETE FROM webhooks WHERE id = $1`, [id]);
}

export async function toggleWebhook(pool: Pool, id: string, enabled: boolean): Promise<void> {
  await pool.query(`UPDATE webhooks SET enabled = $1, fail_count = 0 WHERE id = $2`, [enabled, id]);
}

export async function getWebhookDeliveries(pool: Pool, webhookId: string, limit = 50): Promise<{
  id: string; event: string; statusCode: number | null; responseMs: number | null;
  error: string | null; firedAt: string;
}[]> {
  const r = await pool.query<{
    id: string; event: string; status_code: number | null; response_ms: number | null;
    error: string | null; fired_at: string;
  }>(
    `SELECT id, event, status_code, response_ms, error, fired_at
     FROM webhook_deliveries WHERE webhook_id = $1 ORDER BY fired_at DESC LIMIT $2`,
    [webhookId, limit]
  );
  return r.rows.map(row => ({
    id: row.id, event: row.event, statusCode: row.status_code,
    responseMs: row.response_ms, error: row.error, firedAt: row.fired_at,
  }));
}

// ── Fire a webhook event to all matching registered hooks ────────────────────

export async function fireWebhookEvent(
  pool: Pool,
  event: WebhookEvent,
  payload: Record<string, unknown>
): Promise<void> {
  const hooks = await pool.query<{
    id: string; url: string; secret: string | null; events: string[];
  }>(
    `SELECT id, url, secret, events FROM webhooks
     WHERE enabled = TRUE AND fail_count < 5 AND $1 = ANY(events)`,
    [event]
  );

  for (const hook of hooks.rows) {
    const start = Date.now();
    let statusCode: number | null = null;
    let errorMsg: string | null = null;

    try {
      const body = JSON.stringify({ event, timestamp: new Date().toISOString(), data: payload });
      const headers: Record<string, string> = { "Content-Type": "application/json", "User-Agent": "AtlasRevenue-Webhook/1.0" };

      if (hook.secret) {
        const { createHmac } = await import("node:crypto");
        headers["X-AtlasRevenue-Signature"] = "sha256=" + createHmac("sha256", hook.secret).update(body).digest("hex");
      }

      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 10_000);
      try {
        const resp = await fetch(hook.url, { method: "POST", headers, body, signal: ctrl.signal });
        statusCode = resp.status;
      } finally {
        clearTimeout(timer);
      }
    } catch (err: unknown) {
      errorMsg = err instanceof Error ? err.message : String(err);
    }

    const responseMs = Date.now() - start;
    const success = statusCode !== null && statusCode >= 200 && statusCode < 300;

    await pool.query(
      `INSERT INTO webhook_deliveries (webhook_id, event, payload, status_code, response_ms, error)
       VALUES ($1,$2,$3,$4,$5,$6)`,
      [hook.id, event, JSON.stringify(payload), statusCode, responseMs, errorMsg]
    );

    if (success) {
      await pool.query(
        `UPDATE webhooks SET last_fired_at = now(), last_status_code = $1, fail_count = 0 WHERE id = $2`,
        [statusCode, hook.id]
      );
    } else {
      await pool.query(
        `UPDATE webhooks SET last_fired_at = now(), last_status_code = $1, fail_count = fail_count + 1 WHERE id = $2`,
        [statusCode, hook.id]
      );
    }
  }
}
