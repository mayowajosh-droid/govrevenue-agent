import { EventEmitter } from "events";
import "dotenv/config";
import express from "express";
import Stripe from "stripe";
import * as Sentry from "@sentry/node";
import { Pool } from "pg";
import OpenAI from "openai";
import Anthropic from "@anthropic-ai/sdk";
import { Queue } from "bullmq";
import { Redis } from "ioredis";
import type { ScanRecord, SubscriptionRecord, HomepageSignal, ProcurementData } from "./types.js";

export const PORT = Number(process.env.PORT || 3000);
export const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-5-mini";
export const ANTHROPIC_MODEL = process.env.ANTHROPIC_MODEL || "claude-opus-4-8";
export const ADMIN_TOKEN = process.env.ADMIN_TOKEN || "change-me-now";
export const JWT_SECRET = process.env.JWT_SECRET || "atlasrevenue-jwt-secret-change-in-prod";
export const STRIPE_SECRET_KEY = process.env.STRIPE_SECRET_KEY || "";
export const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET || "";
export const STRIPE_PRO_PRICE_ID = process.env.STRIPE_PRO_PRICE_ID || "";
export const STRIPE_AGENCY_PRICE_ID = process.env.STRIPE_AGENCY_PRICE_ID || "";
export const STRIPE_PAYG_PRICE_ID = process.env.STRIPE_PAYG_PRICE_ID || "";
export const BASE_URL = process.env.BASE_URL || "https://atlasrevenue-agent-production.up.railway.app";
export const REDIS_URL = process.env.REDIS_URL || null;
export const RUN_WEB = process.env.RUN_WEB !== "false";
export const RUN_WORKER = process.env.RUN_WORKER !== "false";
export const SENTRY_DSN = process.env.SENTRY_DSN || "";
export const SENTRY_ENABLED = Boolean(SENTRY_DSN);
export const SLACK_WEBHOOK_URL = process.env.SLACK_WEBHOOK_URL || "";
export const DESK_CACHE_TTL_MS = 24 * 60 * 60 * 1000;

export const stripe = STRIPE_SECRET_KEY ? new Stripe(STRIPE_SECRET_KEY) : null;

if (SENTRY_ENABLED) {
  Sentry.init({
    dsn: SENTRY_DSN,
    environment: process.env.SENTRY_ENVIRONMENT || process.env.NODE_ENV || "development",
    tracesSampleRate: 0
  });
}

export const pool = process.env.DATABASE_URL
  ? new Pool({ connectionString: process.env.DATABASE_URL })
  : null;

export const memoryStore = new Map<string, ScanRecord>();
export const subMemStore = new Map<string, SubscriptionRecord>();
export const sigMemStore = new Map<string, HomepageSignal>();
export const briefMemStore = new Map<string, { id: string; email: string; category: string | null; source: string | null; created_at: string }>();
export const deskCacheMemStore = new Map<string, { data: ProcurementData; cached_at: string }>();
export const compilingDesks = new Set<string>();
export const scanEvents = new EventEmitter();

export const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
export const anthropic = process.env.ANTHROPIC_API_KEY
  ? new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY })
  : null;

export const redisConnection = REDIS_URL
  ? new Redis(REDIS_URL, {
      maxRetriesPerRequest: null,
      enableReadyCheck: false
    })
  : null;

export const scanQueue = redisConnection
  ? new Queue("atlasrevenue-scans", { connection: redisConnection as any })
  : null;

export const alertQueue = redisConnection
  ? new Queue("atlasrevenue-alerts", { connection: redisConnection as any })
  : null;

export const signalQueue = redisConnection
  ? new Queue("atlasrevenue-signals", { connection: redisConnection as any })
  : null;

export function asyncRoute(handler: import("./types.js").AsyncRouteHandler) {
  return (req: express.Request, res: express.Response, next: express.NextFunction) => {
    handler(req, res, next).catch(next);
  };
}

export function captureError(error: unknown, context?: Record<string, unknown>) {
  if (!SENTRY_ENABLED) return;

  Sentry.withScope(scope => {
    for (const [key, value] of Object.entries(context || {})) {
      scope.setContext(key, typeof value === "object" && value !== null ? value as Record<string, unknown> : { value });
    }
    Sentry.captureException(error);
  });
}
