import {
generateGovRevenueReport,
  GovRevenueQualityGateError,
  type CompanyIntake,
  type ProcurementRecord,
} from "./lib/govrevenue/govrevenue-report-engine.js";
import { EventEmitter } from "events";
import "dotenv/config";
import express from "express";
import cors from "cors";
import cookieParser from "cookie-parser";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import Stripe from "stripe";
import * as Sentry from "@sentry/node";
import { Pool } from "pg";
import OpenAI from "openai";
import Anthropic from "@anthropic-ai/sdk";
import { z } from "zod";
import { Queue, Worker } from "bullmq";
import { Redis } from "ioredis";
import puppeteer from "puppeteer";
import { renderWorldClassDashboard } from "./designEngine.js";
import { buildPdfStorageKey, isPdfStorageConfigured, storePdfObject } from "./lib/pdfStorage.js";
import { buildScanLinks, isEmailConfigured, notifyScanCompleted, notifyScanFailed, sendWeeklyAlert, sendBriefingEmail } from "./lib/emailNotifications.js";
import {
  escapeHtml, formatMoney, formatDate, fmtMoney, slugify,
  computeOutlierThreshold, parseEdpFromMarkdown, stripEdpFromMarkdown,
  validateReportConsistency, isAggregatorBuyer, isOverseasNotice,
  type ParsedEdp
} from "./lib/intel.js";
import {
  normaliseFromProcurementNotice,
  scoreAndBucketNotices,
  buildWinBrief,
  renderOpportunityCard,
  renderWinBriefHtml,
  renderOpportunityBoardContent,
  renderChaseNowPanel,
  renderChaseNowSection,
  oppCardCss,
  winBriefCss,
  deskOpportunityCss,
  reportChaseNowCss,
  chaseNowCss,
  noticesBoardCss,
  type ScoredOpportunity,
  type ScanOpportunityContext,
  type DeskOpportunityContext,
  type HomepageTeaserSignal,
  type ChaseStats as OppChaseStats,
} from "./lib/opportunityEngine.js";
type ScanStatus = "pending" | "running" | "completed" | "failed";
type UserTier = "free" | "pro" | "agency";
type UserRecord = {
  id: string;
  email: string;
  password_hash: string;
  created_at: string;
  tier: UserTier;
  stripe_customer_id: string | null;
  stripe_subscription_id: string | null;
  stripe_subscription_status: string | null;
};
type ScanRecord = {
  id: string;
  created_at: string;
  updated_at: string;
  status: ScanStatus;
  company_name: string;
  input_json: any;
  procurement_json: any | null;
  report_markdown: string | null;
  error_message: string | null;
  pdf_storage_key?: string | null;
  pdf_storage_url?: string | null;
  pdf_storage_etag?: string | null;
  pdf_storage_updated_at?: string | null;
  progress_stage?: string | null;
  user_id?: string | null;
  capability_statement?: string | null;
  outreach_emails?: string | null;
  frameworks_assessment?: string | null;
};

type SubscriptionRecord = {
  id: string;
  scan_id: string;
  company_name: string;
  email: string;
  input_json: any;
  alerted_notice_ids: string[];
  active: boolean;
  created_at: string;
  last_alerted_at: string | null;
};

type ProcurementNotice = {
  source: "Contracts Finder" | "Find a Tender";
  id: string;
  title: string;
  buyer: string;
  description: string;
  status: string;
  type: string;
  region: string;
  publishedDate: string | null;
  deadlineDate: string | null;
  awardedDate: string | null;
  valueLow: number | null;
  valueHigh: number | null;
  awardedValue: number | null;
  awardedSupplier: string;
  suitableForSme: boolean | null;
  url: string;
  keyword: string;
  sourceConfidence?: string;
  relevanceScore?: number;
  relevanceReason?: string;
};

type CompanyHouseRecord = {
  companyName: string;
  companyNumber: string;
  companyStatus: string;
  companyType: string;
  dateOfCreation: string | null;
  address: string;
  sicCodes: string[];
  url: string;
};

type ProcurementData = {
  generatedAt: string;
  quality?: any;
  keywords: string[];
  regions: string;
  companiesHouse?: {
    matches: CompanyHouseRecord[];
    errors: string[];
  };
  findTender?: {
    notices: ProcurementNotice[];
    errors: string[];
  };
  contractsFinder: {
    open: ProcurementNotice[];
    awarded: ProcurementNotice[];
    errors: string[];
  };
};

type HomepageSignal = {
  id: string;          // = source_url (unique per notice)
  category: string;
  title: string;
  buyer: string | null;
  source: string;      // 'CF' | 'FTS'
  source_url: string;
  notice_date: string | null;
  deadline_date: string | null;
  value_amount: number | null;
  status: string;
  fetched_at: string;
};

const app = express();
app.use(cors());
app.use(cookieParser());
app.use(express.json({ limit: "2mb" }));
app.use(express.urlencoded({ extended: true, limit: "2mb" }));

// Page-view tracker — fire-and-forget, never blocks a request
app.use((req, _res, next) => {
  if (pool && req.method === "GET" && !req.path.startsWith("/api") && !req.path.startsWith("/admin") && !req.path.startsWith("/billing") && !req.path.includes(".")) {
    const ip = String(req.headers["x-forwarded-for"] || req.ip || "").split(",")[0].trim().slice(0, 64);
    pool.query(
      `INSERT INTO visitor_logs (ip, path, user_agent, referer) VALUES ($1,$2,$3,$4)`,
      [ip, req.path.slice(0, 200), String(req.headers["user-agent"] || "").slice(0, 300), String(req.headers.referer || req.headers.referrer || "").slice(0, 300)]
    ).catch(() => {});
  }
  next();
});

const PORT = Number(process.env.PORT || 3000);
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4.1-mini";
// Report generation uses Claude when ANTHROPIC_API_KEY is set (the report is the product —
// a frontier model writes materially better ones), falling back to OpenAI otherwise.
const ANTHROPIC_MODEL = process.env.ANTHROPIC_MODEL || "claude-opus-4-8";
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || "change-me-now";
const JWT_SECRET = process.env.JWT_SECRET || "govrevenue-jwt-secret-change-in-prod";
const STRIPE_SECRET_KEY = process.env.STRIPE_SECRET_KEY || "";
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET || "";
const STRIPE_PRO_PRICE_ID = process.env.STRIPE_PRO_PRICE_ID || "";
const STRIPE_AGENCY_PRICE_ID = process.env.STRIPE_AGENCY_PRICE_ID || "";
const BASE_URL = process.env.BASE_URL || "https://govrevenue-agent-production.up.railway.app";
const REDIS_URL = process.env.REDIS_URL || null;
const RUN_WEB = process.env.RUN_WEB !== "false";
const RUN_WORKER = process.env.RUN_WORKER !== "false";
const SENTRY_DSN = process.env.SENTRY_DSN || "";
const SENTRY_ENABLED = Boolean(SENTRY_DSN);
// Opportunity bot: when set, newly-discovered signals are pushed to this Slack
// (or Discord-compatible) incoming webhook during the hourly refresh.
const SLACK_WEBHOOK_URL = process.env.SLACK_WEBHOOK_URL || "";

const stripe = STRIPE_SECRET_KEY ? new Stripe(STRIPE_SECRET_KEY) : null;

if (SENTRY_ENABLED) {
  Sentry.init({
    dsn: SENTRY_DSN,
    environment: process.env.SENTRY_ENVIRONMENT || process.env.NODE_ENV || "development",
    tracesSampleRate: 0
  });
}

const pool = process.env.DATABASE_URL
  ? new Pool({ connectionString: process.env.DATABASE_URL })
  : null;

const memoryStore = new Map<string, ScanRecord>();
const subMemStore = new Map<string, SubscriptionRecord>();
const sigMemStore = new Map<string, HomepageSignal>();
const briefMemStore = new Map<string, { id: string; email: string; category: string | null; source: string | null; created_at: string }>();
const deskCacheMemStore = new Map<string, { data: ProcurementData; cached_at: string }>();
const compilingDesks = new Set<string>();
const scanEvents = new EventEmitter();
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
const anthropic = process.env.ANTHROPIC_API_KEY
  ? new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY })
  : null;

const redisConnection = REDIS_URL
  ? new Redis(REDIS_URL, {
      maxRetriesPerRequest: null,
      enableReadyCheck: false
    })
  : null;

const scanQueue = redisConnection
  ? new Queue("govrevenue-scans", { connection: redisConnection as any })
  : null;

const alertQueue = redisConnection
  ? new Queue("govrevenue-alerts", { connection: redisConnection as any })
  : null;

const signalQueue = redisConnection
  ? new Queue("govrevenue-signals", { connection: redisConnection as any })
  : null;

type AsyncRouteHandler = (
  req: express.Request,
  res: express.Response,
  next: express.NextFunction
) => Promise<void>;

function asyncRoute(handler: AsyncRouteHandler) {
  return (req: express.Request, res: express.Response, next: express.NextFunction) => {
    handler(req, res, next).catch(next);
  };
}

function captureError(error: unknown, context?: Record<string, unknown>) {
  if (!SENTRY_ENABLED) return;

  Sentry.withScope(scope => {
    for (const [key, value] of Object.entries(context || {})) {
      scope.setContext(key, typeof value === "object" && value !== null ? value as Record<string, unknown> : { value });
    }
    Sentry.captureException(error);
  });
}


const intakeSchema = z.object({
  companyName: z.string().min(2),
  clientEmail: z.string().email().optional().or(z.literal("")).default(""),
  email: z.string().email().optional().or(z.literal("")).default(""),
  website: z.string().optional().default(""),
  location: z.string().optional().default(""),
  areasServed: z.string().optional().default(""),
  mainServices: z.string().min(5),
  secondaryServices: z.string().optional().default(""),
  idealBuyers: z.string().optional().default(""),
  idealContractSize: z.string().optional().default(""),
  maximumContractSize: z.string().optional().default(""),
  teamSize: z.string().optional().default(""),
  publicSectorExperience: z.string().optional().default(""),
  caseStudies: z.string().optional().default(""),
  certifications: z.string().optional().default(""),
  excludedServices: z.string().optional().default(""),
  regionsToScan: z.string().optional().default(""),
  mainGoal: z.string().optional().default(""),
  biggestConcern: z.string().optional().default(""),
  preferredOutput: z.string().optional().default(""),
  frameworkStatus: z.string().optional().default(""),
  lastPublicContract: z.string().optional().default("")
});

function nowIso() {
  return new Date().toISOString();
}

function makeId() {
  return `scan_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function renderInline(value: string) {
  return escapeHtml(value)
    .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
    .replace(/\[(.*?)\]\((https?:\/\/[^\s)]+)\)/g, '<a href="$2" target="_blank" rel="noreferrer">$1</a>');
}

function markdownToHtml(markdown: string) {
  const lines = markdown.split(/\r?\n/);
  let html = "";
  let inList = false;
  let inOrderedList = false;
  let inTable = false;

  const closeLists = () => {
    if (inList) html += "</ul>";
    if (inOrderedList) html += "</ol>";
    inList = false;
    inOrderedList = false;
  };

  const closeTable = () => {
    if (inTable) html += "</tbody></table>";
    inTable = false;
  };

  for (const rawLine of lines) {
    const line = rawLine.trim();

    if (!line) {
      closeLists();
      closeTable();
      continue;
    }

    if (line.startsWith("|") && line.endsWith("|")) {
      closeLists();
      const cells = line.split("|").slice(1, -1);
      const isDivider = cells.every(cell => /^:?-{3,}:?$/.test(cell.trim()));
      if (isDivider) continue;
      if (!inTable) {
        html += '<table class="report-table"><tbody>';
        inTable = true;
      }
      html += "<tr>" + cells.map(cell => `<td>${renderInline(cell.trim())}</td>`).join("") + "</tr>";
      continue;
    }

    closeTable();

    if (line.startsWith("# ")) {
      closeLists();
      html += `<h1>${renderInline(line.slice(2))}</h1>`;
      continue;
    }

    if (line.startsWith("## ")) {
      closeLists();
      html += `<h2>${renderInline(line.slice(3))}</h2>`;
      continue;
    }

    if (line.startsWith("### ")) {
      closeLists();
      html += `<h3>${renderInline(line.slice(4))}</h3>`;
      continue;
    }

    if (line.startsWith("- ")) {
      if (!inList) {
        closeLists();
        html += "<ul>";
        inList = true;
      }
      html += `<li>${renderInline(line.slice(2))}</li>`;
      continue;
    }

    if (/^\d+\.\s+/.test(line)) {
      if (!inOrderedList) {
        closeLists();
        html += "<ol>";
        inOrderedList = true;
      }
      html += `<li>${renderInline(line.replace(/^\d+\.\s+/, ""))}</li>`;
      continue;
    }

    closeLists();
    html += `<p class="justify-force">${renderInline(line)}</p>`;
  }

  closeLists();
  closeTable();
  return html;
}

function buildKeywords(input: z.infer<typeof intakeSchema>): string[] {
  const text = [
    input.mainServices,
    input.secondaryServices,
    input.idealBuyers,
    input.mainGoal,
    input.preferredOutput,
    input.lastPublicContract,
    input.frameworkStatus
  ]
    .join(" ")
    .toLowerCase();

  interface SectorDef {
    key: string;
    // All triggers are multi-word phrases — score = number of these that match
    primary: string[];
    // If ANY exclusion phrase is present the sector is disqualified even if primary matched
    excludeIf?: string[];
    keywords: string[];
  }

  const SECTORS: SectorDef[] = [
    {
      key: "social-housing-maintenance",
      primary: [
        "social housing", "housing repairs", "void maintenance", "void property",
        "responsive repairs", "planned maintenance", "housing maintenance",
        "repairs and maintenance", "housing refurbishment", "almo",
        "registered social landlord", "housing association repairs",
        "housing management", "voids and planned", "resident-facing",
        "housing with care", "housing compliance", "housing contract"
      ],
      keywords: [
        "housing repairs and maintenance",
        "responsive repairs",
        "void property maintenance",
        "planned maintenance",
        "social housing maintenance",
        "housing refurbishment",
        "property maintenance housing",
        "housing compliance works"
      ]
    },
    {
      key: "housing-retrofit",
      primary: [
        "social housing retrofit", "shdf", "social housing decarbonisation",
        "housing decarbonisation", "whole house retrofit",
        "housing energy efficiency", "housing insulation", "external wall insulation",
        "loft insulation housing", "ventilation upgrade", "housing net zero"
      ],
      keywords: [
        "social housing retrofit",
        "housing decarbonisation",
        "SHDF retrofit",
        "energy efficiency housing",
        "whole house retrofit"
      ]
    },
    {
      key: "construction-qs",
      primary: [
        "quantity surveying", "quantity surveyor", "cost consultancy",
        "cost consultant", "cost management", "qs services",
        "bills of quantities", "pre-contract cost", "post-contract cost"
      ],
      excludeIf: ["social housing", "housing repairs", "void maintenance", "responsive repairs"],
      keywords: [
        "quantity surveying",
        "cost management",
        "cost consultancy",
        "construction consultancy",
        "employer's agent"
      ]
    },
    {
      key: "construction-pm",
      primary: [
        "construction project management", "employer's agent", "employer agent",
        "contract administration", "project controls", "programme management",
        "project management construction", "site management", "clerk of works"
      ],
      excludeIf: ["social housing", "housing repairs", "void maintenance", "responsive repairs"],
      keywords: [
        "construction project management",
        "project management",
        "programme management",
        "employer's agent",
        "contract administration"
      ]
    },
    {
      key: "building-surveying",
      primary: [
        "building surveying", "building survey", "condition survey",
        "six facet survey", "estate consultancy", "estate management services",
        "strategic estate", "built asset consultancy",
        "asset management consultancy", "property consultancy",
        "dilapidations", "measured survey", "planned preventive maintenance survey"
      ],
      excludeIf: ["social housing", "housing repairs", "void maintenance", "responsive repairs"],
      keywords: [
        "building surveying",
        "condition survey",
        "estate consultancy",
        "asset management",
        "property consultancy",
        "built asset consultancy"
      ]
    },
    {
      key: "cleaning",
      primary: [
        "cleaning services", "specialist cleaning", "deep cleaning",
        "clinical cleaning", "healthcare cleaning", "infection control cleaning",
        "office cleaning", "communal cleaning", "janitorial services",
        "washroom services", "bio fogging", "sanitisation services",
        "environmental cleaning", "school cleaning", "academy cleaning"
      ],
      excludeIf: ["social housing", "housing repairs", "building surveying"],
      keywords: [
        "cleaning services",
        "specialist cleaning",
        "healthcare cleaning",
        "deep cleaning",
        "infection control cleaning",
        "communal area cleaning",
        "estate cleaning",
        "school cleaning"
      ]
    },
    {
      key: "facilities-management",
      primary: [
        "facilities management", "hard fm", "soft fm",
        "total fm", "integrated fm", "fm services", "tupe fm",
        "building services maintenance", "mechanical and electrical maintenance",
        "m&e maintenance", "helpdesk services"
      ],
      excludeIf: ["social housing", "housing repairs", "void maintenance"],
      keywords: [
        "facilities management",
        "hard FM",
        "soft FM",
        "integrated facilities management",
        "building services maintenance"
      ]
    },
    {
      key: "energy-retrofit",
      primary: [
        "solar pv", "photovoltaic", "heat pump installation",
        "ev charging", "energy efficiency consultancy",
        "retrofit consultancy", "net zero consultancy",
        "decarbonisation consultancy", "energy assessment",
        "epc assessment", "insulation contractor",
        "air source heat pump", "ground source heat pump"
      ],
      excludeIf: ["social housing", "housing repairs", "housing maintenance"],
      keywords: [
        "energy efficiency",
        "retrofit",
        "solar PV",
        "heat pumps",
        "net zero services",
        "decarbonisation",
        "energy consultancy"
      ]
    },
    {
      key: "software-ict",
      primary: [
        "software development", "software services", "digital transformation",
        "saas", "cloud services", "it services", "cyber security",
        "data analytics", "app development", "technology services",
        "software platform", "managed it", "it support services"
      ],
      keywords: [
        "software development",
        "digital transformation",
        "IT services",
        "technology solutions",
        "cloud services",
        "cyber security"
      ]
    },
    {
      key: "training-skills",
      primary: [
        "training services", "learning and development", "skills training",
        "apprenticeship", "coaching services", "professional development",
        "training provider", "workforce development", "skills programme",
        "e-learning", "classroom training", "cpd training"
      ],
      keywords: [
        "training services",
        "professional development",
        "skills training",
        "apprenticeship programmes",
        "workforce development"
      ]
    },
    {
      key: "photography",
      primary: [
        "photography services", "event photography",
        "portrait photography", "graduation photography",
        "property photography", "commercial photography",
        "wedding photography", "corporate photography"
      ],
      keywords: [
        "photography",
        "event photography",
        "corporate photography",
        "property photography",
        "visual content services"
      ]
    },
    {
      key: "marketing-creative",
      primary: [
        "marketing agency", "creative agency", "communications agency",
        "content production", "video production", "campaign management",
        "brand strategy", "public relations", "media production",
        "graphic design services", "social media management"
      ],
      keywords: [
        "marketing services",
        "communications",
        "creative services",
        "content production",
        "video production"
      ]
    },
    {
      key: "healthcare",
      primary: [
        "healthcare consultancy", "nhs consultancy", "clinical services",
        "health services management", "patient pathway", "primary care services",
        "community health", "mental health services", "care home management"
      ],
      keywords: [
        "healthcare services",
        "NHS services",
        "clinical services",
        "health management",
        "patient services"
      ]
    }
  ];

  // Score each sector: count primary phrase matches, skip if excluded
  const scored = SECTORS
    .filter(def => !def.excludeIf?.some(exc => text.includes(exc)))
    .map(def => ({
      def,
      score: def.primary.filter(t => text.includes(t)).length
    }))
    .filter(m => m.score > 0)
    .sort((a, b) => b.score - a.score);

  if (!scored.length) {
    // Fallback: extract service phrases directly from intake
    const phrases = [input.mainServices, input.secondaryServices]
      .join(",")
      .split(/[,./;|]+/)
      .map(v => v.trim().toLowerCase())
      .filter(v => v.length >= 4 && v.length <= 60)
      .slice(0, 8);
    return Array.from(new Set(phrases));
  }

  // Primary sector keywords
  const selected = new Set<string>(scored[0].def.keywords);

  // Allow one compatible secondary sector to supplement keywords up to the cap
  const COMPATIBLE_SECONDARY: Record<string, string[]> = {
    "social-housing-maintenance": ["housing-retrofit"],
    "housing-retrofit":           ["social-housing-maintenance"],
    "construction-qs":            ["construction-pm", "building-surveying"],
    "construction-pm":            ["construction-qs", "building-surveying"],
    "building-surveying":         ["construction-qs", "construction-pm"],
    "energy-retrofit":            ["housing-retrofit"],
    "cleaning":                   ["facilities-management"],
    "facilities-management":      ["cleaning"]
  };

  if (scored.length > 1 && selected.size < 8) {
    const allowed = COMPATIBLE_SECONDARY[scored[0].def.key] ?? [];
    const secondary = scored.slice(1).find(m => allowed.includes(m.def.key));
    if (secondary) {
      for (const kw of secondary.def.keywords) {
        if (selected.size >= 8) break;
        selected.add(kw);
      }
    }
  }

  return [...selected].slice(0, 8);
}

function buildRegion(input: z.infer<typeof intakeSchema>) {
  const raw = `${input.regionsToScan} ${input.areasServed} ${input.location}`.toLowerCase();
  const regions: string[] = [];

  if (raw.includes("west midlands") || raw.includes("birmingham") || raw.includes("coventry") || raw.includes("wolverhampton") || raw.includes("walsall") || raw.includes("sandwell")) regions.push("West Midlands");
  if (raw.includes("london") || raw.includes("greater london")) regions.push("London");
  if (raw.includes("north west") || raw.includes("manchester") || raw.includes("liverpool") || raw.includes("lancashire") || raw.includes("cheshire") || raw.includes("cumbria")) regions.push("North West");
  if (raw.includes("east midlands") || raw.includes("nottingham") || raw.includes("leicester") || raw.includes("derby") || raw.includes("northampton") || raw.includes("lincoln")) regions.push("East Midlands");
  if (raw.includes("south east") || raw.includes("kent") || raw.includes("surrey") || raw.includes("sussex") || raw.includes("hampshire") || raw.includes("oxford") || raw.includes("reading") || raw.includes("brighton")) regions.push("South East");
  if (raw.includes("yorkshire") || raw.includes("sheffield") || raw.includes("leeds") || raw.includes("bradford") || raw.includes("hull") || raw.includes("york") || raw.includes("rotherham") || raw.includes("doncaster") || raw.includes("barnsley") || raw.includes("wakefield")) regions.push("Yorkshire and The Humber");
  if (raw.includes("north east") || raw.includes("newcastle") || raw.includes("sunderland") || raw.includes("durham") || raw.includes("tyne") || raw.includes("gateshead") || raw.includes("middlesbrough") || raw.includes("stockton")) regions.push("North East");
  if (raw.includes("east of england") || raw.includes("norfolk") || raw.includes("suffolk") || raw.includes("essex") || raw.includes("cambridge") || raw.includes("hertfordshire") || raw.includes("bedfordshire") || raw.includes("norwich") || raw.includes("ipswich")) regions.push("East of England");
  if (raw.includes("south west") || raw.includes("bristol") || raw.includes("plymouth") || raw.includes("exeter") || raw.includes("devon") || raw.includes("cornwall") || raw.includes("somerset") || raw.includes("gloucester") || raw.includes("swindon") || raw.includes("dorset") || raw.includes("wiltshire")) regions.push("South West");
  if (raw.includes("wales") || raw.includes("cardiff") || raw.includes("swansea") || raw.includes("newport") || raw.includes("welsh") || raw.includes("wrexham") || raw.includes("rhondda")) regions.push("Wales");
  if (raw.includes("scotland") || raw.includes("edinburgh") || raw.includes("glasgow") || raw.includes("aberdeen") || raw.includes("dundee") || raw.includes("scottish") || raw.includes("highland") || raw.includes("stirling")) regions.push("Scotland");
  if (raw.includes("northern ireland") || raw.includes("belfast") || raw.includes("ni council") || raw.includes("derry") || raw.includes("antrim") || raw.includes("armagh")) regions.push("Northern Ireland");

  return regions.length ? Array.from(new Set(regions)).join(",") : "";
}

function noticeUrl(id: string) {
  return id ? `https://www.contractsfinder.service.gov.uk/Notice/${encodeURIComponent(id)}` : "https://www.contractsfinder.service.gov.uk/";
}

function decodeHtmlEntities(str: string): string {
  return str
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#0*39;/g, "'")
    .replace(/&apos;/g, "'");
}

function normaliseNotice(raw: any, keyword: string): ProcurementNotice | null {
  const item = raw?.item || raw;
  if (!item) return null;

  const id = String(item.id || item.noticeIdentifier || "");
  const title = decodeHtmlEntities(String(item.title || "").trim());
  if (!title) return null;

  return {
    source: "Contracts Finder",
    id,
    title,
    buyer: decodeHtmlEntities(String(item.organisationName || "Not stated")),
    description: decodeHtmlEntities(String(item.description || "").slice(0, 900)),
    status: String(item.noticeStatus || ""),
    type: String(item.noticeType || ""),
    region: String(item.regionText || item.region || ""),
    publishedDate: item.publishedDate || null,
    deadlineDate: item.deadlineDate || null,
    awardedDate: item.awardedDate || null,
    valueLow: item.valueLow != null ? (Number(item.valueLow) || null) : null,
    valueHigh: item.valueHigh != null ? (Number(item.valueHigh) || null) : null,
    awardedValue: item.awardedValue != null ? (Number(item.awardedValue) || null) : null,
    awardedSupplier: decodeHtmlEntities(String(item.awardedSupplier || "")),
    suitableForSme: typeof item.isSuitableForSme === "boolean" ? item.isSuitableForSme : null,
    url: noticeUrl(id),
    keyword
  };
}


function companyHouseAddress(address: any) {
  if (!address) return "";
  return [
    address.premises,
    address.address_line_1,
    address.address_line_2,
    address.locality,
    address.region,
    address.postal_code,
    address.country
  ].filter(Boolean).join(", ");
}

async function companiesHouseSearch(companyName: string): Promise<{ matches: CompanyHouseRecord[]; errors: string[] }> {
  const apiKey = process.env.COMPANIES_HOUSE_API_KEY || "";
  const errors: string[] = [];

  if (!apiKey || !companyName.trim()) {
    return { matches: [], errors: apiKey ? [] : ["Companies House API key not configured."] };
  }

  try {
    const url = new URL("https://api.company-information.service.gov.uk/search/companies");
    url.searchParams.set("q", companyName.trim());
    url.searchParams.set("items_per_page", "5");

    const response = await fetch(url, {
      headers: {
        Authorization: `Basic ${Buffer.from(`${apiKey}:`).toString("base64")}`,
        Accept: "application/json"
      }
    });

    if (!response.ok) {
      return { matches: [], errors: [`Companies House search failed: ${response.status} ${response.statusText}`] };
    }

    const data = await response.json();
    const items = Array.isArray(data.items) ? data.items : [];

    const matches = items.map((item: any): CompanyHouseRecord => ({
      companyName: String(item.title || item.company_name || "").trim(),
      companyNumber: String(item.company_number || ""),
      companyStatus: String(item.company_status || ""),
      companyType: String(item.company_type || ""),
      dateOfCreation: item.date_of_creation || null,
      address: companyHouseAddress(item.address),
      sicCodes: Array.isArray(item.sic_codes) ? item.sic_codes.map(String) : [],
      url: item.links?.self
        ? `https://find-and-update.company-information.service.gov.uk${item.links.self}`
        : "https://find-and-update.company-information.service.gov.uk/"
    })).filter((item: CompanyHouseRecord) => item.companyName && item.companyNumber);

    return { matches, errors };
  } catch (error: any) {
    return { matches: [], errors: [error?.message || String(error)] };
  }
}

function normaliseFindTenderRelease(release: any, keyword: string): ProcurementNotice | null {
  const tender = release?.tender || {};
  const title = String(tender.title || release.title || "").trim();
  if (!title) return null;

  const buyerName = decodeHtmlEntities(String(release?.buyer?.name || release?.parties?.find?.((party: any) => Array.isArray(party.roles) && party.roles.includes("buyer"))?.name || "Not stated"));
  const amount = tender?.value?.amount != null ? (Number(tender.value.amount) || null) : null;
  const region = String(
    tender?.items?.[0]?.deliveryAddresses?.[0]?.region ||
    release?.parties?.[0]?.address?.region ||
    ""
  );

  return {
    source: "Find a Tender",
    id: String(release.id || release.ocid || ""),
    title,
    buyer: buyerName,
    description: String(tender.description || release.description || "").slice(0, 900),
    status: String(tender.status || ""),
    type: Array.isArray(release.tag) ? release.tag.join(", ") : "",
    region,
    publishedDate: release.date || null,
    deadlineDate: tender?.tenderPeriod?.endDate || null,
    awardedDate: release.date || null,
    valueLow: amount,
    valueHigh: amount,
    awardedValue: amount,
    awardedSupplier: "",
    suitableForSme: null,
    url: release.id ? `https://www.find-tender.service.gov.uk/Notice/${encodeURIComponent(String(release.id))}` : "https://www.find-tender.service.gov.uk/",
    keyword
  };
}

async function findTenderSearch(keywords: string[], signal?: AbortSignal): Promise<{ notices: ProcurementNotice[]; errors: string[] }> {
  const errors: string[] = [];
  const notices: ProcurementNotice[] = [];
  const keywordSet = keywords.map(k => k.toLowerCase()).filter(Boolean);

  try {
    const to = new Date();
    const from = new Date(to.getTime() - 1000 * 60 * 60 * 24 * 90);
    const url = new URL("https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages");
    url.searchParams.set("limit", "100");
    url.searchParams.set("stages", "tender,award");
    url.searchParams.set("updatedFrom", from.toISOString().slice(0, 19));
    url.searchParams.set("updatedTo", to.toISOString().slice(0, 19));

    const response = await fetch(url, { headers: { Accept: "application/json" }, signal });

    if (!response.ok) {
      return { notices: [], errors: [`Find a Tender OCDS search failed: ${response.status} ${response.statusText}`] };
    }

    const data = await response.json();
    const releases = Array.isArray(data.releases) ? data.releases : [];

    const scored: { notice: ProcurementNotice; score: number }[] = [];
    for (const release of releases) {
      const haystack = [
        release?.tender?.title,
        release?.tender?.description,
        release?.buyer?.name,
        release?.parties?.map?.((party: any) => party?.name).join(" ")
      ].filter(Boolean).join(" ").toLowerCase();

      const matchCount = keywordSet.filter(kw => haystack.includes(kw)).length;
      if (!matchCount) continue;

      const matchedKeyword = keywordSet.find(kw => haystack.includes(kw))!;
      const notice = normaliseFindTenderRelease(release, matchedKeyword);
      if (notice) scored.push({ notice, score: matchCount });
    }

    // primary: most keyword matches; secondary: most recent date
    scored.sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      const da = new Date(a.notice.publishedDate || a.notice.awardedDate || 0).getTime();
      const db = new Date(b.notice.publishedDate || b.notice.awardedDate || 0).getTime();
      return db - da;
    });
    const notices = scored.map(s => s.notice);

    return { notices: dedupeNotices(notices).map(notice => enrichNoticeQuality(notice, keywords)), errors };
  } catch (error: any) {
    return { notices: [], errors: [error?.message || String(error)] };
  }
}


const CF_ENDPOINTS = [
  "https://www.contractsfinder.service.gov.uk/api/rest/2/search_notices/json",
  "https://www.contractsfinder.service.gov.uk/api/rest/2/search_notices/JSON",
  "https://www.contractsfinder.service.gov.uk/api/rest/2/search_notices"
];

async function contractsFinderPage(
  searchCriteria: any,
  keyword: string,
  from: number,
  size: number,
  signal?: AbortSignal
): Promise<{ notices: ProcurementNotice[]; total: number }> {
  let lastError = "";
  for (const url of CF_ENDPOINTS) {
    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({ searchCriteria, size, from }),
        signal
      });
      if (!resp.ok) { lastError = `${resp.status} ${resp.statusText}`; continue; }
      const data = await resp.json();
      const list = Array.isArray(data.noticeList) ? data.noticeList : [];
      // CF returns maxResults = total matching notices in the full result set
      const total: number = typeof data.maxResults === "number" ? data.maxResults : list.length + from;
      const notices = list.map((e: any) => normaliseNotice(e, keyword)).filter(Boolean) as ProcurementNotice[];
      return { notices, total };
    } catch (err: any) {
      lastError = err?.message || String(err);
    }
  }
  throw new Error(lastError || "Contracts Finder search failed");
}

// Paginates through CF exhaustively — stops only when CF has no more results.
async function contractsFinderSearchAll(
  searchCriteria: any,
  keyword: string,
  signal?: AbortSignal
): Promise<ProcurementNotice[]> {
  const PAGE_SIZE = 100;
  const all: ProcurementNotice[] = [];
  let from = 0;

  while (true) {
    const { notices, total } = await contractsFinderPage(searchCriteria, keyword, from, PAGE_SIZE, signal);
    all.push(...notices);
    if (notices.length < PAGE_SIZE || all.length >= total) break;
    from += PAGE_SIZE;
  }

  return all;
}

function dedupeNotices(notices: ProcurementNotice[]) {
  const seen = new Set<string>();
  const output: ProcurementNotice[] = [];

  for (const notice of notices) {
    const key = notice.id || `${notice.title}-${notice.buyer}-${notice.publishedDate}`;
    if (seen.has(key)) continue;
    seen.add(key);
    output.push(notice);
  }

  return output;
}

// Second-pass dedup: collapse framework lots that share the exact same title+buyer.
// Sorts by deadline (earliest first) so the most urgent lot is kept.
// Also deduplicates by URL for cross-source (CF + FTS) overlaps.
function dedupeNoticesSoft(notices: ProcurementNotice[]): ProcurementNotice[] {
  const byDeadline = [...notices].sort((a, b) => {
    const da = a.deadlineDate ? new Date(a.deadlineDate).getTime() : Infinity;
    const db = b.deadlineDate ? new Date(b.deadlineDate).getTime() : Infinity;
    return da - db;
  });
  const seenUrl = new Set<string>();
  const seenTitleBuyer = new Set<string>();
  const out: ProcurementNotice[] = [];
  for (const n of byDeadline) {
    const urlKey = n.url || "";
    if (urlKey && seenUrl.has(urlKey)) continue;
    if (urlKey) seenUrl.add(urlKey);
    const tbKey = `${n.title.trim().toLowerCase()}|||${(n.buyer || "").trim().toLowerCase()}`;
    if (seenTitleBuyer.has(tbKey)) continue;
    seenTitleBuyer.add(tbKey);
    out.push(n);
  }
  return out;
}







async function initDb() {
  if (!pool) {
    console.log("[db] DATABASE_URL not set. Using memory store.");
    return;
  }

  await pool.query(`
    CREATE TABLE IF NOT EXISTS scans (
      id TEXT PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      status TEXT NOT NULL,
      company_name TEXT NOT NULL,
      input_json JSONB NOT NULL,
      procurement_json JSONB,
      report_markdown TEXT,
      error_message TEXT,
      pdf_storage_key TEXT,
      pdf_storage_url TEXT,
      pdf_storage_etag TEXT,
      pdf_storage_updated_at TIMESTAMPTZ
    );
  `);

  await pool.query(`ALTER TABLE scans ADD COLUMN IF NOT EXISTS procurement_json JSONB;`);
  await pool.query(`ALTER TABLE scans ADD COLUMN IF NOT EXISTS pdf_storage_key TEXT;`);
  await pool.query(`ALTER TABLE scans ADD COLUMN IF NOT EXISTS pdf_storage_url TEXT;`);
  await pool.query(`ALTER TABLE scans ADD COLUMN IF NOT EXISTS pdf_storage_etag TEXT;`);
  await pool.query(`ALTER TABLE scans ADD COLUMN IF NOT EXISTS pdf_storage_updated_at TIMESTAMPTZ;`);
  await pool.query(`ALTER TABLE scans ADD COLUMN IF NOT EXISTS progress_stage TEXT;`);
  await pool.query(`ALTER TABLE scans ADD COLUMN IF NOT EXISTS user_id TEXT;`);
  await pool.query(`ALTER TABLE scans ADD COLUMN IF NOT EXISTS capability_statement TEXT;`);
  await pool.query(`ALTER TABLE scans ADD COLUMN IF NOT EXISTS outreach_emails TEXT;`);
  await pool.query(`ALTER TABLE scans ADD COLUMN IF NOT EXISTS frameworks_assessment TEXT;`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      email TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      tier TEXT NOT NULL DEFAULT 'free',
      stripe_customer_id TEXT,
      stripe_subscription_id TEXT,
      stripe_subscription_status TEXT
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS subscriptions (
      id TEXT PRIMARY KEY,
      scan_id TEXT NOT NULL,
      company_name TEXT NOT NULL,
      email TEXT NOT NULL,
      input_json JSONB NOT NULL,
      alerted_notice_ids TEXT[] NOT NULL DEFAULT '{}',
      active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL,
      last_alerted_at TIMESTAMPTZ
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS homepage_signals (
      id            TEXT PRIMARY KEY,
      category      TEXT NOT NULL,
      title         TEXT NOT NULL,
      buyer         TEXT,
      source        TEXT NOT NULL,
      source_url    TEXT NOT NULL,
      notice_date   TIMESTAMPTZ,
      deadline_date TIMESTAMPTZ,
      value_amount  BIGINT,
      status        TEXT NOT NULL,
      fetched_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  await pool.query(`ALTER TABLE homepage_signals ADD COLUMN IF NOT EXISTS deadline_date TIMESTAMPTZ`);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_homepage_signals_cat_fetched
      ON homepage_signals (category, fetched_at DESC);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS briefing_subscribers (
      id          TEXT PRIMARY KEY,
      email       TEXT NOT NULL,
      category    TEXT,
      source      TEXT,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE (email)
    );
  `);
  // Backfill for tables created before the source column existed.
  await pool.query(`ALTER TABLE briefing_subscribers ADD COLUMN IF NOT EXISTS source TEXT`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS desk_cache (
      slug        TEXT PRIMARY KEY,
      data        JSONB NOT NULL,
      cached_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS visitor_logs (
      id          BIGSERIAL PRIMARY KEY,
      ip          TEXT,
      path        TEXT,
      user_agent  TEXT,
      referer     TEXT,
      visited_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_visitor_logs_visited ON visitor_logs (visited_at DESC);`);

  console.log("[db] ready");
}

// Returns the signals that were genuinely new (inserted, not updated) so the
// opportunity bot can announce only fresh notices. Postgres `xmax = 0` is true
// only for a freshly-inserted row, false when ON CONFLICT performed an UPDATE.
async function upsertSignals(signals: HomepageSignal[]): Promise<HomepageSignal[]> {
  if (signals.length === 0) return [];
  const inserted: HomepageSignal[] = [];
  if (pool) {
    for (const s of signals) {
      const r = await pool.query<{ inserted: boolean }>(
        `INSERT INTO homepage_signals (id, category, title, buyer, source, source_url, notice_date, deadline_date, value_amount, status, fetched_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
         ON CONFLICT (id) DO UPDATE SET
           deadline_date = EXCLUDED.deadline_date,
           value_amount  = EXCLUDED.value_amount,
           status        = EXCLUDED.status,
           fetched_at    = EXCLUDED.fetched_at
         RETURNING (xmax = 0) AS inserted`,
        [s.id, s.category, s.title, s.buyer, s.source, s.source_url,
         s.notice_date, s.deadline_date ?? null, s.value_amount, s.status, s.fetched_at]
      );
      if (r.rows[0]?.inserted) inserted.push(s);
    }
  } else {
    for (const s of signals) {
      if (!sigMemStore.has(s.id)) inserted.push(s);
      sigMemStore.set(s.id, s);
    }
  }
  return inserted;
}

async function queryLatestSignals(limit: number): Promise<HomepageSignal[]> {
  if (pool) {
    const r = await pool.query<HomepageSignal>(
      `SELECT * FROM homepage_signals ORDER BY fetched_at DESC LIMIT $1`, [limit]
    );
    return r.rows;
  }
  return [...sigMemStore.values()]
    .sort((a, b) => b.fetched_at.localeCompare(a.fetched_at))
    .slice(0, limit);
}

async function count24hSignals(): Promise<number> {
  if (pool) {
    const r = await pool.query<{ n: string }>(
      `SELECT COUNT(*) AS n FROM homepage_signals WHERE notice_date > NOW() - INTERVAL '24 hours'`
    );
    return parseInt(r.rows[0]?.n || "0", 10);
  }
  const cutoff = new Date(Date.now() - 86_400_000).toISOString();
  return [...sigMemStore.values()].filter(s => (s.notice_date || s.fetched_at) > cutoff).length;
}

async function findSamplePdf(): Promise<string | null> {
  // Only serve an explicitly configured sample — never expose real customer PDFs.
  const explicit = process.env.SAMPLE_PDF_URL?.trim();
  return explicit || null;
}

async function queryDeskSignals(categories: string[]): Promise<Map<string, HomepageSignal>> {
  const out = new Map<string, HomepageSignal>();
  if (pool) {
    const r = await pool.query<HomepageSignal>(
      `SELECT DISTINCT ON (category) id, category, title, buyer, source, source_url, notice_date, deadline_date, value_amount, status, fetched_at
       FROM homepage_signals WHERE category = ANY($1) ORDER BY category, notice_date DESC NULLS LAST`,
      [categories]
    );
    for (const row of r.rows) out.set(row.category, row);
    return out;
  }
  for (const s of sigMemStore.values()) {
    if (categories.includes(s.category)) {
      const existing = out.get(s.category);
      if (!existing || s.fetched_at > existing.fetched_at) out.set(s.category, s);
    }
  }
  return out;
}

async function queryOpenDeskSignals(limit: number): Promise<HomepageSignal[]> {
  if (pool) {
    const r = await pool.query<HomepageSignal>(
      `SELECT id, category, title, buyer, source, source_url, notice_date, deadline_date, value_amount, status, fetched_at
       FROM homepage_signals
       WHERE LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%'
       ORDER BY notice_date DESC NULLS LAST
       LIMIT $1`,
      [limit]
    );
    return r.rows;
  }
  return [...sigMemStore.values()]
    .filter(s => /open|active/i.test(s.status || ""))
    .sort((a, b) => (b.notice_date || "").localeCompare(a.notice_date || ""))
    .slice(0, limit);
}

async function queryChaseableSignals(limit: number): Promise<HomepageSignal[]> {
  if (pool) {
    const r = await pool.query<HomepageSignal>(
      `SELECT DISTINCT ON (LOWER(TRIM(title)), LOWER(TRIM(COALESCE(buyer,''))))
         id, category, title, buyer, source, source_url, notice_date, deadline_date, value_amount, status, fetched_at
       FROM homepage_signals
       WHERE (LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')
         AND (deadline_date IS NULL OR deadline_date > NOW() + INTERVAL '5 days')
       ORDER BY LOWER(TRIM(title)), LOWER(TRIM(COALESCE(buyer,''))), deadline_date ASC NULLS LAST, notice_date DESC NULLS LAST
       LIMIT $1`,
      [limit]
    );
    return r.rows;
  }
  return [...sigMemStore.values()]
    .filter(s => /open|active/i.test(s.status || ""))
    .sort((a, b) => {
      if (a.deadline_date && b.deadline_date) return a.deadline_date.localeCompare(b.deadline_date);
      if (a.deadline_date) return -1;
      if (b.deadline_date) return 1;
      return (b.notice_date || "").localeCompare(a.notice_date || "");
    })
    .slice(0, limit);
}

type ChaseStats = {
  totalOpen: number;
  avgValueK: number | null;
  closingThisMonth: number;
  byDesk: { category: string; count: number }[];
};

async function queryChaseableStats(): Promise<ChaseStats> {
  if (!pool) return { totalOpen: 0, avgValueK: null, closingThisMonth: 0, byDesk: [] };
  const [totals, byDesk, closing, avgVal] = await Promise.all([
    pool.query<{ total: string }>(
      `SELECT COUNT(*) AS total
       FROM homepage_signals
       WHERE (LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')
         AND (deadline_date IS NULL OR deadline_date > NOW() + INTERVAL '5 days')`
    ),
    pool.query<{ category: string; cnt: string }>(
      `SELECT category, COUNT(*) AS cnt
       FROM homepage_signals
       WHERE (LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')
         AND (deadline_date IS NULL OR deadline_date > NOW() + INTERVAL '5 days')
       GROUP BY category ORDER BY cnt DESC LIMIT 5`
    ),
    pool.query<{ cnt: string }>(
      `SELECT COUNT(*) AS cnt FROM homepage_signals
       WHERE (LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')
         AND deadline_date BETWEEN NOW() AND NOW() + INTERVAL '30 days'`
    ),
    pool.query<{ avg_k: string }>(
      `SELECT ROUND(AVG(value_amount) / 1000) AS avg_k
       FROM homepage_signals
       WHERE (LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')
         AND (deadline_date IS NULL OR deadline_date > NOW() + INTERVAL '5 days')
         AND value_amount > 0`
    ),
  ]);
  const rawAvg = parseFloat(avgVal.rows[0]?.avg_k || "0");
  return {
    totalOpen: parseInt(totals.rows[0]?.total || "0"),
    avgValueK: rawAvg > 0 ? rawAvg : null,
    closingThisMonth: parseInt(closing.rows[0]?.cnt || "0"),
    byDesk: byDesk.rows.map(r => ({ category: r.category, count: parseInt(r.cnt) })),
  };
}

type ChartDataPoint = { month: string; total_m: number };
async function queryChartData(): Promise<{ points: ChartDataPoint[]; illustrative: boolean; topDesk: string | null }> {
  if (pool) {
    const [r, topR] = await Promise.all([
      pool.query<ChartDataPoint>(
        `SELECT to_char(date_trunc('month', notice_date), 'Mon') AS month,
                ROUND(SUM(COALESCE(value_amount, 0)) / 1e6::numeric, 2)::float AS total_m
         FROM homepage_signals
         WHERE notice_date > NOW() - INTERVAL '12 months'
           AND notice_date <= NOW()
           AND value_amount IS NOT NULL
           AND value_amount > 0
         GROUP BY date_trunc('month', notice_date)
         ORDER BY date_trunc('month', notice_date)`
      ),
      pool.query<{ category: string }>(
        `SELECT category
         FROM homepage_signals
         WHERE notice_date > NOW() - INTERVAL '12 months'
           AND notice_date <= NOW()
           AND value_amount IS NOT NULL
           AND value_amount > 0
         GROUP BY category
         ORDER BY SUM(value_amount) DESC
         LIMIT 1`
      ),
    ]);
    return {
      points: r.rows,
      illustrative: r.rows.length < 3,
      topDesk: topR.rows[0]?.category ?? null,
    };
  }
  return { points: [], illustrative: true, topDesk: null };
}

const DESK_CACHE_TTL_MS = 24 * 60 * 60 * 1000;

async function getDeskCache(slug: string): Promise<{ data: ProcurementData; cached_at: string } | null> {
  if (pool) {
    const r = await pool.query<{ data: ProcurementData; cached_at: string }>(
      `SELECT data, cached_at::text FROM desk_cache WHERE slug = $1 AND cached_at > NOW() - INTERVAL '24 hours'`,
      [slug]
    );
    return r.rows[0] || null;
  }
  const mem = deskCacheMemStore.get(slug);
  if (!mem) return null;
  if (Date.now() - new Date(mem.cached_at).getTime() > DESK_CACHE_TTL_MS) return null;
  return mem;
}

async function setDeskCache(slug: string, data: ProcurementData): Promise<void> {
  const now = nowIso();
  if (pool) {
    await pool.query(
      `INSERT INTO desk_cache (slug, data, cached_at) VALUES ($1, $2, $3)
       ON CONFLICT (slug) DO UPDATE SET data = EXCLUDED.data, cached_at = EXCLUDED.cached_at`,
      [slug, JSON.stringify(data), now]
    );
    return;
  }
  deskCacheMemStore.set(slug, { data, cached_at: now });
}

async function compileDeskInBackground(profile: DeskProfile): Promise<void> {
  if (compilingDesks.has(profile.slug)) return;
  compilingDesks.add(profile.slug);
  try {
    console.log(`[desk] compiling ${profile.slug}`);
    const data = await pullProcurementData(profile.pinnedProfile);
    await setDeskCache(profile.slug, data);
    console.log(`[desk] compiled ${profile.slug} — ${data.contractsFinder.open.length} open, ${data.contractsFinder.awarded.length} awarded`);
  } catch (err: any) {
    console.error(`[desk] compile failed for ${profile.slug}: ${err?.message}`);
    captureError(err, { desk: { slug: profile.slug } });
  } finally {
    compilingDesks.delete(profile.slug);
  }
}

async function createScan(input: z.infer<typeof intakeSchema>): Promise<ScanRecord> {
  const record: ScanRecord = {
    id: makeId(),
    created_at: nowIso(),
    updated_at: nowIso(),
    status: "pending",
    company_name: input.companyName,
    input_json: input,
    procurement_json: null,
    report_markdown: null,
    error_message: null,
    pdf_storage_key: null,
    pdf_storage_url: null,
    pdf_storage_etag: null,
    pdf_storage_updated_at: null
  };

  if (pool) {
    await pool.query(
      `INSERT INTO scans (id, created_at, updated_at, status, company_name, input_json, procurement_json, report_markdown, error_message)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [
        record.id,
        record.created_at,
        record.updated_at,
        record.status,
        record.company_name,
        record.input_json,
        record.procurement_json,
        record.report_markdown,
        record.error_message
      ]
    );
  } else {
    memoryStore.set(record.id, record);
  }

  return record;
}

async function getScan(id: string): Promise<ScanRecord | null> {
  if (pool) {
    const result = await pool.query(`SELECT * FROM scans WHERE id=$1`, [id]);
    return result.rows[0] || null;
  }

  return memoryStore.get(id) || null;
}

async function updateScan(id: string, patch: Partial<ScanRecord>) {
  const current = await getScan(id);
  if (!current) return;

  const next = { ...current, ...patch, updated_at: nowIso() };

  if (pool) {
    await pool.query(
      `UPDATE scans
       SET updated_at=$2, status=$3, procurement_json=$4, report_markdown=$5, error_message=$6
       WHERE id=$1`,
      [id, next.updated_at, next.status, next.procurement_json, next.report_markdown, next.error_message]
    );
  } else {
    memoryStore.set(id, next);
  }
}

async function emitScanStage(id: string, stage: string): Promise<void> {
  if (pool) {
    await pool.query(`UPDATE scans SET progress_stage=$2, updated_at=$3 WHERE id=$1`, [id, stage, nowIso()])
      .catch(() => {});
  } else {
    const s = memoryStore.get(id);
    if (s) memoryStore.set(id, { ...s, progress_stage: stage, updated_at: nowIso() });
  }
  scanEvents.emit(`scan:${id}`, stage);
}

async function getScansByCompany(companyName: string, excludeId: string): Promise<ScanRecord[]> {
  if (pool) {
    const r = await pool.query<ScanRecord>(
      `SELECT * FROM scans WHERE company_name=$1 AND id!=$2 AND status='completed' ORDER BY created_at DESC LIMIT 10`,
      [companyName, excludeId]
    );
    return r.rows;
  }
  return [...memoryStore.values()]
    .filter(s => s.company_name === companyName && s.id !== excludeId && s.status === "completed")
    .sort((a, b) => b.created_at.localeCompare(a.created_at))
    .slice(0, 10);
}

async function updateScanPdfStorage(
  id: string,
  storage: Pick<ScanRecord, "pdf_storage_key" | "pdf_storage_url" | "pdf_storage_etag">
) {
  const updatedAt = nowIso();

  if (pool) {
    await pool.query(
      `UPDATE scans
       SET pdf_storage_key=$2, pdf_storage_url=$3, pdf_storage_etag=$4, pdf_storage_updated_at=$5
       WHERE id=$1`,
      [id, storage.pdf_storage_key, storage.pdf_storage_url, storage.pdf_storage_etag, updatedAt]
    );
    return;
  }

  const current = await getScan(id);
  if (!current) return;

  memoryStore.set(id, {
    ...current,
    ...storage,
    pdf_storage_updated_at: updatedAt
  });
}

async function listScans(): Promise<ScanRecord[]> {
  if (pool) {
    const result = await pool.query(`SELECT * FROM scans ORDER BY created_at DESC LIMIT 100`);
    return result.rows;
  }

  return Array.from(memoryStore.values()).sort((a, b) =>
    b.created_at.localeCompare(a.created_at)
  );
}

async function deleteScan(id: string) {
  if (pool) {
    await pool.query(`DELETE FROM scans WHERE id=$1`, [id]);
    return;
  }

  memoryStore.delete(id);
}

async function createSubscription(
  scanId: string,
  email: string,
  input: z.infer<typeof intakeSchema>,
  companyName: string
): Promise<SubscriptionRecord> {
  const record: SubscriptionRecord = {
    id: `sub_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    scan_id: scanId,
    company_name: companyName,
    email,
    input_json: input,
    alerted_notice_ids: [],
    active: true,
    created_at: nowIso(),
    last_alerted_at: null
  };

  if (pool) {
    await pool.query(
      `INSERT INTO subscriptions (id, scan_id, company_name, email, input_json, alerted_notice_ids, active, created_at, last_alerted_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [record.id, record.scan_id, record.company_name, record.email, record.input_json,
       record.alerted_notice_ids, record.active, record.created_at, record.last_alerted_at]
    );
  } else {
    subMemStore.set(record.id, record);
  }
  return record;
}

async function getSubscription(id: string): Promise<SubscriptionRecord | null> {
  if (pool) {
    const result = await pool.query(`SELECT * FROM subscriptions WHERE id=$1`, [id]);
    return result.rows[0] || null;
  }
  return subMemStore.get(id) || null;
}

async function updateSubscriptionAlerted(id: string, noticeIds: string[]) {
  const now = nowIso();
  if (pool) {
    await pool.query(
      `UPDATE subscriptions SET alerted_notice_ids=$2, last_alerted_at=$3 WHERE id=$1`,
      [id, noticeIds, now]
    );
  } else {
    const sub = subMemStore.get(id);
    if (sub) subMemStore.set(id, { ...sub, alerted_notice_ids: noticeIds, last_alerted_at: now });
  }
}

async function deactivateSubscription(id: string) {
  if (pool) {
    await pool.query(`UPDATE subscriptions SET active=FALSE WHERE id=$1`, [id]);
  } else {
    const sub = subMemStore.get(id);
    if (sub) subMemStore.set(id, { ...sub, active: false });
  }
}

async function listAllSubscriptions(): Promise<SubscriptionRecord[]> {
  if (pool) {
    const result = await pool.query(`SELECT * FROM subscriptions ORDER BY created_at DESC LIMIT 200`);
    return result.rows;
  }
  return Array.from(subMemStore.values()).sort((a, b) => b.created_at.localeCompare(a.created_at));
}

function requireAdmin(req: express.Request, res: express.Response, next: express.NextFunction) {
  const token = req.header("x-admin-token") || String(req.query.token || "");
  if (token !== ADMIN_TOKEN) {
    res.status(401).json({ error: "Unauthorized" });
    return;
  }
  next();
}

// ── Auth helpers ─────────────────────────────────────────────────────────────

function signToken(user: Pick<UserRecord, "id" | "email" | "tier">): string {
  return jwt.sign({ userId: user.id, email: user.email, tier: user.tier }, JWT_SECRET, { expiresIn: "30d" });
}

function getAuthUser(req: express.Request): { userId: string; email: string; tier: UserTier } | null {
  const token = req.cookies?.gr_token;
  if (!token) return null;
  try {
    const p = jwt.verify(token, JWT_SECRET) as { userId: string; email: string; tier: string };
    return { userId: p.userId, email: p.email, tier: p.tier as UserTier };
  } catch {
    return null;
  }
}

function requireAuth(req: express.Request, res: express.Response, next: express.NextFunction) {
  if (!getAuthUser(req)) { res.redirect("/login"); return; }
  next();
}

async function getUserById(id: string): Promise<UserRecord | null> {
  if (!pool) return null;
  const r = await pool.query<UserRecord>(`SELECT * FROM users WHERE id=$1`, [id]);
  return r.rows[0] || null;
}

async function getUserByEmail(email: string): Promise<UserRecord | null> {
  if (!pool) return null;
  const r = await pool.query<UserRecord>(`SELECT * FROM users WHERE email=$1`, [email]);
  return r.rows[0] || null;
}

async function createUser(email: string, password: string): Promise<UserRecord> {
  const id = makeId();
  const hash = await bcrypt.hash(password, 10);
  if (pool) {
    const r = await pool.query<UserRecord>(
      `INSERT INTO users (id, email, password_hash, tier) VALUES ($1,$2,$3,'free') RETURNING *`,
      [id, email.toLowerCase().trim(), hash]
    );
    return r.rows[0];
  }
  throw new Error("Database required for user accounts");
}

async function updateUserTier(userId: string, tier: UserTier, stripeCustomerId?: string, stripeSubId?: string, stripeSubStatus?: string) {
  if (!pool) return;
  await pool.query(
    `UPDATE users SET tier=$2, stripe_customer_id=COALESCE($3,stripe_customer_id), stripe_subscription_id=COALESCE($4,stripe_subscription_id), stripe_subscription_status=COALESCE($5,stripe_subscription_status) WHERE id=$1`,
    [userId, tier, stripeCustomerId ?? null, stripeSubId ?? null, stripeSubStatus ?? null]
  );
}

async function updateScanCachedField(id: string, field: "capability_statement" | "outreach_emails" | "frameworks_assessment", value: string) {
  if (pool) {
    await pool.query(`UPDATE scans SET ${field}=$2 WHERE id=$1`, [id, value]);
  }
}

// ── Auth CSS (shared across login/register/account pages) ────────────────────
const authCss = `
  @import url('https://fonts.googleapis.com/css2?family=Newsreader:ital,opsz,wght@0,6..72,400;0,6..72,500;1,6..72,400&family=Libre+Franklin:wght@400;500;600;700&family=Spline+Sans+Mono:wght@400;500;600&display=swap');
  :root{
    --base:#ECE7DA;--surface:#FBF9F3;--surface-2:#F6F2E8;
    --brand:#B4924E;--brand-dim:rgba(180,146,78,.12);
    --green:#1d6b4f;--red:#9b2d20;
    --text:#1B1E19;--muted:#86897E;--faint:#9AA093;
    --border:rgba(27,30,25,.10);--border-2:rgba(27,30,25,.16);
    --sans:"Libre Franklin",system-ui,sans-serif;
    --serif:"Newsreader",Georgia,serif;
    --mono:"Spline Sans Mono",ui-monospace,monospace;
  }
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--base);color:var(--text);font-family:var(--sans);-webkit-font-smoothing:antialiased;min-height:100vh;display:flex;flex-direction:column}
  .auth-nav{background:rgba(236,231,218,0.92);backdrop-filter:blur(10px);border-bottom:1px solid var(--border-2);padding:0 32px;height:60px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:50}
  .auth-nav-brand{display:flex;align-items:center;gap:9px}
  .auth-nav-dot{width:10px;height:10px;background:var(--brand);border-radius:50%}
  .auth-nav-logo{font-family:var(--serif);font-size:20px;font-weight:500;color:var(--text);text-decoration:none}
  .auth-nav a{color:var(--muted);text-decoration:none;font-size:13px;font-weight:500}
  .auth-wrap{flex:1;display:flex;align-items:center;justify-content:center;padding:48px 16px}
  .auth-card{background:var(--surface);border:1px solid var(--border-2);padding:44px;max-width:440px;width:100%}
  .auth-card-label{font-family:var(--mono);font-size:11px;letter-spacing:0.18em;text-transform:uppercase;color:var(--brand);margin-bottom:14px}
  .auth-card h1{font-family:var(--serif);font-size:28px;font-weight:400;letter-spacing:-0.01em;margin-bottom:8px;color:var(--text)}
  .auth-card p.sub{color:var(--muted);font-size:14px;line-height:1.55;margin-bottom:32px}
  .field{margin-bottom:22px}
  .field label{display:block;font-family:var(--mono);font-size:11px;letter-spacing:0.16em;text-transform:uppercase;color:#5C6157;margin-bottom:9px}
  .field input{width:100%;padding:13px 15px;border:1px solid var(--border-2);font-size:15px;background:#FBF9F3;color:var(--text);outline:none;font-family:var(--sans)}
  .field input:focus{border-color:var(--brand)}
  .btn-primary{width:100%;background:#102A1E;color:#F3EFE6;border:none;padding:15px;font-size:15px;font-weight:600;cursor:pointer;letter-spacing:0.01em;font-family:var(--sans)}
  .btn-primary:hover{background:#0A1C12}
  .auth-alt{text-align:center;margin-top:20px;font-size:13px;color:var(--muted)}
  .auth-alt a{color:var(--brand);font-weight:600;text-decoration:none}
  .err{background:rgba(155,45,32,.06);border:1px solid rgba(155,45,32,.22);color:#9b2d20;padding:11px 14px;font-size:13px;margin-bottom:20px}
  .ok{background:rgba(29,107,79,.06);border:1px solid rgba(29,107,79,.22);color:#1d6b4f;padding:11px 14px;font-size:13px;margin-bottom:20px}
`;

function clientEmailFromInput(input: z.infer<typeof intakeSchema>) {
  return input.clientEmail || input.email || null;
}


function keywordCoreTokens(keywords: string[]) {
  const stop = new Set([
    "and", "the", "for", "with", "from", "into", "services", "service", "consultancy",
    "consultant", "management", "project", "programme", "public", "sector"
  ]);

  return Array.from(
    new Set(
      keywords
        .join(" ")
        .toLowerCase()
        .split(/[^a-z0-9]+/)
        .map(token => token.trim())
        .filter(token => token.length >= 4 && !stop.has(token))
    )
  );
}

function scoreNoticeRelevance(notice: ProcurementNotice, keywords: string[]) {
  const searchable = [
    notice.title,
    notice.buyer,
    notice.description,
    notice.type,
    notice.region,
    notice.awardedSupplier
  ]
    .join(" ")
    .toLowerCase();

  let score = 15;
  const exactMatches: string[] = [];
  const tokenMatches: string[] = [];

  for (const keyword of keywords) {
    const clean = keyword.toLowerCase().trim();
    if (clean && searchable.includes(clean)) {
      exactMatches.push(keyword);
      score += 22;
    }
  }

  for (const token of keywordCoreTokens(keywords)) {
    if (searchable.includes(token)) {
      tokenMatches.push(token);
      score += 8;
    }
  }

  if (notice.suitableForSme === true) score += 6;
  if (notice.valueHigh && notice.valueHigh >= 25000) score += 4;
  if (notice.valueHigh && notice.valueHigh >= 100000) score += 5;

  score = clampScore(score);

  const reason =
    exactMatches.length > 0
      ? `Direct phrase match: ${Array.from(new Set(exactMatches)).slice(0, 4).join(", ")}`
      : tokenMatches.length > 0
        ? `Partial keyword overlap: ${Array.from(new Set(tokenMatches)).slice(0, 5).join(", ")}`
        : "Weak keyword overlap; treat as a broad market signal, not a direct opportunity.";

  return { score, reason };
}

function enrichNoticeQuality(notice: ProcurementNotice, keywords: string[]) {
  const relevance = scoreNoticeRelevance(notice, keywords);

  return {
    ...notice,
    sourceConfidence: "Pulled record",
    relevanceScore: relevance.score,
    relevanceReason: relevance.reason
  };
}

function dataQualitySummary(open: ProcurementNotice[], awarded: ProcurementNotice[], errors: string[], keywords: string[], regions: string) {
  const all = [...open, ...awarded];
  const total = all.length;
  const strong = all.filter(item => (item.relevanceScore || 0) >= 65).length;
  const moderate = all.filter(item => (item.relevanceScore || 0) >= 45).length;
  const average =
    total === 0
      ? 0
      : Math.round(all.reduce((sum, item) => sum + (item.relevanceScore || 0), 0) / total);

  // Use relevant-record counts, not averageRelevance — CPV noise tanks the average
  // even when genuine strong matches exist.
  const relevantRatio = total === 0 ? 0 : moderate / total;

  let level = "Weak";
  let warning = "The data pull returned limited or noisy matches. Treat named buyer suggestions as strategy targets unless linked to pulled records or verified source URLs.";

  if (total === 0) {
    level = "Critical";
    warning = "No structured Contracts Finder records were returned for this scan. The report must rely on strategy mapping and verified web facts only.";
  } else if (strong >= 8 && relevantRatio >= 0.25) {
    level = "Strong";
    warning = "The structured data pull returned several relevant records. Pulled records can be used as source-backed market signals.";
  } else if (strong >= 3 || (moderate >= 10 && relevantRatio >= 0.15)) {
    level = "Moderate";
    warning = "The structured data pull returned some useful records, but not every buyer or supplier in the report should be treated as confirmed.";
  }

  if (errors.length > 0) {
    warning += " Some API searches failed; use the errors list before making commercial decisions.";
  }

  return {
    level,
    warning,
    totalRecords: total,
    strongMatches: strong,
    moderateMatches: moderate,
    averageRelevance: average,
    keywords,
    regions,
    errors
  };
}

async function generateSearchKeywords(input: z.infer<typeof intakeSchema>): Promise<string[]> {
  const prompt = `You are a UK public-sector procurement specialist helping to find relevant contracts on Contracts Finder.

Given this company intake, return exactly 6–8 keyword search phrases to use as Contracts Finder search terms.

Rules:
- Use 2–4 word phrases that Contracts Finder would return genuine contract matches for
- Match the company's actual core services precisely — do not invent or assume sectors
- Do not use company names, buyer names, or location names
- Focus on contract and tender terminology (what councils and housing associations would call the service in a tender)
- If the company does social housing repairs, use housing maintenance terms — not property surveying terms
- If the company does cleaning, use cleaning terms — not facilities management terms
- Return ONLY valid JSON: { "keywords": ["phrase 1", "phrase 2", ...] }

Company intake:
Company: ${input.companyName}
Main services: ${input.mainServices}
Secondary services: ${input.secondaryServices || "none"}
Ideal buyers: ${input.idealBuyers || "public sector"}
Main goal: ${input.mainGoal || "win public sector contracts"}
Framework access: ${input.frameworkStatus || "none stated"}
Last public contract: ${input.lastPublicContract || "none stated"}`;

  const response = await withOpenAiTimeout(signal =>
    openai.chat.completions.create({
      model: "gpt-4.1-mini",
      messages: [{ role: "user", content: prompt }],
      response_format: { type: "json_object" },
      temperature: 0.1,
      max_tokens: 300
    }, { signal })
  );

  const raw = response.choices[0]?.message?.content ?? "{}";
  const parsed = JSON.parse(raw);
  const keywords: unknown = parsed.keywords ?? parsed.terms ?? Object.values(parsed)[0];

  if (!Array.isArray(keywords) || keywords.length === 0) {
    throw new Error("LLM returned no keywords");
  }

  return (keywords as unknown[])
    .filter((k): k is string => typeof k === "string" && k.length >= 3 && k.length <= 80)
    .slice(0, 8);
}

// CPV codes are supplementary — kept tight to avoid noisy cross-sector matches.
// 45000000 (general construction) and 79000000 (all business services) are intentionally
// excluded: too broad, they pull new-build and unrelated contracts.
const SECTOR_CPV: Record<string, string[]> = {
  "social-housing":    ["50700000", "45453100", "45262700"],  // maintenance, renovation, conversion
  "cleaning":          ["90910000", "90911000", "90919000", "90920000"],
  "facilities":        ["50700000", "79993000"],               // maintenance + facilities mgmt only
  "built-environment": ["71315300", "71315200", "45453100"],   // building surveying, testing, renovation
  "creative":          ["79342200", "79952000"],               // promotional, events
  "photography":       ["79962000"],
  "energy":            ["71314000", "50710000"],               // energy services, heating/ventilation maintenance
  "health":            ["85100000", "85110000", "85140000", "85120000"],
  "digital":          ["72000000", "48000000", "72212000"],   // IT services, software, programming
  "social-care":      ["85311000", "85312000", "85320000"],   // social work with/without accommodation, social services
  "childrens":        ["85312000", "85311200", "85321000"],   // social work without accommodation, child day care, admin social services
  "waste":            ["90500000", "90511000", "90600000"],   // waste collection, street cleaning
  "security":         ["79710000", "79711000", "79715000"],   // security services, alarm monitoring, patrol
  "catering":         ["55500000", "55520000", "55523000"],   // canteen/catering, catering outside premises, catering for schools
  "legal":            ["79100000", "79200000", "79212000"],   // legal services, accountancy, auditing
  "housing-support":  ["85311000", "70220000"],               // social work, commercial property letting (supported housing)
  "finance":          ["79200000", "66100000", "79212000"],   // accountancy, banking, auditing
  "comms":            ["79340000", "79341000", "79960000"],   // advertising, general advertising, photography services
  "leisure":          ["92000000", "92600000", "92610000"],   // recreational, sporting, sports grounds
  "planning":         ["71400000", "71410000", "71420000"],   // urban planning, regional planning, landscape
  "justice":          ["75231200", "75231210", "79997000"],   // rehabilitation, community service, business travel arrangements
  "emergency":        ["75250000", "75252000", "35110000"],   // fire/rescue, ambulance, firefighting equipment
  "research":         ["73100000", "73200000", "72316000"],   // R&D, business consultancy, data analysis
  "consulting":       ["73200000", "72224000", "72220000"],   // business consultancy, project management consulting, systems/tech consulting
};

async function pullProcurementData(input: z.infer<typeof intakeSchema>, signal?: AbortSignal): Promise<ProcurementData> {
  let keywords: string[];
  try {
    keywords = await generateSearchKeywords(input);
    console.log(`[keywords] LLM generated: ${keywords.join(", ")}`);
  } catch (err: any) {
    console.warn(`[keywords] LLM failed, using static fallback: ${err?.message}`);
    keywords = buildKeywords(input);
  }
  const regions = buildRegion(input);
  const open: ProcurementNotice[] = [];
  const awarded: ProcurementNotice[] = [];
  const errors: string[] = [];

  const now = new Date();
  // Open notices: only pull from last 90 days so live signal stays fresh
  const openPublishedFrom = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  // Awarded notices: last 18 months for meaningful buyer/value history
  const awardedDateFrom = new Date(now.getTime() - 548 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

  const staticCriteria = {
    keyword: null as string | null,
    queryString: null,
    regions,
    postcode: null,
    radius: 0,
    valueFrom: null,
    valueTo: null,
    publishedFrom: null,
    publishedTo: null,
    deadlineFrom: null,
    deadlineTo: null,
    approachMarketFrom: null,
    approachMarketTo: null,
    awardedFrom: null,
    awardedTo: null,
    isSubcontract: null,
    suitableForSme: true,
    suitableForVco: null,
    awardedToSme: null,
    awardedToVcse: null,
    cpvCodes: null as string[] | null
  };

  for (const keyword of keywords) {
    const base = { ...staticCriteria, keyword };

    try {
      open.push(...(await contractsFinderSearchAll(
        { ...base, types: ["Contract"], statuses: ["Open"], publishedFrom: openPublishedFrom },
        keyword,
        signal
      )));
    } catch (error: any) {
      if ((error as any)?.name === "AbortError") throw error;
      captureError(error, { dataPull: { source: "contracts_finder", status: "open", keyword } });
      errors.push(`Open search failed for "${keyword}": ${error?.message || error}`);
    }

    try {
      awarded.push(...(await contractsFinderSearchAll(
        { ...base, types: ["Contract"], statuses: ["Awarded"], awardedFrom: awardedDateFrom },
        keyword,
        signal
      )));
    } catch (error: any) {
      if ((error as any)?.name === "AbortError") throw error;
      captureError(error, { dataPull: { source: "contracts_finder", status: "awarded", keyword } });
      errors.push(`Awarded search failed for "${keyword}": ${error?.message || error}`);
    }
  }

  // CPV-code parallel pass — catches notices that keyword search misses
  const cpvCodes = SECTOR_CPV[resolveSectorFromInput(input).key] ?? null;
  if (cpvCodes) {
    const cpvBase = { ...staticCriteria, cpvCodes };
    try {
      open.push(...(await contractsFinderSearchAll(
        { ...cpvBase, types: ["Contract"], statuses: ["Open"], publishedFrom: openPublishedFrom },
        "cpv",
        signal
      )));
    } catch (error: any) {
      if ((error as any)?.name === "AbortError") throw error;
      errors.push(`CPV open search failed: ${error?.message || error}`);
    }
    try {
      awarded.push(...(await contractsFinderSearchAll(
        { ...cpvBase, types: ["Contract"], statuses: ["Awarded"], awardedFrom: awardedDateFrom },
        "cpv",
        signal
      )));
    } catch (error: any) {
      if ((error as any)?.name === "AbortError") throw error;
      errors.push(`CPV awarded search failed: ${error?.message || error}`);
    }
  }

  const companiesHouse = await companiesHouseSearch(input.companyName);
  if (companiesHouse.errors.length) {
    for (const error of companiesHouse.errors) {
      errors.push(`Companies House: ${error}`);
    }
  }

  const findTender = await findTenderSearch(keywords, signal);
  if (findTender.errors.length) {
    for (const error of findTender.errors) {
      errors.push(`Find a Tender: ${error}`);
    }
  }

  const finalOpen = dedupeNotices(open).map(notice => enrichNoticeQuality(notice, keywords));
  const finalAwarded = dedupeNotices(awarded).map(notice => enrichNoticeQuality(notice, keywords));
  const quality = dataQualitySummary(
    finalOpen,
    [...finalAwarded, ...findTender.notices],
    errors,
    keywords,
    regions
  );

  return {
    generatedAt: nowIso(),
    quality,
    keywords,
    regions,
    companiesHouse,
    findTender,
    contractsFinder: {
      open: finalOpen,
      awarded: finalAwarded,
      errors
    }
  };
}

function procurementDataMarkdown(data: ProcurementData) {
  const open = [...data.contractsFinder.open]
    .sort((a, b) => (b.relevanceScore ?? 0) - (a.relevanceScore ?? 0))
    .slice(0, 20);
  const awarded = [...data.contractsFinder.awarded]
    .sort((a, b) => (b.relevanceScore ?? 0) - (a.relevanceScore ?? 0))
    .slice(0, 20);
  const findTender = [...(data.findTender?.notices || [])]
    .sort((a, b) => new Date(b.publishedDate || 0).getTime() - new Date(a.publishedDate || 0).getTime())
    .slice(0, 20);
  const companiesHouse = data.companiesHouse?.matches || [];
  const allErrors = [
    ...(data.contractsFinder.errors || []).map(error => `Contracts Finder: ${error}`),
    ...(data.findTender?.errors || []).map(error => `Find a Tender: ${error}`),
    ...(data.companiesHouse?.errors || []).map(error => `Companies House: ${error}`)
  ];

  const quality = data.quality || dataQualitySummary(
    open,
    [...awarded, ...findTender],
    allErrors,
    data.keywords,
    data.regions
  );

  const renderRows = (items: ProcurementNotice[], emptyLabel: string) =>
    items.length
      ? items
          .map(
            item =>
              `- [Pulled record: ${item.source}] ${item.title} | Buyer: ${item.buyer} | Status: ${item.status || "-"} | Relevance: ${item.relevanceScore ?? "-"} /100 | Reason: ${item.relevanceReason || "-"} | Value: ${formatMoney(
                item.valueLow || item.awardedValue
              )}-${formatMoney(item.valueHigh || item.awardedValue)} | Deadline: ${formatDate(
                item.deadlineDate
              )} | Awarded supplier: ${item.awardedSupplier || "-"} | SME: ${
                item.suitableForSme === null ? "-" : item.suitableForSme ? "yes" : "no"
              } | Source URL: ${item.url}`
          )
          .join("\n")
      : `- No matching structured records returned from ${emptyLabel} for this scan.`;

  const companyRows = companiesHouse.length
    ? companiesHouse
        .map(
          company =>
            `- [Pulled record: Companies House] ${company.companyName} | Company number: ${company.companyNumber} | Status: ${company.companyStatus || "-"} | Type: ${company.companyType || "-"} | Created: ${company.dateOfCreation || "-"} | SIC: ${company.sicCodes.length ? company.sicCodes.join(", ") : "-"} | Address: ${company.address || "-"} | Source URL: ${company.url}`
        )
        .join("\n")
    : "- No matching Companies House company profile returned for this intake company name.";

  return `
STRUCTURED PROCUREMENT AND COMPANY DATA PULLED BEFORE ANALYSIS

Source confidence labels allowed in this report:
- Pulled record = record returned directly from Contracts Finder, Find a Tender or Companies House and includes Source URL.
- Verified web fact = fact verified by web search and listed with a URL in Source Appendix.
- Strategy target = commercially relevant buyer/supplier/opportunity suggested by analyst logic, but not confirmed as a pulled record.
- Unconfirmed = mentioned in intake or plausible from context but not verified from sources checked.

Data quality warning:
Level: ${quality.level}
Warning: ${quality.warning}
Average relevance: ${quality.averageRelevance}/100
Strong matches: ${quality.strongMatches}
Moderate matches: ${quality.moderateMatches}
Total structured records: ${quality.totalRecords}

Data generated at: ${data.generatedAt}
Sources: Contracts Finder API v2 search_notices; Find a Tender public OCDS release packages; Companies House search/companies
Regions searched: ${data.regions}
Keywords searched: ${data.keywords.join(", ")}

Companies House company profile matches:
${companyRows}

Contracts Finder open opportunities:
${renderRows(open, "Contracts Finder open opportunities")}

Contracts Finder awarded / historical signals:
${renderRows(awarded, "Contracts Finder awarded records")}

Find a Tender public OCDS notices:
${renderRows(findTender, "Find a Tender")}

Data pull errors:
${allErrors.length ? allErrors.map(error => `- ${error}`).join("\n") : "- None"}
`;
}



function enforceDataQualityLanguage(report: string) {
  return String(report || "")
    .replace(/\bConfirmed\b/g, "Source-labelled")
    .replace(/\bconfirmed\b/g, "source-labelled")
    .replace(/\bsource-backed\b/gi, "source-labelled")
    .replace(/\bSource-backed\b/g, "Source-labelled");
}



function trustNumber(value: number) {
  return new Intl.NumberFormat("en-GB").format(value || 0);
}

function trustMoney(value: number) {
  if (!value || Number.isNaN(value)) return "Not stated";
  return new Intl.NumberFormat("en-GB", {
    style: "currency",
    currency: "GBP",
    maximumFractionDigits: 0
  }).format(value);
}

function parseMoneyCap(input: any) {
  const text = [
    input?.maximumContractSize,
    input?.idealContractSize,
    input?.mainServices,
    input?.mainGoal
  ]
    .join(" ")
    .toLowerCase();

  // Require £ prefix OR explicit k/m/million/thousand suffix to avoid matching bare years/counts
  const matches = [...text.matchAll(/£\s*([0-9,]+(?:\.[0-9]+)?)\s*(k|m|million|thousand)?|([0-9,]+(?:\.[0-9]+)?)\s*(k|m|million|thousand)/gi)];
  const values = matches
    .map(match => {
      const n = parseFloat((match[1] || match[3]).replace(/,/g, ""));
      const unit = (match[2] || match[4] || "").toLowerCase();
      if (unit === "m" || unit === "million") return n * 1_000_000;
      if (unit === "k" || unit === "thousand") return n * 1_000;
      return n;
    })
    .filter(value => value >= 1_000);

  if (values.length) return Math.max(...values);

  if (text.includes("quantity surveying") || text.includes("construction") || text.includes("cost management") || text.includes("project management")) {
    return 500_000;
  }

  if (text.includes("marketing") || text.includes("campaign") || text.includes("video") || text.includes("film") || text.includes("creative")) {
    return 150_000;
  }

  if (text.includes("photography")) {
    return 25_000;
  }

  return 75_000;
}

type SectorResult = { key: string; label: string; terms: string[] };

// Single canonical sector resolver. Check order matters: social-care and social-housing
// must win before health/facilities to prevent ICB-buyer false reclassification.
function resolveSector(text: string): SectorResult {
  const t = text.toLowerCase();

  if (t.includes("adult social care") || t.includes("domiciliary") || t.includes("domiciliary care") ||
      t.includes("residential care") || t.includes("care services") || t.includes("personal care") ||
      t.includes("home care") || t.includes("homecare") || t.includes("care provider") ||
      t.includes("reablement") || t.includes("learning disability") || t.includes("care home") ||
      t.includes("supported living") || t.includes("community care") || t.includes("care worker")) {
    return {
      key: "social-care",
      label: "Adult social care",
      terms: ["adult social care", "domiciliary care", "residential care", "learning disability", "reablement", "supported living", "nursing home", "care services", "personal care", "home care", "care provider"]
    };
  }

  if (t.includes("nhs") || t.includes("integrated care board") || t.includes("integrated care system") ||
      t.includes("clinical commissioning") || t.includes("health trust") || t.includes("icb") ||
      t.includes("mental health service") || t.includes("community health") || t.includes("primary care") ||
      t.includes("public health commissioning") || t.includes("gp service") ||
      t.includes("healthcare commissioning") || t.includes("nhs england")) {
    return {
      key: "health",
      label: "Health & NHS commissioning",
      terms: ["NHS", "integrated care board", "clinical commissioning", "mental health", "community health", "primary care", "public health", "GP services", "healthcare", "NHS trust", "ICB", "health commissioning"]
    };
  }

  if (t.includes("social housing") || t.includes("housing maintenance") ||
      t.includes("responsive repairs") || t.includes("void") ||
      t.includes("damp and mould") || t.includes("tenancy")) {
    return {
      key: "social-housing",
      label: "Social housing / housing maintenance",
      terms: ["social housing", "housing maintenance", "responsive repairs", "void properties", "damp and mould", "tenancy", "housing association", "council housing"]
    };
  }

  if (t.includes("cleaning") || t.includes("deep clean") || t.includes("hygiene") ||
      t.includes("clinical") || t.includes("domestic cleaning") || t.includes("commercial cleaning")) {
    return {
      key: "cleaning",
      label: "Specialist cleaning / facilities hygiene",
      terms: ["cleaning", "contract cleaning", "deep clean", "clinical cleaning", "domestic cleaning", "commercial cleaning", "hygiene", "facilities cleaning"]
    };
  }

  if (t.includes("facilities") || t.includes(" fm ") || t.includes("soft fm") ||
      t.includes("hard fm") || t.includes("property services") || t.includes("maintenance")) {
    return {
      key: "facilities",
      label: "Facilities management / property services",
      terms: ["facilities management", "fm services", "soft fm", "hard fm", "maintenance", "property services", "estates management"]
    };
  }

  if (t.includes("construction") || t.includes("quantity surveying") ||
      t.includes("cost management") || t.includes("employer") ||
      t.includes("building surveying") || t.includes("estate")) {
    return {
      key: "built-environment",
      label: "Built environment / construction consultancy",
      terms: ["construction", "quantity surveying", "cost management", "project management", "employer", "building surveying", "estate", "asset management", "contract administration", "programme management"]
    };
  }

  if (t.includes("marketing") || t.includes("creative") || t.includes("campaign") ||
      t.includes("video") || t.includes("film") || t.includes("communications") ||
      t.includes("event production") || t.includes("drpg")) {
    return {
      key: "creative",
      label: "Creative / marketing production / events",
      terms: ["marketing", "creative", "campaign", "video", "film", "communications", "event", "production", "digital content", "media services"]
    };
  }

  if (t.includes("photography") || t.includes("portrait") || t.includes("graduation") ||
      t.includes("property photography") || t.includes("wedding")) {
    return {
      key: "photography",
      label: "Photography / visual content / public communications",
      terms: ["photography", "event photography", "corporate photography", "graduation", "portrait", "property photography", "visual content", "creative services"]
    };
  }

  if (t.includes("retrofit") || t.includes("solar") || t.includes("energy") ||
      t.includes("decarbonisation") || t.includes("net zero")) {
    return {
      key: "energy",
      label: "Energy / retrofit / built-environment decarbonisation",
      terms: ["energy", "retrofit", "solar", "decarbonisation", "net zero", "energy efficiency", "low carbon", "sustainability"]
    };
  }

  if (t.includes("it services") || t.includes("software") || t.includes("digital") ||
      t.includes("cloud") || t.includes("cyber") || t.includes("g-cloud") ||
      t.includes("data analytics") || t.includes("it infrastructure") || t.includes("saas")) {
    return {
      key: "digital",
      label: "Digital, IT & technology",
      terms: ["IT services", "software", "digital transformation", "cloud", "cyber security", "G-Cloud", "data analytics", "infrastructure", "SaaS", "ICT"]
    };
  }

  if (t.includes("children") || t.includes("fostering") || t.includes("camhs") ||
      t.includes("early years") || t.includes("looked after") || t.includes("youth services") ||
      t.includes("young people") || t.includes("adoption")) {
    return {
      key: "childrens",
      label: "Children's services",
      terms: ["children's services", "fostering", "adoption", "CAMHS", "early years", "looked after children", "youth services", "safeguarding", "short breaks"]
    };
  }

  if (t.includes("waste management") || t.includes("refuse") || t.includes("recycling") ||
      t.includes("street cleansing") || t.includes("grounds maintenance") || t.includes("waste collection")) {
    return {
      key: "waste",
      label: "Waste, environment & grounds",
      terms: ["waste management", "refuse collection", "recycling", "street cleansing", "grounds maintenance", "composting", "environmental monitoring"]
    };
  }

  if (t.includes("security guard") || t.includes("manned guarding") || t.includes("cctv") ||
      t.includes("access control") || t.includes("event security") || t.includes("lone worker")) {
    return {
      key: "security",
      label: "Security services",
      terms: ["security guarding", "manned guarding", "CCTV", "access control", "event security", "lone worker", "key holding", "patrol services"]
    };
  }

  if (t.includes("catering") || t.includes("school meals") || t.includes("food services") ||
      t.includes("vending") || t.includes("hospital catering") || t.includes("meals on wheels")) {
    return {
      key: "catering",
      label: "Catering & food services",
      terms: ["catering services", "school meals", "hospital catering", "meals on wheels", "vending", "food services", "kitchen management"]
    };
  }

  if (t.includes("legal services") || t.includes("solicitor") || t.includes("legal advice") ||
      t.includes("litigation") || t.includes("barrister") || t.includes("legal counsel")) {
    return {
      key: "legal",
      label: "Legal & professional services",
      terms: ["legal services", "solicitor", "barrister", "litigation", "procurement advisory", "HR advisory", "management consultancy"]
    };
  }

  if (t.includes("homelessness") || t.includes("housing support") || t.includes("rough sleeping") ||
      t.includes("supported housing") || t.includes("refuge") || t.includes("temporary accommodation")) {
    return {
      key: "housing-support",
      label: "Housing & homelessness support",
      terms: ["homelessness prevention", "rough sleeping", "supported housing", "temporary accommodation", "refuge", "floating support", "housing advice"]
    };
  }

  if (t.includes("external audit") || t.includes("internal audit") || t.includes("treasury") ||
      t.includes("payroll") || t.includes("insurance services") || t.includes("council tax collection")) {
    return {
      key: "finance",
      label: "Finance, audit & insurance",
      terms: ["external audit", "internal audit", "treasury management", "payroll", "insurance", "banking", "debt recovery", "financial systems"]
    };
  }

  if (t.includes("leisure management") || t.includes("leisure centre") || t.includes("library") ||
      t.includes("arts services") || t.includes("museum") || t.includes("parks management") || t.includes("sports development")) {
    return {
      key: "leisure",
      label: "Leisure, culture & parks",
      terms: ["leisure management", "libraries", "arts & culture", "museums", "parks management", "sports development", "heritage"]
    };
  }

  if (t.includes("planning consultancy") || t.includes("urban regeneration") || t.includes("economic development") ||
      t.includes("masterplan") || t.includes("heritage conservation") || t.includes("transport planning")) {
    return {
      key: "planning",
      label: "Planning, regeneration & economic development",
      terms: ["planning consultancy", "urban regeneration", "economic development", "masterplanning", "heritage", "transport planning", "land development"]
    };
  }

  if (t.includes("probation") || t.includes("prison") || t.includes("custody") ||
      t.includes("youth justice") || t.includes("community safety") || t.includes("rehabilitation")) {
    return {
      key: "justice",
      label: "Justice, probation & community safety",
      terms: ["probation", "prison services", "custody", "youth justice", "community safety", "rehabilitation", "electronic monitoring"]
    };
  }

  if (t.includes("police") || t.includes("fire service") || t.includes("ambulance") ||
      t.includes("emergency planning") || t.includes("fire rescue") || t.includes("paramedic")) {
    return {
      key: "emergency",
      label: "Emergency services",
      terms: ["police", "fire & rescue", "ambulance", "paramedic", "emergency planning", "control room", "PPE emergency services"]
    };
  }

  if (t.includes("policy evaluation") || t.includes("social research") || t.includes("research evaluation") ||
      t.includes("public health research") || t.includes("epidemiology") || t.includes("deliberative")) {
    return {
      key: "research",
      label: "Research, evaluation & data",
      terms: ["social research", "policy evaluation", "public health research", "epidemiology", "data analytics", "consultation research", "market research"]
    };
  }

  if (t.includes("management consulting") || t.includes("transformation consultancy") ||
      t.includes("programme delivery") || t.includes("operating model") || t.includes("business case") ||
      t.includes("central government") || t.includes("cabinet office")) {
    return {
      key: "consulting",
      label: "Central government consulting & transformation",
      terms: ["management consulting", "digital transformation", "programme delivery", "policy development", "operating model", "commercial advisory"]
    };
  }

  return {
    key: "general",
    label: "General public-sector services",
    terms: text.split(/\s+/).filter((word: string) => word.length > 5).slice(0, 12)
  };
}

function resolveSectorFromInput(input: any): SectorResult {
  // Use only what the company DOES — not who their buyers are.
  // idealBuyers/frameworkStatus/lastPublicContract contain buyer-type terms (e.g. "NHS Trust")
  // that trigger the health-sector check before cleaning/facilities, causing wrong sector classification.
  const text = [
    input?.companyName, input?.mainServices, input?.secondaryServices,
  ].filter(Boolean).join(" ");
  return resolveSector(text);
}

function resolveSectorFromScan(scan: any): SectorResult {
  const input: any = scan.input_json || {};
  const text = [
    input.companyName, input.mainServices, input.secondaryServices,
    scan.company_name,
  ].filter(Boolean).join(" ");
  return resolveSector(text);
}

function normaliseCompanyName(value: string) {
  return String(value || "")
    .toLowerCase()
    .replace(/\b(ltd|limited|uk|plc|llp|group|company|co)\b/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function noticeRecordValue(notice: any) {
  return Number(notice?.awardedValue || notice?.valueHigh || notice?.valueLow || 0) || 0;
}

function noticeRecordId(notice: any) {
  return String(notice?.id || "").trim();
}

type IncumbentEntry = { name: string; count: number; totalValue: number; latestAward: string | null };

function buildIncumbentMap(data: ProcurementData): IncumbentEntry[] {
  const awarded = (data?.contractsFinder?.awarded || []) as any[];
  const map = new Map<string, IncumbentEntry>();
  for (const n of awarded) {
    const raw = String(n.awardedSupplier || "").trim();
    if (!raw || raw.toLowerCase().includes("not stated") || raw.length < 3) continue;
    const key = normaliseCompanyName(raw);
    if (!key) continue;
    const entry = map.get(key) || { name: raw, count: 0, totalValue: 0, latestAward: null };
    entry.count++;
    const v = Number(n.awardedValue || n.valueHigh || 0);
    entry.totalValue += v;
    if (n.awardedDate && (!entry.latestAward || n.awardedDate > entry.latestAward)) entry.latestAward = n.awardedDate;
    if (entry.count === 1) entry.name = raw; // keep display name from first seen
    map.set(key, entry);
  }
  return [...map.values()].sort((a, b) => b.count - a.count).slice(0, 8);
}

function renderIncumbentSection(data: ProcurementData): string {
  const incumbents = buildIncumbentMap(data);
  if (incumbents.length === 0) return "";
  const total = incumbents.reduce((s, e) => s + e.count, 0);
  const rows = incumbents.map(e => {
    const pct = total > 0 ? Math.round((e.count / total) * 100) : 0;
    const val = e.totalValue > 0
      ? (e.totalValue >= 1_000_000 ? `£${(e.totalValue / 1_000_000).toFixed(1)}m` : `£${Math.round(e.totalValue / 1000)}k`)
      : "—";
    const latest = e.latestAward
      ? new Date(e.latestAward).toLocaleDateString("en-GB", { month: "short", year: "numeric" })
      : "—";
    return `<tr>
      <td style="padding:9px 14px;font-size:14px;color:var(--text);border-bottom:1px solid var(--border-2)">${escapeHtml(e.name)}</td>
      <td style="padding:9px 14px;text-align:center;font-size:13px;font-family:var(--mono);border-bottom:1px solid var(--border-2)">${e.count}</td>
      <td style="padding:9px 14px;text-align:right;font-size:13px;font-family:var(--mono);border-bottom:1px solid var(--border-2)">${escapeHtml(val)}</td>
      <td style="padding:9px 14px;border-bottom:1px solid var(--border-2)">
        <div style="display:flex;align-items:center;gap:8px">
          <div style="height:6px;width:${Math.max(pct, 2)}%;background:var(--brand);border-radius:2px"></div>
          <span style="font-size:11px;font-family:var(--mono);color:var(--muted)">${pct}%</span>
        </div>
      </td>
      <td style="padding:9px 14px;font-size:12px;font-family:var(--mono);color:var(--muted);border-bottom:1px solid var(--border-2)">${escapeHtml(latest)}</td>
    </tr>`;
  }).join("");
  return `<section style="margin:40px 0;background:var(--surface-2);border:1px solid var(--border-2);padding:28px 32px" class="no-print">
  <h2 style="font-family:var(--sans);font-size:22px;font-weight:800;margin-bottom:6px;color:var(--text)">Incumbent map</h2>
  <p style="font-size:13px;color:var(--muted);margin-bottom:18px;font-family:var(--mono)">Derived from awarded contract records in this dataset. Not exhaustive — covers notices returned by keyword search only.</p>
  <table style="width:100%;border-collapse:collapse">
    <thead>
      <tr style="background:var(--surface-3)">
        <th style="padding:8px 14px;text-align:left;font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);font-weight:600">Supplier</th>
        <th style="padding:8px 14px;text-align:center;font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);font-weight:600">Awards</th>
        <th style="padding:8px 14px;text-align:right;font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);font-weight:600">Total value</th>
        <th style="padding:8px 14px;font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);font-weight:600">Share</th>
        <th style="padding:8px 14px;font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);font-weight:600">Latest award</th>
      </tr>
    </thead>
    <tbody>${rows}</tbody>
  </table>
</section>`;
}

function noticeText(notice: any) {
  return [
    notice?.title,
    notice?.buyer,
    notice?.description,
    notice?.awardedSupplier,
    notice?.keyword,
    notice?.region,
    notice?.type,
    notice?.status
  ]
    .join(" ")
    .toLowerCase();
}

function confidenceGrade(score: number) {
  if (score >= 80) return "High";
  if (score >= 60) return "Medium";
  if (score >= 45) return "Low";
  return "Very low";
}

function trustStatusForNotice(score: number, notice: any, input: any) {
  const company = normaliseCompanyName(input?.companyName || "");
  const supplier = normaliseCompanyName(notice?.awardedSupplier || "");
  const hasSource = Boolean(notice?.url && noticeRecordId(notice));

  if (company && supplier && supplier.includes(company)) return "Verified";
  if (score >= 70 && hasSource) return "Verified";
  if (score >= 55 && hasSource) return "Inferred";
  if (score >= 40) return "Strategic target";
  return "Not confirmed";
}

function scoreNoticeForClient(notice: any, input: any) {
  const sector = resolveSectorFromInput(input);
  const text = noticeText(notice);
  const company = normaliseCompanyName(input?.companyName || "");
  const supplier = normaliseCompanyName(notice?.awardedSupplier || "");
  const recordValue = noticeRecordValue(notice);
  const reasons: string[] = [];

  let score = 0;

  const matchedTerms = sector.terms.filter(term => text.includes(term.toLowerCase()));
  if (matchedTerms.length) {
    score += Math.min(45, matchedTerms.length * 14);
    reasons.push(`service match: ${matchedTerms.slice(0, 4).join(", ")}`);
  }

  if (notice?.keyword && sector.terms.some(term => String(notice.keyword).toLowerCase().includes(term.split(" ")[0]))) {
    score += 12;
    reasons.push(`keyword match: ${notice.keyword}`);
  }

  if (company && supplier && supplier.includes(company)) {
    score += 45;
    reasons.push("client appears as awarded supplier");
  }

  if (String(notice?.status || "").toLowerCase().includes("open")) {
    score += 10;
    reasons.push("open opportunity");
  }

  if (String(notice?.status || "").toLowerCase().includes("award") || notice?.awardedSupplier) {
    score += 8;
    reasons.push("award / incumbent signal");
  }

  if (recordValue > 0) {
    score += 8;
    reasons.push("stated contract value");
  }

  if (notice?.buyer) {
    score += 5;
    reasons.push("named buyer");
  }

  if (notice?.url && noticeRecordId(notice)) {
    score += 7;
    reasons.push("source record available");
  }

  score = Math.max(0, Math.min(100, score));

  const cap = parseMoneyCap(input);
  const isRelevant = score >= 55;
  const addressableValue = isRelevant && recordValue > 0 ? Math.min(recordValue, cap) : 0;

  return {
    ...notice,
    recordId: noticeRecordId(notice),
    recordValue,
    relevanceScore: score,
    confidence: confidenceGrade(score),
    trustStatus: trustStatusForNotice(score, notice, input),
    relevanceReasons: reasons,
    addressableValue,
    sourceUrl: notice?.url || ""
  };
}

function buildTrustLayer(input: any, data: any) {
  const open = data?.contractsFinder?.open || [];
  const awarded = data?.contractsFinder?.awarded || [];

  const scoredOpen = open.map((notice: any) => ({
    ...scoreNoticeForClient(notice, input),
    sourceBucket: "open"
  }));

  const scoredAwarded = awarded.map((notice: any) => ({
    ...scoreNoticeForClient(notice, input),
    sourceBucket: "awarded"
  }));

  const pulled = [...scoredOpen, ...scoredAwarded];
  const relevant = pulled
    .filter((notice: any) => notice.relevanceScore >= 55)
    .sort((a: any, b: any) => b.relevanceScore - a.relevanceScore);

  const relevantOpen = relevant.filter((notice: any) => notice.sourceBucket === "open");
  const relevantAwarded = relevant.filter((notice: any) => notice.sourceBucket === "awarded");
  const verified = relevant.filter((notice: any) => notice.trustStatus === "Verified");
  const inferred = relevant.filter((notice: any) => notice.trustStatus === "Inferred");
  const strategic = relevant.filter((notice: any) => notice.trustStatus === "Strategic target");

  const buyers = new Set(relevant.map((notice: any) => notice.buyer).filter(Boolean));
  const suppliers = new Set(relevantAwarded.map((notice: any) => notice.awardedSupplier).filter(Boolean));

  const pulledValue = pulled.reduce((sum: number, notice: any) => sum + notice.recordValue, 0);
  const relevantValue = relevant.reduce((sum: number, notice: any) => sum + notice.recordValue, 0);
  const addressableValue = relevant.reduce((sum: number, notice: any) => sum + notice.addressableValue, 0);

  return {
    generatedAt: nowIso(),
    sectorLens: resolveSectorFromInput(input).label,
    clientCapacityCap: parseMoneyCap(input),
    pulledCount: pulled.length,
    relevantCount: relevant.length,
    noisyCount: pulled.length - relevant.length,
    relevantOpenCount: relevantOpen.length,
    relevantAwardCount: relevantAwarded.length,
    verifiedCount: verified.length,
    inferredCount: inferred.length,
    strategicCount: strategic.length,
    distinctRelevantBuyers: buyers.size,
    distinctRelevantSuppliers: suppliers.size,
    totalPulledRecordValue: pulledValue,
    totalRelevantRecordValue: relevantValue,
    addressableOpportunityValue: addressableValue,
    keywords: data?.keywords || [],
    regions: data?.regions || "Not stated",
    relevantRecords: relevant.slice(0, 30),
    topRelevantRecords: relevant.slice(0, 10),
    excludedRecords: pulled.filter((notice: any) => notice.relevanceScore < 55).slice(0, 20)
  };
}

function trustLayerMarkdown(input: any, data: any) {
  const trust = buildTrustLayer(input, data);

  const rows = trust.topRelevantRecords.length
    ? trust.topRelevantRecords
        .map((n: any, index: number) => {
          return `${index + 1}. ${n.title}
- Trust: ${n.trustStatus}
- Confidence: ${n.confidence}
- Relevance score: ${n.relevanceScore}/100
- Buyer: ${n.buyer || "Not stated"}
- Supplier / incumbent: ${n.awardedSupplier || "Not stated"}
- Pulled record value: ${trustMoney(n.recordValue)}
- Addressable value signal: ${trustMoney(n.addressableValue)}
- Source record ID: ${n.recordId || "Not stated"}
- Source URL: ${n.sourceUrl || "Not stated"}
- Why included: ${n.relevanceReasons.join("; ") || "No strong reason recorded"}`
        })
        .join("\n\n")
    : "No relevant records passed the relevance threshold. Treat buyer ideas as strategic targets, not verified opportunities.";

  return `
TRUST LAYER — STRUCTURED DATA FILTER

Definitions:
- Pulled records = all Contracts Finder records returned by the data search.
- Relevant records = pulled records that match the client's services and buyer route with relevance score >= 55/100.
- Addressable opportunity value = capped value signal from relevant records only. It is not a revenue forecast.
- Verified = directly supported by a source record URL/ID and strong relevance.
- Inferred = supported by a source record but relevance still needs human checking.
- Strategic target = commercially sensible target but not directly verified by a pulled record.
- Not confirmed = do not present as fact.

Summary:
- Sector lens: ${trust.sectorLens}
- Pulled records: ${trust.pulledCount}
- Relevant records: ${trust.relevantCount}
- Excluded / noisy records: ${trust.noisyCount}
- Relevant open opportunities: ${trust.relevantOpenCount}
- Relevant award signals: ${trust.relevantAwardCount}
- Verified source-backed records: ${trust.verifiedCount}
- Inferred source-backed records: ${trust.inferredCount}
- Strategic target records: ${trust.strategicCount}
- Distinct relevant buyers: ${trust.distinctRelevantBuyers}
- Distinct relevant suppliers: ${trust.distinctRelevantSuppliers}
- Total pulled-record value: ${trustMoney(trust.totalPulledRecordValue)}
- Relevant pulled-record value: ${trustMoney(trust.totalRelevantRecordValue)}
- Addressable opportunity value signal: ${trustMoney(trust.addressableOpportunityValue)}
- Client capacity cap used: ${trustMoney(trust.clientCapacityCap)}
- Regions searched: ${trust.regions}
- Keywords searched: ${trust.keywords.join(", ") || "Not stated"}

Top relevant source records:
${rows}
`;
}

function collectEvidenceStats(scan: ScanRecord) {
  const trust = buildTrustLayer(scan.input_json || {}, scan.procurement_json || {});

  return {
    openCount: trust.pulledCount,
    awardCount: trust.relevantCount,
    buyerCount: trust.distinctRelevantBuyers,
    supplierCount: trust.distinctRelevantSuppliers,
    knownValue: trust.addressableOpportunityValue,
    largestValue: trust.totalRelevantRecordValue,
    liveDeadlines: trust.relevantOpenCount,
    pulledCount: trust.pulledCount,
    relevantCount: trust.relevantCount,
    noisyCount: trust.noisyCount,
    verifiedCount: trust.verifiedCount,
    inferredCount: trust.inferredCount,
    strategicCount: trust.strategicCount,
    totalPulledRecordValue: trust.totalPulledRecordValue,
    totalRelevantRecordValue: trust.totalRelevantRecordValue,
    addressableOpportunityValue: trust.addressableOpportunityValue,
    keywords: trust.keywords,
    regions: trust.regions,
    sectorLens: trust.sectorLens
  };
}

function evidenceBar(label: string, detail: string, valueLabel: string, width: number) {
  const safeWidth = Math.max(8, Math.min(100, width));
  return `
    <div class="evidence-bar">
      <div>
        <strong>${escapeHtml(label)}</strong>
        <small>${escapeHtml(detail)}</small>
      </div>
      <b>${escapeHtml(valueLabel)}</b>
      <span><i style="width:${safeWidth}%"></i></span>
    </div>
  `;
}


function evidenceDashboard(scan: ScanRecord) {
  const trust = buildTrustLayer(scan.input_json || {}, scan.procurement_json || {});
  return renderWorldClassDashboard(trust);
}



function buildPrompt(input: z.infer<typeof intakeSchema>, data: ProcurementData) {
  return `
You are GovRevenue Agent, a sharp UK public-sector revenue intelligence analyst.

This is a paid commercial scan. It must feel like a practical revenue map, not a generic AI report.

ABSOLUTE IDENTITY RULE:
The intake is the source of truth. Do not replace the company with another similar-name business.
Before using a public fact, confirm it matches at least two of: website, location, services, founder/name, sector, trading identity.
If not confirmed, write "not confirmed from sources checked" and do not use the fact.
Never infer Companies House records, assets, address, team size, founding date, or service category from name-only matching.

TRUST LAYER RULE:
You must separate:
1. Pulled records
2. Relevant records
3. Addressable opportunity value
4. AI interpretation

Every major recommendation must carry one of these labels:
- [Verified] Directly supported by a pulled source record ID/URL.
- [Inferred] Supported by a source record, but relevance requires human verification.
- [Strategic target] Commercially sensible but not directly verified by a pulled record.
- [Not confirmed] Do not treat as fact.

Do not present total pulled-record value as revenue potential.
Only use addressable opportunity value signal when discussing opportunity size, and state that it is capped and directional, not a forecast.

Company intake:
${JSON.stringify(input, null, 2)}

${trustLayerMarkdown(input, data)}

---

PARTNER VOICE — apply throughout the entire report:
Write as a senior commercial partner addressing the managing director directly. "You" means the company. No passive voice where "you" works. Sound like someone who has read the data, drawn a conclusion, and is now telling the client what it means for their business — not summarising the output of a system. Confident where the data supports it. Honest where it does not.

ZERO SELF-REFERENCE — hard constraint:
Remove from your output any sentence that:
- Names or references this scan, this report, this dashboard, or this analysis
- Explains what the report is doing ("This section analyses...", "The following table shows...", "The goal of this report is...")
- Describes how data was gathered ("We pulled X records from...", "The trust filter identified...", "GovRevenue indexed...")
Every word must be commercial intelligence directed at the client. Not process description.

LOAD-BEARING NUMBERS — hard constraint:
Every figure above £50k must be followed immediately by: (1) where it sits — named buyer, region, or contract reference; (2) when it becomes live — renewal window, deadline, or urgency horizon; (3) what to do with it — the specific next action. A number without a direct line to action is dead weight. Connect it or cut it.

OPENING THESIS — required before the EDP table:
Section 1 must open with a direct paragraph BEFORE the table. It must:
- Name the specific geography, sector, and the single biggest commercial opportunity or threat visible in the pulled data
- State a specific urgency window (days or months, not "soon" or "in due course")
- Name actual buyers or incumbents from pulled records where possible
- End with the commercial consequence — what the client gains by moving, and what they concede by waiting
If the evidence is too thin to support a specific thesis, write: "The evidence base for [Company] is limited. The clearest available move is [specific action from available data] — start there."

VERDICT VOICE:
The Verdict field must be a direct commercial sentence, not a label.
BAD: "Bid Selectively"
BAD: "Company shows moderate readiness"
GOOD: "Move on [Buyer X] in the next 60 days — this is your clearest opening."
GOOD: "Your [region] incumbency expires before a competitor notices. Contact [Buyer] this month."
GOOD: "Not ready for a prime contract. Get on [Framework Y] first — that is your 18-month revenue route."
The verdict must be specific to this company's actual situation.

---

Return clean Markdown only.

Use this exact structure:

# GovRevenue Scan: [Company Name]

## 1. Executive Decision Panel
Open with the OPENING THESIS paragraph, then give the decision panel with these exact fields:

| Field | Answer |
|---|---|
| Verdict | |
| Can they win now? | |
| Best first money route | |
| Fastest action this week | |
| Main blocker | |
| Evidence Grade | |
| Recommended route | |

Rules:
- Write the thesis paragraph first — before the table.
- The verdict must be a direct commercial sentence with a named action (see VERDICT VOICE rule above).
- If verified evidence is weak, do not pretend they are bid-ready.
- Evidence Grade must be A, B, C, D or E:
  - A = strong source-backed evidence and strong sector alignment
  - B = good market signal but some readiness checks needed
  - C = usable but mixed/noisy evidence
  - D = low exact-match evidence; strategic scan, not bid-ready map
  - E = insufficient evidence
- Do not use the phrase "Data quality: Weak" anywhere. Use Evidence Grade instead.

## 2. Evidence Grade and Scan Basis
Include:
- Corrected sector lens
- Regions searched
- Keywords searched
- Open records shown
- Award signals shown
- Total records pulled
- Relevant records
- Verified evidence records
- Inferred records
- Strategic target records
- Excluded / noisy records
- Addressable value signal
- Relevant pulled-record value
- Evidence interpretation

Rules:
- If sector lens and keywords conflict, say this is a QA issue and correct the lens/keyword interpretation.
- Cleaning reports must not use property-survey keywords like building surveying, condition survey, estate consultancy, asset management, property consultancy or built asset consultancy unless the company is actually property/built-environment.
- Software/ICT reports must be framed as software, ICT and digital transformation, not generic professional services.
- Training/enterprise support reports must be framed as training, skills, enterprise support and professional services.

## 3. Market Position Summary
Write as if briefing the client on their market position, not summarising a tool's output. Cover:
- What the evidence base shows about their real competitive position in this sector and region
- Which buyers have money moving and which are dormant
- What the addressable value signal means in practice — named buyers, named routes, specific amounts tied to specific actions
- Where they sit relative to the identified incumbents
Use compact value notation (£k / £m / £bn). No sentence may start with "The data shows..." or "This section..." — start every sentence with a commercial insight.

## 4. Source-Backed Evidence
Only use pulled source records or clearly labelled client-provided evidence.
For each top record:
- Record name
- Buyer
- Evidence status
- Confidence
- Value shown
- Source reference
- Commercial meaning
- Best use

Rules:
- If verified evidence count is 0, do not label any recommendation [Verified] unless it is explicitly client-provided.
- Client-provided evidence must be labelled [Client-provided], not generic [Verified].
- Do not invent buyers, awards, suppliers, values, source URLs, certifications or case studies.

## 5. Money Map: Best Routes to Revenue
Create a table:
Route | Buyer type | Speed to revenue | Difficulty | Evidence strength | Why money exists | Best next action | Score /100

Include 5-7 routes where possible:
- Direct tender
- Framework
- DPS
- Subcontract / partner
- Pilot or grant-funded route
- Buyer outreach / renewal watch
- Compliance preparation

Rules:
- Rank by evidence strength, buyer fit, speed, difficulty and readiness.
- If evidence is weak, make the route cautious.

## 6. Buyer Watchlist
Create a table:
Buyer | Buyer type | Current incumbent | Why they matter | Likely buying route | Evidence strength | Fit score | Next action

Rules:
- Verified buyers must come from pulled records or verified sources.
- Strategic buyers are allowed only when labelled [Strategic target].
- Do not invent named buyers.
- For weak evidence reports, use cautious language such as monitor, validate or qualify.
- The "Current incumbent" column must name the current holder where the pulled data includes an "Awarded supplier" field. Use the format "Incumbent: [name]" or "Not stated" if unknown. This is critical intelligence — do not leave it blank if an awarded supplier is in the data.
- Where an incumbent appears in multiple awarded records, note the repeat win: "Incumbent: [name] (×3 awards)".

## 6a. Incumbent Contract Timeline
Map every contract in the pulled data where a supplier is named (incumbent or recently awarded) onto a 0-to-24-month renewal horizon.

Build a table:
Contract | Buyer | Incumbent / awarded supplier | Value | Published / awarded date | Est. contract end | Est. renewal window opens | Urgency

Urgency:
- **ACT NOW** — renewal process likely already underway or opening within 6 months
- **POSITION** — renewal window opens 6-12 months out; start building buyer relationship
- **WATCH** — 12-24 months; log and track; seek informal contact
- **HORIZON** — beyond 24 months or dates unconfirmable

After the table, write a direct paragraph (no preamble, no self-reference):
Name the single most valuable incumbent position approaching renewal, state the estimated window date, and give one specific action — who to contact, what to say, and why waiting past [specific month] forfeits the advantage.

If no incumbents are named in the pulled records, write: "No named incumbents in pulled records. The Buyer Watchlist above identifies who to approach before contracts are formally re-tendered."

Date estimation guidance: if no end date is given, estimate from published/awarded date + typical sector contract length. Cleaning/FM: 3-5 years. IT: 2-4 years. Consultancy: 1-3 years. Construction: 1-3 years. Training: 1-2 years.

## 7. Bid Readiness Score
Give:
- Overall score /100
- Verdict: Bid now / Bid selectively / Prepare first / Not ready
- Category scores:
  - Public-sector fit
  - Evidence strength
  - Buyer relevance
  - Compliance readiness
  - Capacity fit
  - Case study strength
  - Route clarity
  - Immediate bid readiness

Then explain:
- What blocks the next 20 points
- What to fix in 30 days

Rules:
- Market demand does not automatically mean bid readiness.
- Missing insurance, accreditations, case studies, framework access, capacity or cyber/compliance proof should reduce readiness.

## 8. Do Not Chase These Yet
Create a table:
Contract / route to avoid | Why risky | Proof missing | When it becomes suitable

Give honest negative guidance. Include unsuitable or premature routes such as:
- oversized prime contracts
- wrong-sector framework lots
- low-relevance pulled records
- generic low-margin work
- tenders demanding proof the company does not yet have

## 9. 30-Day Activation Pack
Write this section as direct instructions addressed to the company: "Week 1, you need to..." — not "The company should...".

Weekly actions:
- Week 1: specific evidence-gathering and access actions tied to the named buyers and routes in sections 4-6
- Week 2: capability statement drafting and bid pack — with specific section headings drawn from this company's actual services and evidence
- Week 3: buyer outreach — name specific buyers from the watchlist, with timing and opening angle
- Week 4: bid / partner activation — specify which route, which buyer, and what the entry point looks like

Then include:
- Documents needed before bidding (sector-specific — list exact certifications, insurances, case study formats)
- Capability statement bullets (pull from the company's stated services and the verified/inferred evidence)
- Buyer outreach email (named buyer from watchlist; open with a specific hook from the pulled records, not a generic intro)
- Partner outreach email (named sector; reference the route identified in the Money Map)
- LinkedIn message (short; reference a specific notice or buyer activity)
- Bid/no-bid checklist (specific to the top-scoring route in section 5)

Every item must be specific to this company's actual situation. No sentence that could apply to any other company is allowed.

## 10. QA Notes / Integrity Checks
Create a final table:
Check | Status | Notes

Include checks for:
- sector and keyword match
- verified labels
- zero verified evidence with verified claims
- buyer invention
- client-provided evidence labelling
- company punctuation
- HTML entity cleanup
- source formatting
- required sections
- human verification required

Final commercial note:
End with:
"No outcome is guaranteed. This scan is commercial intelligence, not legal, procurement or financial advice. Human verification is required before bid decisions."
`;
}


function withTimeout<T>(ms: number, fn: (signal: AbortSignal) => Promise<T>): Promise<T> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), ms);
  return fn(controller.signal).finally(() => clearTimeout(timer));
}

// OpenAI calls are capped at 90s (see CLAUDE.md) to keep the scan queue from stalling.
function withOpenAiTimeout<T>(fn: (signal: AbortSignal) => Promise<T>): Promise<T> {
  return withTimeout(90_000, fn);
}

async function callOpenAiReport(prompt: string): Promise<string> {
  return withOpenAiTimeout(async signal => {
    try {
      const response = await openai.responses.create({
        model: OPENAI_MODEL,
        tools: [{ type: "web_search" } as any],
        input: prompt
      }, { signal });
      return enforceDataQualityLanguage(response.output_text || "No report returned.");
    } catch (firstError: any) {
      try {
        const response = await openai.responses.create({
          model: OPENAI_MODEL,
          tools: [{ type: "web_search_preview" } as any],
          input: prompt
        }, { signal });
        return enforceDataQualityLanguage(response.output_text || "No report returned.");
      } catch (secondError: any) {
        captureError(secondError, {
          openai: { model: OPENAI_MODEL, fallbackAfterPrimaryFailure: true, primaryError: firstError?.message || String(firstError) }
        });
        throw secondError;
      }
    }
  });
}

// Claude generates the report (the product) with server-side web search. Opus + search
// needs more headroom than the 90s OpenAI budget, so it gets its own 150s cap.
async function callClaudeReport(prompt: string): Promise<string> {
  if (!anthropic) throw new Error("Anthropic client not configured.");
  return withTimeout(150_000, async signal => {
    const message = await anthropic.messages.create({
      model: ANTHROPIC_MODEL,
      max_tokens: 8000,
      system: "You are GovRevenue's senior UK public-sector procurement analyst. Follow the user's instructions exactly. Return only the finished report as clean GitHub-flavored Markdown — no preamble, no sign-off, no commentary outside the report itself.",
      messages: [{ role: "user", content: prompt }],
      tools: [{ type: "web_search_20250305", name: "web_search", max_uses: 5 }] as any
    }, { signal });
    const text = message.content
      .map(block => (block.type === "text" ? block.text : ""))
      .join("")
      .trim();
    return enforceDataQualityLanguage(text || "No report returned.");
  });
}

async function callLlmReport(prompt: string): Promise<string> {
  if (anthropic) {
    try {
      return await callClaudeReport(prompt);
    } catch (claudeError: any) {
      captureError(claudeError, {
        anthropic: { model: ANTHROPIC_MODEL, fellBackToOpenAI: true, error: claudeError?.message || String(claudeError) }
      });
      console.error("[report] Claude failed, falling back to OpenAI:", claudeError?.message || claudeError);
    }
  }
  return callOpenAiReport(prompt);
}

const SCAN_FETCH_TIMEOUT_MS = 8 * 60 * 1000; // 8 minutes

async function runScan(id: string, input: z.infer<typeof intakeSchema>) {
  await updateScan(id, { status: "running", error_message: null });
  await emitScanStage(id, "fetching");

  const fetchController = new AbortController();
  const fetchTimeout = setTimeout(() => fetchController.abort(), SCAN_FETCH_TIMEOUT_MS);

  try {
    const data = await pullProcurementData(input, fetchController.signal);
    clearTimeout(fetchTimeout);
    await emitScanStage(id, "scoring");

    const prompt = buildPrompt(input, data);
    await emitScanStage(id, "report");

    const report = await callLlmReport(prompt);

    await updateScan(id, {
      status: "completed",
      procurement_json: data,
      report_markdown: report,
      error_message: null
    });
    await emitScanStage(id, "done");

    const links = buildScanLinks(id);
    await notifyScanCompleted({
      scanId: id,
      companyName: input.companyName,
      status: "completed",
      reportUrl: links.reportUrl,
      pdfUrl: links.pdfUrl,
      clientEmail: clientEmailFromInput(input)
    });

    console.log(`[scan] completed ${id}`);
  } catch (err: any) {
    clearTimeout(fetchTimeout);
    const isTimeout = err?.name === "AbortError";
    const message = isTimeout
      ? "Scan timed out during data fetch (8 min limit). Too many results from Contracts Finder — please try again."
      : (err?.message || String(err));

    console.error(`[scan] failed ${id}`, err);
    if (!isTimeout) captureError(err, { scan: { id, companyName: input.companyName, status: "failed" } });

    await updateScan(id, { status: "failed", error_message: message });
    await emitScanStage(id, "failed");

    await notifyScanFailed({
      scanId: id,
      companyName: input.companyName,
      status: "failed",
      errorSummary: message
    });
  }
}

async function enqueueScan(id: string, input: z.infer<typeof intakeSchema>) {
  if (!scanQueue) {
    console.log("[queue] REDIS_URL not set. Running scan in-process.");
    runScan(id, input).catch(error => {
      console.error(error);
      captureError(error, { scan: { id, companyName: input.companyName, queue: "in-process" } });
    });
    return;
  }

  await scanQueue.add(
    "run-scan",
    { id, input },
    {
      attempts: 2,
      backoff: {
        type: "exponential",
        delay: 5000
      },
      removeOnComplete: {
        age: 60 * 60 * 24 * 7,
        count: 1000
      },
      removeOnFail: {
        age: 60 * 60 * 24 * 14,
        count: 1000
      }
    }
  );

  console.log(`[queue] queued scan ${id}`);
}

function startScanWorker() {
  if (!redisConnection) {
    console.log("[queue] Redis not configured. Worker disabled.");
    return;
  }

  const worker = new Worker(
    "govrevenue-scans",
    async job => {
      const { id, input } = job.data || {};
      if (!id || !input) throw new Error("Invalid scan job payload.");
      console.log(`[worker] processing scan ${id}`);
      await runScan(id, input);
    },
    {
      connection: redisConnection as any,
      concurrency: Number(process.env.SCAN_WORKER_CONCURRENCY || 1)
    }
  );

  worker.on("completed", job => {
    console.log(`[worker] completed job ${job.id}`);
  });

  worker.on("failed", (job, error) => {
    console.error(`[worker] failed job ${job?.id}`, error);
    captureError(error, { worker: { jobId: job?.id, scanId: job?.data?.id } });
  });

  console.log("[queue] worker started");
}

function appBaseUrl() {
  const explicit = process.env.PUBLIC_APP_URL || process.env.APP_BASE_URL || process.env.BASE_URL;
  const railway = process.env.RAILWAY_PUBLIC_DOMAIN;
  return (explicit || (railway ? `https://${railway}` : "")).replace(/\/+$/, "");
}

function absoluteAppUrl(path: string) {
  const base = appBaseUrl();
  const p = path.startsWith("/") ? path : `/${path}`;
  return base ? `${base}${p}` : p;
}

async function runWeeklyAlert(subscriptionId: string) {
  const sub = await getSubscription(subscriptionId);
  if (!sub || !sub.active) return;

  const input = sub.input_json as z.infer<typeof intakeSchema>;
  const data = await pullProcurementData(input);

  const allNotices: ProcurementNotice[] = [
    ...(data.contractsFinder?.open || []),
    ...(data.contractsFinder?.awarded || []),
    ...(data.findTender?.notices || [])
  ];

  const alreadySent = new Set(sub.alerted_notice_ids || []);
  const newNotices = allNotices.filter(n => n.id && !alreadySent.has(n.id));

  if (newNotices.length > 0) {
    await sendWeeklyAlert({
      subscriptionId: sub.id,
      companyName: sub.company_name,
      email: sub.email,
      newNotices: newNotices.slice(0, 20).map(n => ({
        title: n.title || "Untitled",
        buyer: n.buyer || "Unknown buyer",
        value: n.valueHigh
          ? `£${Math.round(n.valueHigh / 1000)}k`
          : n.valueLow ? `£${Math.round(n.valueLow / 1000)}k` : "Value not stated",
        deadline: n.deadlineDate || null,
        url: noticeUrl(n.id || ""),
        source: n.source || "Contracts Finder"
      })),
      totalNewCount: newNotices.length,
      reportUrl: absoluteAppUrl(`/scan/${sub.scan_id}`),
      unsubscribeUrl: absoluteAppUrl(`/unsubscribe/${sub.id}`)
    });
  }

  const updatedIds = [
    ...alreadySent,
    ...newNotices.map(n => n.id).filter((id): id is string => Boolean(id))
  ];
  await updateSubscriptionAlerted(sub.id, updatedIds);

  console.log(`[alerts] ${sub.company_name}: ${newNotices.length} new, ${updatedIds.length} total tracked`);
}

async function enqueueWeeklyAlert(subscriptionId: string) {
  if (!alertQueue) {
    console.log("[alerts] Redis not available — weekly alerts require Redis.");
    return;
  }
  await alertQueue.add(
    "weekly-alert",
    { subscriptionId },
    {
      repeat: { every: 7 * 24 * 60 * 60 * 1000 },
      jobId: subscriptionId,
      attempts: 2,
      backoff: { type: "exponential", delay: 10_000 },
      removeOnComplete: { age: 60 * 60 * 24 * 30 },
      removeOnFail: { age: 60 * 60 * 24 * 30 }
    }
  );
  console.log(`[alerts] scheduled weekly alert for ${subscriptionId}`);
}

function startAlertWorker() {
  if (!redisConnection) {
    console.log("[alerts] Redis not configured. Weekly alerts disabled.");
    return;
  }

  const worker = new Worker(
    "govrevenue-alerts",
    async job => {
      const { subscriptionId } = job.data || {};
      if (!subscriptionId) throw new Error("Invalid alert job payload.");
      console.log(`[alerts] processing ${subscriptionId}`);
      await runWeeklyAlert(subscriptionId);
    },
    { connection: redisConnection as any, concurrency: 2 }
  );

  worker.on("completed", job => {
    console.log(`[alerts] completed job ${job.id}`);
  });

  worker.on("failed", (job, error) => {
    console.error(`[alerts] failed job ${job?.id}`, error);
    captureError(error, { alertWorker: { subscriptionId: job?.data?.subscriptionId } });
  });

  console.log("[alerts] worker started");
}

type BriefingSubscriberRow = { id: string; email: string; category: string | null };

async function runBriefingAlerts(): Promise<void> {
  if (!isEmailConfigured()) {
    console.log("[briefing] email not configured, skipping");
    return;
  }
  const subscribers: BriefingSubscriberRow[] = pool
    ? (await pool.query<BriefingSubscriberRow>(`SELECT id, email, category FROM briefing_subscribers ORDER BY created_at ASC`)).rows
    : [...briefMemStore.values()];

  if (subscribers.length === 0) {
    console.log("[briefing] no subscribers");
    return;
  }

  // Fetch recent open signals across all live desks
  let signals: HomepageSignal[] = [];
  if (pool) {
    const r = await pool.query<HomepageSignal>(
      `SELECT id, category, title, buyer, source, source_url, notice_date, deadline_date, value_amount, status, fetched_at
       FROM homepage_signals
       WHERE (LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')
         AND (deadline_date IS NULL OR deadline_date > NOW())
       ORDER BY notice_date DESC NULLS LAST
       LIMIT 10`
    );
    signals = r.rows;
  } else {
    signals = [...sigMemStore.values()]
      .filter(s => !s.status || s.status.toLowerCase().includes("open") || s.status.toLowerCase().includes("active"))
      .sort((a, b) => (b.notice_date || "").localeCompare(a.notice_date || ""))
      .slice(0, 10);
  }

  if (signals.length === 0) {
    console.log("[briefing] no signals to send");
    return;
  }

  const briefingSignals = signals.map(s => ({
    title: s.title || "Untitled",
    buyer: s.buyer || "Buyer not stated",
    category: CATEGORY_LABELS[s.category] || s.category || "General",
    value: s.value_amount && s.value_amount > 0
      ? s.value_amount >= 1_000_000 ? `£${(s.value_amount / 1_000_000).toFixed(1)}m` : `£${Math.round(s.value_amount / 1_000)}k`
      : "Value not stated",
    deadline: s.deadline_date ? new Date(s.deadline_date).toLocaleDateString("en-GB") : null,
    url: s.source_url || absoluteAppUrl(`/desk/${s.category}`)
  }));

  let sent = 0;
  for (const sub of subscribers) {
    try {
      await sendBriefingEmail({
        email: sub.email,
        signals: briefingSignals,
        unsubscribeUrl: absoluteAppUrl(`/unsubscribe-briefing/${sub.id}`)
      });
      sent++;
    } catch (err) {
      console.error(`[briefing] failed to send to ${sub.email}`, err);
    }
  }
  console.log(`[briefing] sent to ${sent}/${subscribers.length} subscribers`);
}

function startBriefingWorker() {
  if (!redisConnection) {
    console.log("[briefing] Redis not configured. Weekly briefings disabled.");
    return;
  }

  const briefingQueue = new Queue("govrevenue-briefing", { connection: redisConnection as any });
  briefingQueue.add(
    "weekly-briefing",
    {},
    {
      repeat: { every: 7 * 24 * 60 * 60 * 1000 },
      jobId: "briefing",
      attempts: 2,
      backoff: { type: "exponential", delay: 10_000 },
      removeOnComplete: { age: 60 * 60 * 24 * 30 },
      removeOnFail: { age: 60 * 60 * 24 * 30 }
    }
  ).catch(err => console.error("[briefing] failed to schedule job", err));

  const worker = new Worker(
    "govrevenue-briefing",
    async () => { await runBriefingAlerts(); },
    { connection: redisConnection as any, concurrency: 1 }
  );

  worker.on("completed", job => { console.log(`[briefing] completed job ${job.id}`); });
  worker.on("failed", (job, err) => {
    console.error(`[briefing] failed job ${job?.id}`, err);
    captureError(err, { briefingWorker: true });
  });

  console.log("[briefing] worker started");
}


// SIGNAL_CATEGORIES and CATEGORY_LABELS are defined after DESK_PROFILES below

type DeskCategory = {
  label: string;
  keywords: string[];  // matched against (title + description).toLowerCase(); first match wins
  subcategories: string[];
};

type DeskProfile = {
  slug: string;
  label: string;
  standfirst: string;
  live: boolean;        // false = interstitial; set to true to promote a desk
  pinnedProfile: z.infer<typeof intakeSchema>;
  categories: DeskCategory[];
};

const DESK_PROFILES: DeskProfile[] = [
  {
    slug: "construction",
    label: "Construction & Estates",
    standfirst: "Capital works, refurbishment and estates services across the public sector.",
    live: true,
    pinnedProfile: intakeSchema.parse({
      companyName: "GovRevenue Desk",
      mainServices: "construction project management estate management building refurbishment capital works planned maintenance",
      idealBuyers: "local authorities housing associations NHS trusts",
      mainGoal: "find construction and estate management contracts"
    }),
    categories: [
      { label: "Repairs, Maintenance & Voids",         keywords: ["repair", "maintenance", "void", "responsive", "reactive", "handyman"],                                          subcategories: ["Reactive repairs","Planned maintenance","Void property works","Minor works","Handyman services","Multi-trade repairs","Damp & mould works","Emergency repairs","Cyclical maintenance","Pre-void inspections","Void turnaround","Graffiti removal","Patch repairs","Drain unblocking","Gutter clearing","Pest control","Lock & key","Sash window repairs","Minor electrical","Minor plumbing"] },
      { label: "Refurbishment & Fit-out",               keywords: ["refurb", "fit-out", "fitout", "renovation", "internal works", "conversion"],                                    subcategories: ["Refurbishment","Internal fit-out","Extensions","Classroom refurbishment","Toilet refurbishment","Kitchen refurbishment","Office fit-out","Strip-out & demolition","Major refurbishment","Window replacement","Door replacement","Flooring renewal","Bathroom pods","Accessible adaptations","Cosmetic upgrades","Loft conversion","Outbuilding conversion"] },
      { label: "Roofing, Windows & Building Fabric",    keywords: ["roofing", "window", "cladding", "brickwork", "flooring", "ceiling", "glazing"],                                 subcategories: ["Roofing works","Windows & doors","Cladding","Brickwork","Flooring","Ceilings & partitions","Painting & decorating","Flat roof replacement","Pitched roof repair","Fascias & soffits","UPVC replacement","Sash restoration","Curtain walling","Stonework","Pointing & repointing","External rendering","Leadwork","Roof inspection"] },
      { label: "M&E, Plumbing & Electrical",            keywords: ["mechanical", "electrical", "plumbing", "hvac", "ventilation", "heating", "boiler", "lift maintenance"],          subcategories: ["Plumbing","Heating & boilers","Electrical works","HVAC","Ventilation","Fire alarms","Lift maintenance","Hot water systems","Underfloor heating","Air conditioning","Switchgear","EV charging points","Solar panels","Mechanical ventilation","Data cabling","Emergency lighting","Sprinkler systems","Consumer units"] },
      { label: "Fire Safety, Compliance & Remediation", keywords: ["fire safety", "asbestos", "legionella", "remediation", "fire door", "fire stopping", "fire risk", "aov"],         subcategories: ["Fire safety works","Fire doors","Asbestos removal","Legionella & water hygiene","Electrical testing","Emergency lighting","Access control & CCTV","Fire risk assessment","ACM cladding removal","EWS1 surveys","Waking watch","Compartmentalisation","Cavity barriers","Fire stopping","Smoke detectors","AOV systems","Fire suppression"] },
      { label: "Decarbonisation & Retrofit",            keywords: ["retrofit", "decarb", "energy efficiency", "solar", "heat pump", "led upgrade", "net zero", "insulation"],       subcategories: ["Energy efficiency","Retrofit works","Heat pumps","Solar PV","LED upgrades","Insulation","Net zero works","Air source heat pumps","Ground source heat pumps","Cavity wall insulation","Solid wall insulation","Loft insulation","PAS 2035 retrofit","EPC improvements","Triple glazing","Demand response"] },
      { label: "Grounds, Civils & External Works",      keywords: ["grounds", "civil", "drainage", "surfacing", "landscaping", "fencing", "car park", "playground", "footpath"],    subcategories: ["Drainage","Surfacing","Landscaping","Fencing","Car parks","Playgrounds","Footpaths","Tarmac resurfacing","Block paving","Boundary walls","Security fencing","Cycle shelters","Bin stores","Street furniture","Retaining walls","Kerbing","SUDS drainage","Attenuation tanks"] },
      { label: "Supplies, Materials & Hire",            keywords: ["materials", "supplies", "plant hire", "tool hire", "scaffolding", "welfare cabin", "building materials"],        subcategories: ["Building materials","Plumbing supplies","Electrical supplies","Plant hire","Tool hire","Scaffolding","Welfare cabins","Timber & joinery","Ironmongery","Fixings & fasteners","Paint & coatings","Insulation materials","Roof materials","Floor coverings","Aggregates","Ready-mixed concrete","Skip hire","Temporary electrics"] },
      { label: "Professional Services & Consultancy",   keywords: ["consultancy", "surveying", "construction project management", "architectural services", "quantity surveying", "structural engineering", "clerk of works"],  subcategories: ["Quantity surveying","Project management","Building surveying","Architectural services","Structural engineering","Clerk of works","Estate strategy","CDM coordination","Principal designer","Planning consultancy","Fire engineering","Mechanical design","Electrical design","Party wall surveying","Condition surveys","Asset management","Energy consultancy","Due diligence"] },
    ]
  },
  {
    slug: "facilities",
    label: "Facilities",
    standfirst: "Hard and soft FM, mechanical and electrical maintenance, and managed services across the public estate.",
    live: true,
    pinnedProfile: intakeSchema.parse({
      companyName: "GovRevenue Desk",
      mainServices: "facilities management hard FM soft FM mechanical electrical maintenance managed services",
      idealBuyers: "local authorities NHS trusts central government",
      mainGoal: "find facilities management contracts"
    }),
    categories: [
      { label: "Hard FM",             keywords: ["hard fm", "mechanical", "electrical", "boiler", "heating", "hvac", "lift"],                    subcategories: ["Mechanical services","Electrical services","Heating systems","Boiler maintenance","HVAC","Lift maintenance","BMS"] },
      { label: "Soft FM",             keywords: ["soft fm", "cleaning", "catering", "security", "portering", "reception", "waste"],              subcategories: ["Cleaning","Catering","Security","Portering","Reception","Waste management","Mail room"] },
      { label: "Managed Services",    keywords: ["total fm", "integrated fm", "outsourced fm", "facilities management", "facilities contract"],  subcategories: ["Total FM","Integrated FM","Outsourced FM","TUPE transfers","KPI management"] },
      { label: "Energy Management",   keywords: ["energy management", "energy efficiency", "energy services", "utilities management", "utility metering", "carbon reduction"],   subcategories: ["Energy procurement","Utilities management","Smart metering","Carbon reporting","Sustainability"] },
      { label: "Compliance & Safety", keywords: ["statutory compliance", "fire safety", "asbestos", "legionella", "water treatment", "pat testing"],      subcategories: ["Fire safety","Asbestos management","Legionella control","Water treatment","PAT testing","Statutory compliance"] },
    ]
  },
  {
    slug: "education",
    label: "Education & Skills",
    standfirst: "Schools, further education, skills and training procurement across local authorities, academy trusts, DfE and colleges.",
    live: true,
    pinnedProfile: intakeSchema.parse({
      companyName: "GovRevenue Desk",
      mainServices: "school refurbishment education technology learning management training skills courses further education academy",
      idealBuyers: "local authorities academy trusts Department for Education further education colleges",
      mainGoal: "find education and skills contracts"
    }),
    categories: [
      { label: "School Buildings & Estates", keywords: ["school", "academy", "classroom", "education premises", "school refurb"],            subcategories: ["School refurbishment","RAAC remediation","Classroom fit-out","School expansion","Academy conversion","Caretaking","DDA works","Site security","Playground equipment","School gates"] },
      { label: "Education Technology",       keywords: ["edtech", "education technology", "learning platform", "mis system", "school software", "vle", "lms"], subcategories: ["MIS systems","Learning platforms","VLE","Interactive displays","Broadband in schools","IT hardware","Digital literacy","Tablets & devices","Filtering software","Staff training tech"] },
      { label: "Training & Skills",          keywords: ["training", "apprenticeship", "upskilling", "skills", "workforce development", "nvq", "cpd"],          subcategories: ["Apprenticeships","NVQ delivery","Leadership training","Digital skills","Workforce development","CPD programmes","Bootcamps","Functional skills","Sector-based work academies"] },
      { label: "SEND & Alternative Provision", keywords: ["send support", "send provision", "special educational needs support", "alternative provision", "pupil referral", "ehcp"],  subcategories: ["SEND support","Alternative provision","Pupil referral units","Educational psychology","Specialist tutoring","EHCP provision","Short breaks","Post-16 SEND"] },
      { label: "Further & Higher Education", keywords: ["further education", "fe college", "higher education", "university", "adult education"],               subcategories: ["FE college services","HE procurement","Adult education budget","T-levels","Higher Technical Qualifications","Skills bootcamps","Multiply numeracy","ESOL provision"] },
    ]
  },
  {
    slug: "transport",
    label: "Transport & SEND",
    standfirst: "Passenger transport, home-to-school travel, and SEND transport commissioned by councils across England and Wales.",
    live: true,
    pinnedProfile: intakeSchema.parse({
      companyName: "GovRevenue Desk",
      mainServices: "passenger transport SEND home to school transport community transport special educational needs",
      idealBuyers: "local authorities councils transport authorities",
      mainGoal: "find passenger transport and SEND contracts"
    }),
    categories: [
      { label: "SEND Transport",              keywords: ["send", "special educational needs", "home to school", "school transport", "children with"],  subcategories: ["SEND home-to-school","Post-16 SEND","Short breaks transport","SEN vehicle provision"] },
      { label: "Passenger Transport",         keywords: ["passenger transport", "bus", "community transport", "minibus", "dial-a-ride"],               subcategories: ["Bus services","Community transport","Dial-a-ride","Ring & ride","Accessible transport"] },
      { label: "Fleet & Vehicle Management",  keywords: ["fleet", "vehicle", "taxi", "accessible vehicle", "wheelchair"],                             subcategories: ["Fleet management","Taxi commissioning","Wheelchair-accessible vehicles","Vehicle maintenance"] },
      { label: "Rail & Specialist",           keywords: ["rail", "coach", "specialist transport", "escort", "accompany"],                             subcategories: ["Rail contracts","Coach hire","Escorted journeys","School crossings"] },
    ]
  },
  {
    slug: "recruitment",
    label: "Recruitment",
    standfirst: "Temporary and permanent staffing frameworks across the NHS, councils, and central government.",
    live: true,
    pinnedProfile: intakeSchema.parse({
      companyName: "GovRevenue Desk",
      mainServices: "recruitment temporary staffing agency workers permanent placement managed service provider",
      idealBuyers: "NHS trusts local councils central government departments",
      mainGoal: "find recruitment and staffing contracts"
    }),
    categories: [
      { label: "Clinical & Medical",    keywords: ["nursing", "medical", "doctor", "clinical", "healthcare professional", "locum"],         subcategories: ["Nursing","Medical locums","Allied health","Healthcare assistants","Band 5–7 nursing","Specialist clinical"] },
      { label: "Social Work & Care",    keywords: ["social work", "social care", "care worker", "support worker", "children services"],     subcategories: ["Social workers","Children services","Adult social care","Support workers","AMHP","IRO"] },
      { label: "Education Staffing",    keywords: ["teacher", "teaching", "supply teacher", "education", "school staff"],                  subcategories: ["Supply teaching","Teaching assistants","SEN support","School leadership","Education admin"] },
      { label: "Admin & Corporate",     keywords: ["admin", "clerical", "office", "secretarial", "finance", "hr staff"],                   subcategories: ["Admin & clerical","Finance officers","HR professionals","Project coordinators","PA/EA"] },
      { label: "Technical & IT",        keywords: ["it staff", "technical", "developer", "engineering", "digital"],                        subcategories: ["IT contractors","Software developers","Data analysts","Digital specialists","Engineers"] },
    ]
  },
  {
    slug: "frameworks",
    label: "Frameworks",
    standfirst: "Open frameworks, Dynamic Purchasing Systems, and call-off routes across all public sectors — the fastest route to market for most SMEs.",
    live: true,
    pinnedProfile: intakeSchema.parse({
      companyName: "GovRevenue Desk",
      mainServices: "framework agreement dynamic purchasing system DPS call-off contract multi-provider",
      idealBuyers: "Crown Commercial Service local authorities NHS central government",
      mainGoal: "find framework and DPS opportunities"
    }),
    categories: [
      { label: "Construction Frameworks",         keywords: ["construction framework", "works framework", "jct", "nec"],                       subcategories: ["Works frameworks","NEC contracts","JCT frameworks","Minor works","Capital delivery"] },
      { label: "Professional Services",           keywords: ["professional services", "consultancy framework", "advisory"],                    subcategories: ["Consultancy","Project management","Legal","Finance advisory","HR advisory"] },
      { label: "IT & Digital",                    keywords: ["digital framework", "technology framework", "ict", "g-cloud", "digital outcomes"], subcategories: ["G-Cloud","Digital Outcomes","Cyber","Hosting","Software"] },
      { label: "Supplies & Goods",                keywords: ["supplies framework", "goods framework", "catalogue", "purchase order"],          subcategories: ["Office supplies","Medical consumables","FM materials","PPE","Catering supplies"] },
      { label: "Dynamic Purchasing Systems",      keywords: ["dynamic purchasing", "dps", "dynamic market"],                                   subcategories: ["Open DPS","Construction DPS","Temporary staffing DPS","Transport DPS"] },
    ]
  },
  {
    slug: "health",
    label: "Health & NHS",
    standfirst: "NHS trusts, integrated care boards, community health, mental health, and public health commissioning across England.",
    live: true,
    pinnedProfile: intakeSchema.parse({
      companyName: "GovRevenue Desk",
      mainServices: "NHS healthcare services integrated care clinical commissioning mental health community health public health primary care GP services",
      idealBuyers: "NHS trusts integrated care boards ICBs NHS England clinical commissioning groups public health teams",
      mainGoal: "find NHS and health commissioning contracts"
    }),
    categories: [
      { label: "Acute & Hospital Services",         keywords: ["hospital", "acute trust", "secondary care", "nhs trust", "surgical", "diagnostics", "pathology"],         subcategories: ["Surgical services","Diagnostic imaging","Pathology","Outpatient services","Pharmacy supplies","Medical equipment","Hospital cleaning","Patient transport","Catering (acute)","Sterile services","Ward supplies","Prosthetics","Physiotherapy (acute)","Occupational therapy","Speech & language therapy"] },
      { label: "Mental Health & Talking Therapies",  keywords: ["mental health", "talking therapies", "iapt", "counselling", "psychological", "wellbeing", "mhst"],        subcategories: ["IAPT/talking therapies","Crisis services","Community mental health teams","Child & adolescent mental health (CAMHS)","Eating disorders","Perinatal mental health","Forensic mental health","Mental health workforce","Supported accommodation (MH)","Recovery & employment support","Advocacy services"] },
      { label: "Community & Primary Care",           keywords: ["primary care", "community health", "gp", "pcn", "primary care network", "district nursing", "health visiting"], subcategories: ["GP services","District nursing","Health visiting","Community physiotherapy","Community podiatry","Community cardiology","Primary care IT systems","Remote monitoring","Care navigation","Pharmacy (community)","Immunisation programmes","Cervical screening"] },
      { label: "Public Health Commissioning",        keywords: ["public health", "health improvement", "prevention", "sexual health", "substance misuse", "tobacco", "obesity", "smoking cessation"], subcategories: ["Sexual health services","Stop smoking","Drug & alcohol services","Obesity & weight management","Health improvement programmes","Screening programmes","Epidemiology & surveillance","Healthy start schemes","Falls prevention","Social prescribing","Health inequalities"] },
      { label: "Health Technology & Digital",        keywords: ["health technology", "nhs digital", "electronic patient record", "epr", "clinical system", "health informatics", "patient management"], subcategories: ["Electronic patient records (EPR)","Clinical decision support","Patient flow systems","NHS app integration","Wearables & remote monitoring","Health data analytics","Cyber security (NHS)","Clinical coding","Workforce management systems","Telemedicine","AI diagnostics"] },
      { label: "Care Commissioning & Social Care",   keywords: ["care home", "residential care", "domiciliary", "supported living", "adult social care", "reablement", "extra care"], subcategories: ["Domiciliary care","Residential care homes","Nursing homes","Supported living","Extra care housing","Reablement services","Learning disability services","Autistic spectrum (residential)","Discharge-to-assess","Personal assistants","Direct payments support","Carer support services"] },
    ]
  },
  { slug: "digital", label: "Digital & IT", standfirst: "Cloud, software, cyber security, networks, and digital transformation across the NHS, councils, and central government.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "IT services software cloud cyber security digital transformation infrastructure managed services", idealBuyers: "NHS trusts local authorities central government HMRC DVLA", mainGoal: "find IT and digital procurement contracts" }),
    categories: [
      { label: "Cloud & Hosting", keywords: ["cloud", "hosting", "g-cloud", "saas", "iaas", "paas", "azure", "aws"], subcategories: ["Cloud hosting","SaaS licensing","IaaS platforms","Azure / AWS","Disaster recovery","Managed cloud","Backup & storage","CDN services","Virtual desktop","Data centre colocation","Cloud migration","Hybrid cloud"] },
      { label: "Software & Licensing", keywords: ["software", "licence", "crm", "erp system", "mis system", "software platform", "case management system"], subcategories: ["ERP systems","CRM platforms","MIS systems","HR software","Finance systems","Document management","Case management","GIS systems","Asset management software","Email & productivity","Planning software","Workforce management"] },
      { label: "Cyber Security", keywords: ["cyber", "security operations", "penetration testing", "vulnerability assessment", "endpoint protection", "siem"], subcategories: ["Penetration testing","SOC services","Endpoint protection","Vulnerability assessment","SIEM platforms","GDPR compliance","Identity & access","Phishing simulation","Data loss prevention","Incident response","Threat intelligence","Security awareness training"] },
      { label: "Networks & Infrastructure", keywords: ["network", "it infrastructure", "ict infrastructure", "digital infrastructure", "broadband", "wifi", "connectivity", "fibre", "telecoms"], subcategories: ["Network infrastructure","Wi-Fi deployment","Connectivity services","Fibre installation","Telecoms","WAN/LAN","SD-WAN","Unified communications","Telephony systems","Public Wi-Fi","Smart city connectivity","MPLS"] },
      { label: "IT Support & Managed Services", keywords: ["it support", "managed service", "service desk", "helpdesk", "desktop support", "device"], subcategories: ["Service desk","Desktop support","Device procurement","Print management","ITSM platforms","IT outsourcing","Field support","Asset lifecycle","Patch management","IT training","Mobile device management","Field engineering"] },
      { label: "Digital Transformation", keywords: ["digital transformation", "agile", "user research", "ux design", "discovery", "data strategy"], subcategories: ["Digital strategy","User research","UX/UI design","Service design","Data strategy","Analytics platforms","Business intelligence","AI & automation","RPA","Open data","API development","Accessibility compliance"] },
    ]
  },
  { slug: "social-care", label: "Adult Social Care", standfirst: "Domiciliary care, residential placements, learning disability, reablement, and carer support commissioned by councils and ICBs.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "adult social care domiciliary care residential care learning disability reablement supported living carer support", idealBuyers: "local authorities councils integrated care boards NHS England", mainGoal: "find adult social care commissioning contracts" }),
    categories: [
      { label: "Domiciliary & Home Care", keywords: ["domiciliary", "home care", "personal care", "home help", "care at home"], subcategories: ["Personal care","Domestic assistance","Medication support","Companionship","Night sits","Live-in care","Reablement support","Emergency home care","Direct payments support","Carers' assessments","Overnight care","Sensory impairment support"] },
      { label: "Residential & Nursing Care", keywords: ["residential care", "nursing home", "care home", "residential placement", "elderly care"], subcategories: ["Residential placements","Nursing placements","Dementia specialist","End-of-life care","Respite residential","EMI beds","Enhanced nursing","Intermediate residential","Frailty services","Specialist residential"] },
      { label: "Learning Disability Services", keywords: ["learning disability", "autism", "challenging behaviour", "complex needs"], subcategories: ["Supported living (LD)","Residential (LD)","Day services","Community support","Behaviour support","Autism services","Transition support","Short breaks (LD)","Advocacy","Hospital discharge (LD)","Forensic LD","Positive behaviour support"] },
      { label: "Mental Health Support", keywords: ["mental health support", "community mental health", "peer support", "recovery", "crisis support"], subcategories: ["Community mental health","Crisis support","Peer support","Wellbeing services","Recovery support","Employment support (MH)","Floating support (MH)","Dual diagnosis","IAPT delivery","Advocacy (MH)","Housing-related support","Personalised care"] },
      { label: "Reablement & Intermediate Care", keywords: ["reablement", "intermediate care", "step-down", "hospital discharge", "discharge support"], subcategories: ["Reablement services","Intermediate care","Hospital discharge","Step-down care","Falls prevention","Telecare","Care technology","Community rehabilitation","Virtual ward support","Extra care housing","Re-enablement","Assistive technology"] },
      { label: "Carer Support", keywords: ["carer support", "carers service", "young carer", "unpaid carer", "carer assessment"], subcategories: ["Carer assessment","Young carers","Carer breaks","Information & advice","Carer training","Emergency carer cover","Sitting service","Carer wellbeing","Support groups","Unpaid carer support","Carer-related respite","Self-directed support"] },
    ]
  },
  { slug: "childrens", label: "Children's Services", standfirst: "Early years, fostering, CAMHS, child protection, short breaks, and youth services commissioned by local authorities.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "children services early years fostering adoption CAMHS child protection youth services short breaks safeguarding", idealBuyers: "local authorities children's services departments family hubs NHS trusts", mainGoal: "find children's services commissioning contracts" }),
    categories: [
      { label: "Early Years & Childcare", keywords: ["early years", "childcare", "nursery", "sure start", "family hub", "children centre"], subcategories: ["Childcare sufficiency","Nursery provision","Family hubs","Children's centres","Two-year-old funding","Early intervention","Parent support","Home learning","Speech therapy (EY)","Portage","Stay & play","Perinatal support"] },
      { label: "Fostering & Adoption", keywords: ["fostering", "adoption", "foster carer", "looked after children", "kinship", "permanence"], subcategories: ["Foster carer recruitment","Foster carer support","Adoption services","Kinship care","SGO support","Therapeutic support","Post-adoption support","Residential (LAC)","Semi-independent (LAC)","Reunification support","Independent fostering","Virtual school"] },
      { label: "CAMHS & Child Mental Health", keywords: ["camhs", "child mental health", "young people mental health", "emotional wellbeing"], subcategories: ["CAMHS Tier 2","CAMHS Tier 3","Crisis CAMHS","ADHD assessment","Autism assessment (children)","School-based counselling","Eating disorders (CYPMH)","Perinatal CAMHS","Transition to adult MH","Early help mental health","Therapeutic assessment","MHST"] },
      { label: "Child Protection & Safeguarding", keywords: ["child protection", "mash", "safeguarding", "looked after", "leaving care", "edge of care"], subcategories: ["Safeguarding support","MASH services","Child protection reviews","IRO services","Leaving care support","Staying put","Personal advisers","Pathway planning","UASC support","Edge of care","Child exploitation","Contextual safeguarding"] },
      { label: "Short Breaks & Respite", keywords: ["short break", "respite", "disabled children", "overnight break", "holiday playscheme"], subcategories: ["Overnight short breaks","Holiday playschemes","Saturday clubs","Sitting services","Befriending","Specialist short breaks","Residential respite","Community short breaks","Carers' short breaks (children)","Playschemes","Family support","Intensive support"] },
      { label: "Youth Services", keywords: ["youth service", "youth work", "young people", "detached youth", "youth club", "mentoring"], subcategories: ["Youth clubs","Detached youth work","Positive activities","Duke of Edinburgh","Youth offending support","Gangs intervention","County lines response","Youth mentoring","Sports & wellbeing","Holiday hunger","Youth justice prevention","Employability (young people)"] },
    ]
  },
  { slug: "waste", label: "Waste & Environment", standfirst: "Waste collection, recycling, street cleansing, grounds maintenance, and environmental monitoring for councils and public bodies.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "waste collection recycling street cleansing grounds maintenance environmental monitoring composting", idealBuyers: "local authorities district councils county councils waste authorities", mainGoal: "find waste management and environmental services contracts" }),
    categories: [
      { label: "Waste Collection & Logistics", keywords: ["waste collection", "refuse", "bin collection", "bulky waste", "clinical waste", "commercial waste"], subcategories: ["Domestic refuse","Recycling collection","Bulky waste","Clinical waste","Commercial waste","Food waste","Garden waste","Hazardous waste","Specialist collection","Skip hire","Fly-tip removal","Street litter bins"] },
      { label: "Recycling & Treatment", keywords: ["recycling", "treatment", "materials recovery", "mrf", "composting", "anaerobic digestion"], subcategories: ["Materials recovery facilities","Composting","Anaerobic digestion","Energy from waste","Glass processing","Metals recycling","Plastics processing","Electrical waste (WEEE)","Wood recycling","Textile reuse","Waste transfer stations","Sorting plants"] },
      { label: "Street Cleansing & Grounds", keywords: ["street cleansing", "grounds maintenance", "litter", "sweeping", "grass cutting"], subcategories: ["Street sweeping","Litter picking","Grass cutting","Floral displays","Tree surgery","Hedge trimming","Weed control","Graffiti removal","Gully cleaning","Road sweeping","Pavement cleaning","Cemetery maintenance"] },
      { label: "Environmental Monitoring", keywords: ["environmental monitoring", "air quality", "noise monitoring", "contaminated land", "ecology"], subcategories: ["Air quality monitoring","Noise monitoring","Contaminated land surveys","Ecological surveys","Water quality","Environmental impact","Carbon accounting","Flood risk","Tree surveys","Biodiversity net gain","Land remediation","Environmental audits"] },
      { label: "Waste Infrastructure & Equipment", keywords: ["waste equipment", "refuse vehicle", "recycling vehicle", "compactor", "container", "recycling centre", "hwrc"], subcategories: ["Refuse vehicles","Recycling vehicles","Compactors","Wheeled bins","Containers","HWRCs","Civic amenity sites","Skip lorries","Bin sensors","Smart waste technology","Solar compactors","Underground bins"] },
    ]
  },
  { slug: "energy", label: "Energy & Utilities", standfirst: "Energy procurement, decarbonisation, smart metering, EV charging, heat networks, and net zero strategy across the public estate.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "energy procurement decarbonisation solar heat pump retrofit smart metering EV charging net zero sustainability", idealBuyers: "local authorities NHS trusts central government housing associations", mainGoal: "find energy and decarbonisation contracts" }),
    categories: [
      { label: "Energy Procurement", keywords: ["energy procurement", "gas contract", "electricity contract", "utilities", "energy buying"], subcategories: ["Electricity supply","Gas supply","Energy framework","Flexible purchasing","Renewable energy","Energy brokering","Half-hourly metering","Water supply","Telecoms contracts","Fuel supply","Energy data management","Utility management"] },
      { label: "Decarbonisation & Retrofit", keywords: ["decarbonisation", "retrofit", "solar", "heat pump", "insulation", "net zero", "low carbon"], subcategories: ["Solar PV","Heat pumps","Insulation works","LED upgrades","Voltage optimisation","Battery storage","EPC improvements","PAS 2035 retrofit","Net zero strategy","Carbon offsetting","Biomass heating","Fabric first retrofit"] },
      { label: "Smart Metering & Monitoring", keywords: ["smart meter", "metering", "bms", "energy monitoring", "sub-metering", "iso 50001"], subcategories: ["Smart meters","AMR metering","BMS upgrades","Energy monitoring","Sub-metering","ISO 50001 support","Carbon reporting","Tariff optimisation","Real-time dashboards","Automated meter reading","M&T systems","Energy certificates"] },
      { label: "EV Charging & Transport", keywords: ["ev charging", "electric vehicle", "charge point", "ulev", "zero emission"], subcategories: ["EV charge points","Public charging network","Fleet charging","Ultra-low emission","EV strategy","Charge point management","Rapid charging hubs","On-street charging","Fleet decarbonisation","E-bike infrastructure","EV grid balancing","Workplace charging"] },
      { label: "Heat Networks & District Energy", keywords: ["heat network", "district heating", "communal heating", "heat interface", "dh scheme"], subcategories: ["Heat network design","District heating","Communal heating","Heat interface units","ESCO contracts","Biomass district","Network metering","Connection agreements","Hydraulic modelling","Heat offtake","Heat pumps (district)","Thermal storage"] },
      { label: "Energy Consultancy", keywords: ["energy consultancy", "energy management", "carbon strategy", "sustainability", "energy audit"], subcategories: ["Energy audits","Carbon strategy","Sustainability reporting","SECR compliance","Energy management","Green fleet strategy","Climate action plans","Scope 3 emissions","Net zero roadmaps","Public sector decarbonisation","BREEAM assessment","Lifecycle carbon"] },
    ]
  },
  { slug: "security", label: "Security", standfirst: "Manned guarding, CCTV, access control, event security, and lone worker protection for public sector sites and estates.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "security guarding manned guarding CCTV access control event security lone worker key holding patrol", idealBuyers: "local authorities NHS trusts central government universities", mainGoal: "find security services contracts" }),
    categories: [
      { label: "Manned Guarding", keywords: ["manned guarding", "security guard", "static guard", "concierge security", "front of house"], subcategories: ["Static guarding","Mobile patrols","Concierge security","Front of house","Car park security","Site security","Night security","Customer service officers","Portering (security)","High-risk guarding","Gatehouse","Foot patrols"] },
      { label: "CCTV & Surveillance", keywords: ["cctv", "surveillance", "camera", "control room cctv", "video analytics", "monitoring"], subcategories: ["CCTV installation","Control room","Remote monitoring","Body-worn cameras","ANPR","Video analytics","Drone surveillance","Night vision","Town centre CCTV","Public space surveillance","PTZ cameras","Thermal imaging"] },
      { label: "Access Control & Intruder", keywords: ["access control", "intruder alarm", "door entry", "biometric", "turnstile", "barrier"], subcategories: ["Door entry systems","Biometric access","Turnstiles","Barriers & bollards","Intruder alarms","Intercom systems","Card readers","Visitor management","Locker systems","Key management","Smart locks","Perimeter protection"] },
      { label: "Event Security", keywords: ["event security", "crowd management", "steward", "venue security", "festival security"], subcategories: ["Event stewarding","Crowd management","Venue security","Search teams","Traffic management","Festival security","Marathon security","Protest management","Sports event security","Music venue security","Temporary structures","Emergency evacuation"] },
      { label: "Lone Worker & Key Holding", keywords: ["lone worker", "key holding", "alarm response", "remote monitoring lone", "personal protection"], subcategories: ["Lone worker protection","Key holding","Alarm response","Personal attack alarms","Remote monitoring","Emergency response","SLA response","Out-of-hours security","Retail security","Healthcare lone worker","Social worker safety"] },
      { label: "Investigative Security", keywords: ["investigation", "fraud investigation", "vetting", "background check", "due diligence security"], subcategories: ["Background vetting","BS7858 screening","Fraud investigation","Counter surveillance","Due diligence","Risk assessment","Covert surveillance","Whistleblowing services","Interviewing","Asset tracing"] },
    ]
  },
  { slug: "catering", label: "Catering & Food", standfirst: "School meals, hospital catering, care home catering, prison catering, vending, and food equipment across the public sector.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "catering school meals hospital catering care home meals on wheels vending food services kitchen", idealBuyers: "local authorities NHS trusts schools academies prison service care homes", mainGoal: "find catering and food services contracts" }),
    categories: [
      { label: "School Meals & Education Catering", keywords: ["school meals", "school catering", "school kitchen", "breakfast club", "universal infant"], subcategories: ["Primary school meals","Secondary school meals","Universal infant free meals","Breakfast clubs","Kitchen management","Catering staff","Allergen management","Food education","Healthy school meals","Academy catering","School tuck shops","Packed lunch alternatives"] },
      { label: "Hospital & NHS Catering", keywords: ["hospital catering", "nhs catering", "patient meals", "ward food", "clinical nutrition"], subcategories: ["Patient meals","Ward trolley service","Cook-chill meals","Texture-modified meals","Clinical nutrition","Specialist diets","Cafe concessions","Staff restaurant","Vending (NHS)","Retail outlets (NHS)","Allergen control (NHS)","Bedside catering"] },
      { label: "Care Home & Residential Catering", keywords: ["care home catering", "residential catering", "meals on wheels", "community meals"], subcategories: ["Residential catering","Meals on wheels","Community meals","Luncheon clubs","Frozen meals delivery","Dementia-friendly meals","Pureed meals","Cultural menus","Nutritional support","Cook-at-home delivery","Snack provision","Specialist diet catering"] },
      { label: "Prison & Custody Catering", keywords: ["prison catering", "custody catering", "custodial catering", "hmps", "detention centre"], subcategories: ["Prison meals","Custody suite catering","Detention centre catering","Halal and Kosher provision","Dietary management","Food education (custody)","Canteen management","Prison kitchen equipment","Vending (custody)","Nutritional compliance"] },
      { label: "Vending & Hospitality", keywords: ["vending", "hospitality", "conference catering", "refreshment", "barista", "coffee machine"], subcategories: ["Hot drinks vending","Snack vending","Coffee concessions","Meeting room catering","Conference catering","Mobile catering","Pop-up kiosks","Barista services","Smart fridges","Water dispensers","Catering management","Events hospitality"] },
      { label: "Catering Equipment & Supplies", keywords: ["catering equipment", "kitchen equipment", "food supplies", "food procurement", "disposables"], subcategories: ["Kitchen equipment","Catering smallwares","Food procurement","Disposables","Cleaning chemicals (kitchen)","Cold storage","Dishwashers","Combi ovens","Regeneration trolleys","Serving equipment","Refrigeration","PPE (kitchen)"] },
    ]
  },
  { slug: "legal", label: "Legal & Professional", standfirst: "Legal services, external audit, internal audit, HR advisory, and management consultancy for local authorities, NHS, and central government.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "legal services solicitor audit HR advisory procurement consultancy management consultancy compliance", idealBuyers: "local authorities NHS trusts central government housing associations", mainGoal: "find legal and professional services contracts" }),
    categories: [
      { label: "Legal Services", keywords: ["legal services", "solicitor", "barrister", "counsel", "litigation", "planning legal"], subcategories: ["Property and conveyancing","Planning and environment","Litigation","Employment law","Contract law","Information law (FOI/DPA)","Public law","Child law","Adult social care law","Procurement law","Regulatory","Debt recovery","GDPR legal","Equality law"] },
      { label: "External Audit", keywords: ["external audit", "financial audit", "statutory audit", "value for money", "audit firm"], subcategories: ["Statutory audit","Financial statements audit","Value for money review","Grant certification","Academy trust audit","Charity audit","Pension fund audit","Housing association audit","NHS audit","Auditor appointment","Assurance reviews","Interim audit"] },
      { label: "Internal Audit & Risk", keywords: ["internal audit", "risk management", "counter fraud", "corporate governance", "assurance"], subcategories: ["Internal audit service","Counter fraud","Risk management","Corporate governance","Assurance mapping","Data quality audit","IT audit","Compliance monitoring","Anti-bribery compliance","Whistleblowing","Risk register","Internal controls"] },
      { label: "HR & Employment Advisory", keywords: ["hr advisory", "employment advisory", "occupational health", "mediation", "workforce advisory"], subcategories: ["HR advisory","Employment relations","Occupational health","Mediation","Pay benchmarking","Job evaluation","Workforce planning","Redundancy support","TUPE advisory","HR transformation","Equality and diversity","Wellbeing at work"] },
      { label: "Finance & Procurement Advisory", keywords: ["finance advisory", "procurement consultancy", "commercial advisory", "treasury advisory"], subcategories: ["Finance transformation","Procurement consultancy","Commercial advisory","Treasury management","Shared services","Accounts payable","Payroll","Business rates","Grant management","Financial modelling","VFM reviews","Category management"] },
      { label: "Management Consultancy", keywords: ["management consultancy", "business case", "options appraisal", "process improvement", "lean"], subcategories: ["Options appraisal","Business cases","Process improvement","Lean and Six Sigma","Service redesign","Operating model","Benchmarking","Programme assurance","Benefits realisation","Post-project review","Change management","OD consultancy"] },
    ]
  },
  { slug: "housing-support", label: "Housing & Homelessness", standfirst: "Homelessness prevention, rough sleeping, temporary accommodation, refuge, floating support, and housing development commissioned by councils.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "homelessness prevention rough sleeping temporary accommodation refuge floating support housing advice tenancy sustainment", idealBuyers: "local authorities district councils housing authorities combined authorities", mainGoal: "find housing support and homelessness contracts" }),
    categories: [
      { label: "Homelessness Prevention", keywords: ["homelessness prevention", "housing options", "housing advice", "eviction prevention", "tenancy sustainment"], subcategories: ["Housing advice","Tenancy sustainment","Eviction prevention","Rent deposit schemes","Mediation","Private sector access","Crisis intervention","Housing first","Rapid rehousing","Housing-related support","Private rented access","Prevention teams"] },
      { label: "Rough Sleeping & Outreach", keywords: ["rough sleeping", "street outreach", "no second night out", "winter pressures", "street homelessness"], subcategories: ["Street outreach","No Second Night Out","Winter shelter","Assessment hubs","Reconnection","Rough sleeper initiative","Assertive outreach","Harm reduction","Alcohol and drug support (RS)","Personalised budgets","Rough sleeper co-ordination","Satellite accommodation"] },
      { label: "Temporary Accommodation", keywords: ["temporary accommodation", "nightly paid", "bed and breakfast", "hostel", "emergency accommodation"], subcategories: ["Bed and breakfast management","Nightly paid provision","Hostel management","Emergency accommodation","Move-on support","TA inspections","Supported lodgings","Night shelters","Cold weather provision","Family TA","Young people's TA","Procurement of TA"] },
      { label: "Refuge & Domestic Abuse", keywords: ["refuge", "domestic abuse", "domestic violence", "idva", "safeguarding domestic"], subcategories: ["Refuge provision","IDVA services","Community IDVA","MARAC support","Perpetrator programmes","Children's IDVA","DAHA accreditation","DASH risk assessment","SARC services","Stalking response","Specialist BAME refuge","Male victim support"] },
      { label: "Floating Support & Supported Housing", keywords: ["floating support", "housing related support", "tenancy support", "supported housing"], subcategories: ["Floating support","Tenancy support","Supported housing (general)","Ex-offender support","Veterans housing","Mental health floating support","Substance misuse support","Young people's housing","LGBTQ+ safe space","Complex needs housing","Step-down supported","Intensive housing"] },
      { label: "Housing Development & Strategy", keywords: ["affordable housing", "housing development", "housing needs", "viability assessment", "s106"], subcategories: ["Affordable housing delivery","Housing needs assessment","Viability appraisals","Section 106 management","Community land trusts","Housing strategy","HRA business plan","Modular housing","Self-build support","Housing data and analytics","Surplus land disposal","Build to rent"] },
    ]
  },
  { slug: "finance", label: "Finance & Audit", standfirst: "External audit, insurance, treasury, payroll, debt recovery, and financial systems for local authorities, NHS bodies, and housing associations.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "external audit internal audit insurance treasury management payroll debt recovery financial systems", idealBuyers: "local authorities NHS trusts housing associations central government pension funds", mainGoal: "find finance and audit contracts" }),
    categories: [
      { label: "Insurance Services", keywords: ["insurance", "liability insurance", "property insurance", "public liability", "employer liability"], subcategories: ["Public liability","Employer liability","Property insurance","Fleet insurance","Professional indemnity","Cyber insurance","Engineering insurance","Directors and officers","Terrorism cover","Captive insurance","Personal accident","Travel insurance"] },
      { label: "Banking & Treasury", keywords: ["banking", "treasury", "cash management", "investment", "borrowing", "money market"], subcategories: ["Bank accounts","Cash management","Short-term investment","PWLB borrowing","Treasury management system","Money market funds","Pooled investments","Debt management","Financial forecasting","Banking framework","Deposit management","Currency hedging"] },
      { label: "External Audit", keywords: ["external audit", "statutory audit", "value for money", "psaa", "audit appointment"], subcategories: ["Statutory audit","Value for money","PSAA appointees","Audit committee","Annual governance statement","External auditor appointment","Pension fund audit","Grant certification","Financial statements","Technical accounting"] },
      { label: "Payroll & Pensions", keywords: ["payroll", "pensions", "pension administration", "payroll bureau", "pension fund", "lgps"], subcategories: ["Payroll bureau","Pension administration","LGPS services","Teachers' pension","NHS pension","Auto-enrolment","P60/P11D","Real-time information","Salary sacrifice","Benefits administration","Payroll software","Actuarial services"] },
      { label: "Debt Recovery & Revenue", keywords: ["debt recovery", "council tax", "business rates", "parking fines", "revenue collection"], subcategories: ["Council tax collection","Business rates","Parking enforcement","Sundry debt","Housing benefit overpayment","Enforcement agents","Pre-litigation","Tracing services","Revenue systems","Benefits administration","Fraud detection","Revenues and benefits"] },
      { label: "Financial Systems & Reporting", keywords: ["financial system", "erp", "oracle", "sap", "agresso", "unit4", "accounts payable"], subcategories: ["Oracle/SAP/Agresso","Accounts payable","Accounts receivable","Financial reporting","Business intelligence","Consolidated accounts","IFRS 16","Group accounting","Statutory returns","CFO advisory","Finance transformation","Shared services finance"] },
    ]
  },
  { slug: "comms", label: "Marketing & Comms", standfirst: "Public health campaigns, council communications, PR, print, digital marketing, and public engagement across the public sector.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "public health campaigns council communications PR media relations print design digital marketing engagement consultation", idealBuyers: "local authorities NHS trusts public health teams central government", mainGoal: "find communications and marketing contracts" }),
    categories: [
      { label: "Public Health Campaigns", keywords: ["public health campaign", "health promotion", "behaviour change", "awareness campaign"], subcategories: ["Smoking cessation campaigns","Drug and alcohol campaigns","Sexual health campaigns","Mental health awareness","Obesity prevention","COVID communications","Vaccination campaigns","NHS recruitment","Emergency communications","Suicide prevention messaging","Physical activity promotion","NHS 111 awareness"] },
      { label: "Council Communications", keywords: ["council communications", "resident communications", "public engagement comms", "council consultation"], subcategories: ["Resident newsletters","Council website","Digital communications","Annual report","Budget consultation","Resident surveys","Neighbourhood comms","Ward briefings","Corporate publications","Intranet","Social media (council)","Corporate branding"] },
      { label: "PR & Media Relations", keywords: ["media relations", "press office", "public relations", "crisis comms", "reputation management"], subcategories: ["Press office","Spokesperson training","Crisis communications","Media monitoring","Stakeholder relations","Reputation management","Parliamentary affairs","Lobbying support","Social media PR","Broadcast PR","Press release writing","Media training"] },
      { label: "Print & Design", keywords: ["print management", "graphic design", "signage", "wayfinding", "publication", "brand identity"], subcategories: ["Graphic design","Brand identity","Print management","Signage and wayfinding","Exhibition materials","Annual reports","Leaflets and posters","Accessibility design","Large-format print","Corporate stationery","Environmental graphics","Translation and print"] },
      { label: "Digital Marketing & Media", keywords: ["digital marketing", "social media management", "seo", "paid media", "email marketing", "content strategy"], subcategories: ["Social media management","Paid search (PPC)","SEO","Email marketing","Content strategy","Video production","Animation","Podcasting","Influencer outreach","Digital analytics","Website development","App development"] },
      { label: "Consultation & Engagement", keywords: ["public consultation", "community engagement", "stakeholder engagement", "co-production", "citizen engagement", "participatory"], subcategories: ["Public consultation","Co-production","Citizen assemblies","Online engagement","Face-to-face events","Accessibility engagement","Community engagement","Hard-to-reach groups","Equalities consultation","Feedback analysis","Deliberative research","Participatory budgeting"] },
    ]
  },
  { slug: "leisure", label: "Leisure & Culture", standfirst: "Leisure centre management, libraries, arts, parks, sports development, and heritage for councils and public bodies.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "leisure management swimming pool sports centre library arts culture parks museums heritage sports development", idealBuyers: "local authorities district councils county councils leisure trusts", mainGoal: "find leisure and cultural services contracts" }),
    categories: [
      { label: "Leisure Management", keywords: ["leisure management", "leisure centre", "swimming pool", "sports centre", "gym", "leisure trust"], subcategories: ["Leisure centre management","Swimming pools","Sports halls","Fitness suites","Outdoor athletics","Dual-use facilities","Community sports","Leisure trust transfer","Pricing and tariff","Health referral","Disability sport","Outdoor education"] },
      { label: "Library Services", keywords: ["library", "lending", "library service", "book purchasing", "reading", "information service"], subcategories: ["Library management","Mobile libraries","Book purchasing","Self-service kiosks","Library IT systems","Reading groups","Rhyme time","Digital inclusion","Home delivery","Archive services","Library buildings","Community library"] },
      { label: "Arts & Culture", keywords: ["arts", "culture", "theatre", "gallery", "creative arts", "cultural programme", "arts development"], subcategories: ["Theatre management","Art gallery","Museum management","Heritage interpretation","Public art commissioning","Artist residencies","Arts development","Cultural programme","Community arts","Festival support","Arts fundraising","Cultural strategy"] },
      { label: "Parks & Open Spaces", keywords: ["parks", "open space", "playground", "outdoor recreation", "allotments", "nature reserve"], subcategories: ["Parks management","Play area maintenance","Allotments","Nature reserves","Sports pitches","Outdoor gym","Footpath maintenance","Countryside access","Urban green space","Biodiversity management","Ecology surveys","Green infrastructure"] },
      { label: "Sports Development", keywords: ["sports development", "active travel", "cycling", "walking", "healthy active"], subcategories: ["Sports development","Active travel","Cycling programmes","Walking networks","Active communities","Disability sport","School sport","Club development","Workforce development (sport)","National governing bodies","Swim England","Move More"] },
      { label: "Museums & Heritage", keywords: ["museum", "heritage", "conservation", "archive", "collection", "artefact"], subcategories: ["Museum collections","Conservation and restoration","Archive digitisation","Heritage consultancy","Listed building work","Archaeological surveys","Heritage at risk","Interpretation design","Loan services","Oral history","War memorial restoration","Historic environment"] },
    ]
  },
  { slug: "planning", label: "Planning & Regeneration", standfirst: "Planning consultancy, urban regeneration, economic development, heritage, transport planning, and land strategy for councils and combined authorities.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "planning consultancy urban regeneration economic development masterplanning heritage transport planning land development", idealBuyers: "local authorities combined authorities planning authorities Homes England", mainGoal: "find planning and regeneration contracts" }),
    categories: [
      { label: "Planning Consultancy", keywords: ["planning consultancy", "planning application", "local plan", "development management", "neighbourhood planning", "planning policy", "planning appeal"], subcategories: ["Local plan support","Development management","Planning applications","Pre-application advice","Appeals","Planning policy","Infrastructure delivery","Community infrastructure levy","Neighbourhood planning","Planning enforcement","Viability assessments","EIA coordination"] },
      { label: "Urban Regeneration", keywords: ["regeneration", "urban regeneration", "town centre", "masterplan", "place making", "urban renewal", "levelling up", "high street"], subcategories: ["Town centre regeneration","Masterplanning","Place making","High street recovery","Levelling Up programmes","UKSPF delivery","Heritage-led regeneration","Housing-led regeneration","Industrial site reclamation","Compulsory purchase","Vacant buildings","Business improvement districts"] },
      { label: "Economic Development", keywords: ["economic development", "inward investment", "business support", "enterprise", "growth hub"], subcategories: ["Inward investment","Business support","Enterprise zones","Growth hubs","Employment land","Skills and employment","Business rates incentives","Trade missions","Economic impact","Supply chain development","Innovation hubs","Start-up support"] },
      { label: "Heritage & Conservation", keywords: ["heritage conservation", "listed building", "historic", "building survey conservation", "conservation area"], subcategories: ["Heritage surveys","Conservation area appraisals","Listed building advice","Historic environment","Archaeology","Building recording","Heritage impact","SMR support","Historic landscape","War memorial restoration","Heritage at risk","Grant-aided works"] },
      { label: "Transport Planning", keywords: ["transport planning", "transport assessment", "traffic", "active travel", "movement strategy"], subcategories: ["Transport assessment","Traffic modelling","Active travel plans","Parking strategy","Travel plans","LTP development","Bus strategy","Cycling and walking","Road safety audits","Transport impact assessment","Vision Zero","Freight strategy"] },
      { label: "Property & Land", keywords: ["property disposal", "land disposal", "surplus land", "asset disposal", "property valuation", "compulsory purchase", "land development"], subcategories: ["Asset valuation","Compulsory purchase","Land disposal","Development appraisal","Estate strategy","Property acquisition","Lease management","Rating appeals","Asset register","Commercial property","Surplus land","Community asset transfer"] },
    ]
  },
  { slug: "justice", label: "Justice & Probation", standfirst: "Prison services, probation, community rehabilitation, electronic monitoring, court services, and youth justice commissioned by HMPPS and local authorities.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "probation prison rehabilitation community sentence electronic monitoring youth justice community safety", idealBuyers: "Ministry of Justice HMPPS probation service local authorities youth offending teams", mainGoal: "find justice and probation contracts" }),
    categories: [
      { label: "Prison & Custodial Services", keywords: ["prison", "custody", "hmps", "hmpps", "offender", "detainee", "secure unit"], subcategories: ["Prison management","Custodial services","Education (prison)","Healthcare (prison)","Catering (prison)","Rehabilitation programmes","Substance misuse (prison)","Mental health (prison)","Resettlement","Prison transport","Vocational training (prison)","Chaplaincy"] },
      { label: "Probation & Rehabilitation", keywords: ["probation", "rehabilitation", "community sentence", "offender management", "through the gate"], subcategories: ["Probation support","Community payback","Unpaid work","Resettlement support","Accommodation (CJS)","Through-the-gate","Employment (offenders)","Mentoring (offenders)","Peer support (CJS)","Domestic abuse perpetrators","Drug rehabilitation requirements","Alcohol treatment requirements"] },
      { label: "Electronic Monitoring", keywords: ["electronic monitoring", "ankle tag", "gps monitoring", "home detention", "curfew monitoring"], subcategories: ["GPS tagging","Radio frequency monitoring","Alcohol monitoring","Home detention","Curfew monitoring","Satellite tracking","Field monitoring","Fitting and removal","Victim notification","Real-time compliance","Remote supervision","Bail monitoring"] },
      { label: "Court & Legal Support", keywords: ["court", "tribunal", "legal aid", "victim support", "witness service", "interpreter courts"], subcategories: ["Court support services","Witness support","Victim services","HMCTS support","Translation and interpretation (courts)","Court security","Transport to court","Remand services","Bail support","Court reporting","Legal aid providers","Victim contact scheme"] },
      { label: "Youth Justice", keywords: ["youth justice", "yot", "youth offending", "young offenders", "restorative justice"], subcategories: ["Youth offending teams","Restorative justice","Bail support (YJ)","Court report writing","Mentoring (YJ)","Substance misuse (YJ)","Education (YJ)","Accommodation (YJ)","Reparation","Police custody volunteers","Triage and diversion","Night stop"] },
      { label: "Community Safety", keywords: ["community safety", "antisocial behaviour", "asb", "crime prevention", "violence reduction"], subcategories: ["ASB case management","Violence reduction","Night-time economy","CCTV (community safety)","Drug diversion","Gang prevention","Knife crime","VAWG","Community protection","Safer streets","Exploitation prevention","Integrated offender management"] },
    ]
  },
  { slug: "emergency", label: "Emergency Services", standfirst: "Police, fire and rescue, ambulance, control rooms, PPE, and emergency planning procurement for blue-light services and local resilience forums.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "police fire rescue ambulance emergency planning PPE control room command dispatch resilience", idealBuyers: "police forces fire and rescue services ambulance trusts Ministry of Justice Home Office", mainGoal: "find emergency services procurement contracts" }),
    categories: [
      { label: "Police Procurement", keywords: ["police", "constabulary", "law enforcement", "custody suite", "detective", "police force"], subcategories: ["Police vehicles","Custody equipment","Forensics","Uniforms (police)","IT (police)","Body-worn cameras","Tasers","Communication systems","Firearms training","Interpreter services","Drones (police)","Surveillance equipment"] },
      { label: "Fire & Rescue", keywords: ["fire", "rescue", "fire service", "fire station", "fire appliance", "breathing apparatus"], subcategories: ["Fire appliances","Breathing apparatus","PPE (fire)","Fire station equipment","Training simulators","Aerial platforms","Water rescue","Hazmat","Drone (fire)","Control room (fire)","Rescue equipment","Thermal imaging (fire)"] },
      { label: "Ambulance & Paramedic", keywords: ["ambulance", "paramedic", "ems", "nhs ambulance", "patient transport", "first responder"], subcategories: ["Ambulance vehicles","Paramedic equipment","Defibrillators","Medical equipment (ambulance)","Patient transport (non-emergency)","First responder training","Control room (ambulance)","Triage systems","Community responders","Dispatch software","Crew training","Helicopter EMS"] },
      { label: "Control Rooms & Dispatch", keywords: ["control room", "999", "dispatch", "command and control", "incident management", "cad system"], subcategories: ["CAD systems","Command and control","999 infrastructure","PSAP equipment","Operator training","Radio communications","Emergency alerting","Major incident","Joint control rooms","Resilience systems","Airwave/ESN","CCTV control"] },
      { label: "PPE & Specialist Equipment", keywords: ["ppe", "protective equipment", "uniform", "body armour", "riot gear", "specialist equipment"], subcategories: ["Body armour","Riot equipment","Chemical protection","Fire PPE","Paramedic PPE","High-visibility clothing","Boots and gloves","Helmets","Respiratory protection","Specialist PPE","Ballistic protection","Nuclear/biological/chemical"] },
      { label: "Emergency Planning & Resilience", keywords: ["emergency planning", "business continuity", "resilience", "civil contingency", "lrf"], subcategories: ["Emergency plans","Business continuity","LRF support","Mass casualty planning","JESIP training","Exercise and testing","Warning and informing","Flood response","Pandemic preparedness","Critical national infrastructure","Community resilience","Mutual aid"] },
    ]
  },
  { slug: "research", label: "Research & Evaluation", standfirst: "Social research, policy evaluation, public health research, data analytics, consultation, and market research for government and NHS bodies.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "social research policy evaluation public health research data analytics consultation market research epidemiology", idealBuyers: "central government NHS England local authorities research councils", mainGoal: "find research and evaluation contracts" }),
    categories: [
      { label: "Social Research", keywords: ["social research", "qualitative research", "quantitative research", "survey", "research study"], subcategories: ["Qualitative research","Quantitative surveys","Longitudinal studies","Ethnographic research","Focus groups","Depth interviews","Research panels","Mystery shopping","Citizen surveys","Public attitude research","Co-design research","Rapid evidence review"] },
      { label: "Policy Evaluation", keywords: ["policy evaluation", "programme evaluation", "impact assessment", "theory of change"], subcategories: ["Summative evaluation","Formative evaluation","Process evaluation","Economic evaluation","Impact assessment","Theory of change","Logic model development","Evaluation framework","Realist evaluation","Contribution analysis","SROI","Rapid evidence review"] },
      { label: "Public Health Research", keywords: ["public health research", "epidemiology", "health surveillance", "needs assessment", "jsna"], subcategories: ["Epidemiological studies","Health needs assessment","JSNA support","Health inequalities research","Behavioural insights","Surveillance systems","Disease modelling","Screening evaluation","Preventive research","Pharmaceutical trials","Genomics","Health data linkage"] },
      { label: "Data & Analytics", keywords: ["data analytics", "business intelligence", "data science", "predictive analytics", "dashboard"], subcategories: ["Data strategy","Business intelligence","Predictive modelling","Machine learning","Data visualisation","Power BI/Tableau","Open data","Statistical analysis","GIS analysis","Performance dashboards","Data governance","Data infrastructure"] },
      { label: "Consultation & Participation", keywords: ["participatory research", "engagement research", "research participation", "deliberative", "citizens assembly"], subcategories: ["Deliberative panels","Citizens' assemblies","Online consultation platforms","Stakeholder mapping","Engagement strategy","Community research","Young people's participation","Hard-to-reach research","Equalities analysis","Accessibility research","JSNA consultation","Public panel management"] },
      { label: "Market & Economic Research", keywords: ["market research", "economic analysis", "feasibility study", "cost benefit", "option appraisal"], subcategories: ["Feasibility studies","Cost-benefit analysis","Options appraisal","Market analysis","Demand forecasting","Competition analysis","Socioeconomic impact","ROI modelling","Wellbeing economics","Green Book appraisal","Economic modelling","Sector intelligence"] },
    ]
  },
  { slug: "consulting", label: "Central Gov Consulting", standfirst: "Management consulting, digital transformation, programme delivery, policy development, and commercial advisory for central government departments.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "management consulting digital transformation programme delivery policy development operating model commercial advisory cabinet office", idealBuyers: "central government departments Cabinet Office HMRC DVLA DWP Home Office NHS England", mainGoal: "find central government consulting contracts" }),
    categories: [
      { label: "Digital Transformation", keywords: ["digital transformation", "digitisation", "digital strategy", "digital government", "service redesign"], subcategories: ["Digital strategy","Technology assessment","Digital service redesign","Legacy modernisation","API-first design","Cloud migration strategy","Data architecture","AI readiness","Digital leadership","GDS standards","GOV.UK Notify","Service standard assessment"] },
      { label: "Programme & Project Delivery", keywords: ["programme delivery", "project management", "pmo", "agile delivery", "prince2", "gateway review"], subcategories: ["Programme management","PMO setup","Agile delivery","Portfolio management","Benefits tracking","Schedule management","Risk register","Governance frameworks","Gateway reviews","Delivery assurance","IPA reviews","Major projects authority"] },
      { label: "Organisational Transformation", keywords: ["organisational transformation", "operating model", "restructuring", "shared services", "merger"], subcategories: ["Operating model design","Shared services","Merger and acquisition","Workforce redesign","Culture change","Behavioural change","Leadership development","Succession planning","Target operating model","Benchmarking","OD consulting","Arm's-length body reform"] },
      { label: "Policy Development", keywords: ["policy development", "policy design", "regulatory reform", "policy strategy", "white paper", "green paper", "ministerial"], subcategories: ["Policy design","Regulatory impact","Strategy development","White paper support","Consultation design","Ministerial briefings","Evidence synthesis","Parliamentary work","Public inquiry support","Arms-length bodies","Spending review","Policy simulation"] },
      { label: "Commercial & Procurement Advisory", keywords: ["commercial advisory", "procurement advisory", "category management", "sourcing strategy"], subcategories: ["Category management","Strategic sourcing","Market engagement","Spend analysis","Commercial strategy","Contract management","Supplier development","Procurement transformation","Crown Commercial","Cabinet Office compliance","Make vs buy","Commercial assurance"] },
      { label: "Financial & Economic Advisory", keywords: ["financial advisory", "economic advisory", "business case", "green book", "spending review"], subcategories: ["Business case development","Green Book","Infrastructure financing","Spending review support","Economic appraisal","Value-for-money assessment","Financial modelling","ROAMEF","Cost modelling","Public accounts support","Fiscal analysis","CDEL/RDEL management"] },
    ]
  }
];

const SIGNAL_CATEGORIES: Array<{ key: string; label: string; input: z.infer<typeof intakeSchema> }> =
  DESK_PROFILES.filter(d => d.live).map(d => ({ key: d.slug, label: d.label, input: d.pinnedProfile }));

const CATEGORY_LABELS: Record<string, string> = Object.fromEntries(
  SIGNAL_CATEGORIES.map(c => [c.key, c.label])
);


function renderDeskCard(sig: HomepageSignal, tag: string, slug: string): string {
  const dateStr = sig.notice_date
    ? new Date(sig.notice_date).toLocaleDateString("en-GB", { day: "numeric", month: "short" })
    : "—";
  const buyer = sig.buyer ? escapeHtml(sig.buyer.slice(0, 55)) : "Buyer not stated";
  const title = escapeHtml(sig.title.slice(0, 85));
  let valueChip = "";
  if (sig.value_amount && sig.value_amount > 0) {
    const v = sig.value_amount;
    const vLabel = v >= 1_000_000 ? `£${(v / 1_000_000).toFixed(1)}M` : `£${Math.round(v / 1_000)}k`;
    valueChip = `<span class="dc-chip dc-chip-value">${vLabel}</span>`;
  }
  const isOpen = !sig.status || !sig.status.toUpperCase().includes("AWARD");
  const statusChip = isOpen
    ? `<span class="dc-chip dc-chip-open">Open</span>`
    : `<span class="dc-chip dc-chip-awarded">Awarded</span>`;
  const srcLabel = sig.source && sig.source.toLowerCase().includes("tender") ? "FTS" : "CF";
  const srcChip = `<span class="dc-chip dc-chip-src">${srcLabel}</span>`;
  return `<a class="desk-card reveal" href="/desk/${escapeHtml(slug)}">
  <div class="dc-top"><span class="dc-label">${escapeHtml(tag)}</span><div class="dc-chips">${valueChip}${statusChip}${srcChip}</div></div>
  <div class="dc-title">${title}</div>
  <div class="dc-buyer">${buyer}</div>
  <div class="dc-foot"><span class="dc-date">${escapeHtml(dateStr)}</span><span class="dc-cta">View opportunities &rarr;</span></div>
</a>`;
}

async function postOpportunitiesToSlack(newSignals: HomepageSignal[]): Promise<void> {
  if (!SLACK_WEBHOOK_URL || newSignals.length === 0) return;
  const fmtVal = (v: number | null) =>
    v && v > 0
      ? v >= 1_000_000 ? `£${(v / 1_000_000).toFixed(1)}m` : `£${Math.round(v / 1_000)}k`
      : "value n/a";
  const top = [...newSignals]
    .sort((a, b) => (b.notice_date || "").localeCompare(a.notice_date || ""))
    .slice(0, 8);
  const lines = top.map(s => {
    const label = CATEGORY_LABELS[s.category] || s.category;
    const buyer = s.buyer || "Buyer not stated";
    // Slack mrkdwn link text must not contain < > &
    const title = (s.title || "Untitled").replace(/[<>&]/g, "").slice(0, 140);
    return `• <${s.source_url}|${title}> — ${buyer} · ${fmtVal(s.value_amount)} · ${label} (${s.source})`;
  });
  const overflow = newSignals.length > top.length ? `\n_…and ${newSignals.length - top.length} more_` : "";
  const text = `*🔔 ${newSignals.length} new public-sector ${newSignals.length === 1 ? "opportunity" : "opportunities"}*\n\n${lines.join("\n")}${overflow}`;
  try {
    const res = await fetch(SLACK_WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text })
    });
    if (!res.ok) console.error(`[slack] webhook returned ${res.status}`);
  } catch (err) {
    console.error("[slack] post failed", err);
  }
}

async function refreshHomepageSignals(): Promise<void> {
  console.log("[signals] refresh started");
  // On a cold table every signal is "new" — that's a seed, not deal-flow, so we
  // suppress the Slack post for the first populating run.
  let hadExistingSignals = false;
  if (pool) {
    const r = await pool.query<{ n: string }>(`SELECT COUNT(*) AS n FROM homepage_signals`);
    hadExistingSignals = parseInt(r.rows[0]?.n || "0", 10) > 0;
  } else {
    hadExistingSignals = sigMemStore.size > 0;
  }
  const allNew: HomepageSignal[] = [];
  for (const cat of SIGNAL_CATEGORIES) {
    try {
      const data = await pullProcurementData(cat.input);
      const allNotices: ProcurementNotice[] = [
        ...data.contractsFinder.open,
        ...data.contractsFinder.awarded,
        ...(data.findTender?.notices || [])
      ];
      const deduped = dedupeNotices(allNotices);
      const deskProfile = DESK_PROFILES.find(d => d.slug === cat.key);
      const deskKw = deskProfile ? deskProfile.categories.flatMap(c => c.keywords) : [];
      const relevant = deskKw.length > 0
        ? deduped.filter(n => {
            const title = n.title.toLowerCase();
            return deskKw.some(kw => title.includes(kw));
          })
        : deduped;
      const signalPool = relevant;
      const now = nowIso();
      const signals: HomepageSignal[] = signalPool.map(n => ({
        id: n.url || `${n.source}-${n.id}`,
        category: cat.key,
        title: n.title.slice(0, 200),
        buyer: n.buyer && n.buyer !== "Not stated" ? n.buyer.slice(0, 120) : null,
        source: n.source === "Find a Tender" ? "FTS" : "CF",
        source_url: n.url,
        notice_date: n.publishedDate || n.awardedDate || null,
        deadline_date: n.deadlineDate || null,
        value_amount: (() => { const v = n.valueHigh ?? n.valueLow ?? n.awardedValue; return v != null ? Math.round(v) : null; })(),
        status: n.status || "unknown",
        fetched_at: now
      })).filter(s => s.id && s.title);
      const newOnes = await upsertSignals(signals);
      allNew.push(...newOnes);
      console.log(`[signals] ${cat.key}: upserted ${signals.length} (${newOnes.length} new)`);
    } catch (err: any) {
      console.error(`[signals] ${cat.key} refresh failed: ${err?.message}`);
      captureError(err, { signalRefresh: { category: cat.key } });
    }
  }
  if (hadExistingSignals) {
    await postOpportunitiesToSlack(allNew);
  } else if (allNew.length > 0) {
    console.log(`[signals] cold start — suppressing Slack post for ${allNew.length} seeded signals`);
  }
  console.log("[signals] refresh complete");
}

function startSignalsWorker(): void {
  if (redisConnection && signalQueue) {
    // hourly repeating job
    signalQueue.add(
      "refresh",
      {},
      {
        repeat: { every: 60 * 60 * 1000 },
        jobId: "homepage-signals-hourly",
        removeOnComplete: { age: 60 * 60 * 24 },
        removeOnFail: { age: 60 * 60 * 24 * 3 }
      }
    ).catch(err => console.error("[signals] failed to schedule job", err));
    // immediate startup run (no repeat)
    signalQueue.add("refresh", {}, { removeOnComplete: true, removeOnFail: { age: 60 * 60 * 24 } })
      .catch(err => console.error("[signals] failed to queue startup run", err));

    const worker = new Worker(
      "govrevenue-signals",
      async () => { await refreshHomepageSignals(); },
      { connection: redisConnection as any, concurrency: 1, lockDuration: 7_200_000, stalledInterval: 120_000 }
    );
    worker.on("completed", () => console.log("[signals] refresh job completed"));
    worker.on("failed", (job, err) => {
      console.error("[signals] refresh job failed", err);
      captureError(err, { signalWorker: { jobId: job?.id } });
    });
    console.log("[signals] worker started");
  } else {
    // In-process fallback: run once on startup then every hour
    console.log("[signals] Redis not configured — running in-process on startup");
    refreshHomepageSignals().catch(err => console.error("[signals] initial refresh failed", err));
    setInterval(() => {
      refreshHomepageSignals().catch(err => console.error("[signals] scheduled refresh failed", err));
    }, 60 * 60 * 1000);
  }
}

function clampScore(value: number) {
  const safe = Number.isFinite(value) ? value : 0;
  return Math.max(0, Math.min(100, Math.round(safe)));
}

function lowerText(value: any) {
  return String(value || "").toLowerCase();
}

function hasAny(text: string, terms: string[]) {
  return terms.some(term => text.includes(term));
}

function calcPremiumScores(scan: ScanRecord) {
  const input = scan.input_json || {};
  const data = scan.procurement_json as ProcurementData | null;

  const openCount = data?.contractsFinder?.open?.length || 0;
  const awardCount = data?.contractsFinder?.awarded?.length || 0;
  const errorCount = data?.contractsFinder?.errors?.length || 0;

  const serviceText = [
    input.companyName,
    input.mainServices,
    input.secondaryServices,
    input.idealBuyers,
    input.publicSectorExperience,
    input.caseStudies,
    input.certifications,
    input.mainGoal
  ].join(" ").toLowerCase();

  const resolvedSector = resolveSectorFromScan(scan);
  const hasSectorFocus = resolvedSector.key !== "general";

  const experience = lowerText(input.publicSectorExperience);
  const caseStudies = lowerText(input.caseStudies);
  const certs = lowerText(input.certifications);
  const team = lowerText(input.teamSize);

  const hasPublicExperience = hasAny(experience, ["strong", "some", "public", "framework", "nhs", "council", "university"]);
  const hasCaseStudies = caseStudies.length > 60 && !hasAny(caseStudies, ["none", "no confirmed", "not yet"]);
  const hasCerts = certs.length > 40 && !hasAny(certs, ["to be confirmed", "none", "not confirmed"]);
  const hasTeam = team.length > 0 && !hasAny(team, ["to be confirmed", "not confirmed"]);

  const buyerFit = clampScore(
    48 +
      (openCount * 2.2) +
      (awardCount * 1.4) +
      (hasSectorFocus ? 12 : 0)
  );

  const evidenceStrength = clampScore(
    22 +
      (hasPublicExperience ? 25 : 0) +
      (hasCaseStudies ? 25 : 0) +
      (hasCerts ? 18 : 0) +
      (hasTeam ? 10 : 0)
  );

  const procurementReadiness = clampScore(
    28 +
      (hasCerts ? 28 : 0) +
      (hasPublicExperience ? 18 : 0) +
      (hasCaseStudies ? 16 : 0) +
      (hasTeam ? 10 : 0)
  );

  const revenueOpportunity = clampScore(
    36 +
      (openCount * 2.4) +
      (awardCount * 1.8) +
      (hasSectorFocus ? 15 : 0)
  );

  const dataConfidence = clampScore(
    35 +
      (openCount > 0 ? 22 : 0) +
      (awardCount > 0 ? 22 : 0) -
      (errorCount * 8) +
      (hasSectorFocus ? 12 : 0)
  );

  const route =
    procurementReadiness >= 72 && evidenceStrength >= 70
      ? "Bid + framework activation"
      : procurementReadiness >= 55
        ? "Targeted bid + partner route"
        : "Partner/subcontract first";

  const sector = escapeHtml(resolvedSector.label);

  return {
    sector,
    route,
    buyerFit,
    evidenceStrength,
    procurementReadiness,
    revenueOpportunity,
    dataConfidence,
    openCount,
    awardCount
  };
}

function scoreLabel(score: number) {
  if (score >= 80) return "Strong";
  if (score >= 65) return "Promising";
  if (score >= 50) return "Developing";
  return "Weak";
}


function premiumDashboardHtml(scan: ScanRecord) {
  return `
    ${evidenceDashboard(scan)}
  `;
}

function premiumClosingHtml(scan: ScanRecord, parsedEdp?: ParsedEdp | null) {
  const scores = calcPremiumScores(scan);
  const company = scan.company_name;
  const finalRoute = parsedEdp?.recommendedRoute || scores.route;
  const finalSector = scores.sector;

  return `
    <section class="marketing-close">
      <div class="close-kicker">Revenue intelligence</div>
      <h2>From public-sector noise to a route-to-revenue system.</h2>
      <p>For <strong>${escapeHtml(company)}</strong>, this scan translates public procurement data into a practical commercial map: where demand exists, which buyers matter, what evidence is missing, and which route should be pursued first.</p>
      <div class="close-grid">
        <div>
          <b>Recommended route</b>
          <span>${escapeHtml(finalRoute)}</span>
        </div>
        <div>
          <b>Sector lens</b>
          <span>${escapeHtml(finalSector)}</span>
        </div>
        <div>
          <b>Next value unlock</b>
          <span>Turn this scan into a 30-day buyer action campaign.</span>
        </div>
      </div>
      <p class="close-note">GovRevenue helps businesses stop guessing at public-sector opportunities. The product turns buyer signals, contract records and readiness gaps into a focused revenue plan that teams can act on immediately.</p>
      <p class="close-note" style="margin-top:8px;font-size:12px;color:var(--muted)">No outcome is guaranteed. This scan is commercial intelligence, not legal, procurement or financial advice. Human verification is required before bid decisions.</p>
    </section>
  `;
}



function evidenceNumber(value: number) {
  return new Intl.NumberFormat("en-GB").format(value || 0);
}

function evidenceMoney(value: number) {
  if (!value || Number.isNaN(value)) return "Not stated";
  return new Intl.NumberFormat("en-GB", {
    style: "currency",
    currency: "GBP",
    maximumFractionDigits: 0
  }).format(value);
}

function getNoticeValue(notice: any) {
  return Number(notice?.awardedValue || notice?.valueHigh || notice?.valueLow || 0) || 0;
}









function assessDataQuality(scan: ScanRecord) {
  const input: any = scan.input_json || {};
  const data: any = scan.procurement_json || {};
  const keywords = (data?.keywords || []).join(" ").toLowerCase();
  const text = [
    input.companyName,
    input.mainServices,
    input.secondaryServices,
    input.mainGoal
  ].join(" ").toLowerCase();

  const openCount = data?.contractsFinder?.open?.length || 0;
  const awardCount = data?.contractsFinder?.awarded?.length || 0;
  const total = openCount + awardCount;

  const constructionClient =
    text.includes("construction") ||
    text.includes("quantity surveying") ||
    text.includes("cost management") ||
    text.includes("project management") ||
    text.includes("building surveying") ||
    text.includes("estate");

  const creativeClient =
    text.includes("marketing") ||
    text.includes("creative") ||
    text.includes("campaign") ||
    text.includes("video") ||
    text.includes("film") ||
    text.includes("communications") ||
    text.includes("event");

  const photographyClient =
    text.includes("photography") ||
    text.includes("portrait") ||
    text.includes("graduation") ||
    text.includes("property photography");

  const constructionKeywords =
    keywords.includes("construction") ||
    keywords.includes("quantity surveying") ||
    keywords.includes("cost management") ||
    keywords.includes("project management") ||
    keywords.includes("building surveying") ||
    keywords.includes("estate");

  const creativeKeywords =
    keywords.includes("marketing") ||
    keywords.includes("creative") ||
    keywords.includes("communications") ||
    keywords.includes("media") ||
    keywords.includes("event");

  const photographyKeywords = keywords.includes("photography");

  const aligned =
    (constructionClient && constructionKeywords) ||
    (creativeClient && creativeKeywords) ||
    (photographyClient && photographyKeywords);

  if (!total) {
    return "Weak — no structured Contracts Finder records returned for this scan.";
  }

  if (!aligned) {
    return "Weak / noisy — records were pulled, but the search terms do not fully align with the client’s core services.";
  }

  if (total >= 20) {
    return "Moderate to strong — useful public procurement records found, but human filtering is still required.";
  }

  if (total >= 8) {
    return "Moderate — enough records for signal analysis, but not enough to treat as a complete market map.";
  }

  return "Early signal — limited records found; use as directional intelligence only.";
}

function stripReportTitleFromMarkdown(markdown: string): string {
  return markdown.replace(/^#\s+GovRevenue\s+Scan:[^\n]*\n?/im, "");
}

function waitingPage(scan: ScanRecord): string {
  const scanId = escapeHtml(scan.id);
  const isFailed = scan.status === "failed";
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>${escapeHtml(scan.company_name)} &mdash; Scanning &mdash; GovRevenue</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Newsreader:ital,opsz,wght@0,6..72,400;0,6..72,500;1,6..72,400&family=Libre+Franklin:wght@400;500;600;700&family=Spline+Sans+Mono:wght@400;500;600&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --base:#ECE7DA;--surface:#FBF9F3;--surface-2:#F6F2E8;
  --brand:#B4924E;--brand-dim:rgba(180,146,78,.14);
  --text:#1B1E19;--muted:#86897E;--faint:#9AA093;
  --border:rgba(27,30,25,.10);--border-2:rgba(27,30,25,.16);
  --green:#1d6b4f;--red:#9b2d20;
  --sans:"Libre Franklin",system-ui,sans-serif;
  --mono:"Spline Sans Mono",ui-monospace,monospace;
  --serif:"Newsreader",Georgia,serif;
}
body{background:var(--base);color:var(--text);font-family:var(--sans);min-height:100vh;display:flex;align-items:center;justify-content:center;padding:32px;-webkit-font-smoothing:antialiased}
.card{max-width:560px;width:100%;background:var(--surface);border:1px solid var(--border-2);padding:44px 48px}
.brand{font-family:var(--mono);font-size:11px;letter-spacing:.2em;text-transform:uppercase;color:var(--brand);margin-bottom:28px;display:flex;align-items:center;gap:8px}
.brand-dot{width:8px;height:8px;background:var(--brand);border-radius:50%}
h1{font-family:var(--serif);font-size:26px;font-weight:400;line-height:1.25;margin-bottom:28px;color:var(--text)}
h1 b{color:var(--brand);font-weight:500}
.stage-list{list-style:none;margin-bottom:28px}
.stage{display:flex;align-items:center;gap:12px;padding:11px 0;border-bottom:1px solid var(--border);font-size:14px;color:var(--faint);transition:color .3s}
.stage:last-child{border-bottom:0}
.dot{width:9px;height:9px;border-radius:50%;background:var(--border-2);flex-shrink:0;transition:background .3s}
.stage.active{color:var(--text)}
.stage.active .dot{background:var(--brand);box-shadow:0 0 0 4px var(--brand-dim)}
.stage.done{color:var(--green)}
.stage.done .dot{background:var(--green)}
.stage.fail{color:var(--red)}
.stage.fail .dot{background:var(--red)}
.eta{font-size:12.5px;color:var(--muted);font-family:var(--mono);letter-spacing:0.04em}
.err{margin-top:20px;padding:14px;background:rgba(155,45,32,.06);border:1px solid rgba(155,45,32,.22);font-size:13px;color:#9b2d20}
</style>
</head>
<body>
<div class="card">
  <div class="brand"><span class="brand-dot"></span>GovRevenue &mdash; Intelligence Scan</div>
  <h1>${isFailed ? "Scan failed for " : "Scanning for "}<b>${escapeHtml(scan.company_name)}</b></h1>
  <ul class="stage-list" id="stages">
    <li class="stage" id="s-fetching"><span class="dot"></span><span>Fetching procurement data from Contracts Finder &amp; Find a Tender</span></li>
    <li class="stage" id="s-scoring"><span class="dot"></span><span>Scoring opportunities and routes to revenue</span></li>
    <li class="stage" id="s-report"><span class="dot"></span><span>Generating intelligence report</span></li>
    <li class="stage" id="s-done"><span class="dot"></span><span>Report complete &mdash; loading</span></li>
  </ul>
  <p class="eta" id="eta">Typically 2&ndash;4 minutes. Stay on this page.</p>
  ${isFailed ? `<div class="err">Error: ${escapeHtml(scan.error_message || "Unknown error")}. <a href="/scan/${scanId}">Refresh</a></div>` : ""}
</div>
<script>
(function(){
  const stages=['fetching','scoring','report','done'];
  let cur=-1;
  function mark(stage,cls){const el=document.getElementById('s-'+stage);if(el){el.classList.remove('active','done','fail');el.classList.add(cls);}}
  function setStage(stage){
    const idx=stages.indexOf(stage);
    if(stage==='failed'){stages.forEach(s=>mark(s,''));mark('fetching','fail');document.getElementById('eta').textContent='Scan failed. Refresh the page for details.';return;}
    if(idx<0||idx<=cur) return;
    cur=idx;
    stages.forEach((s,i)=>{if(i<idx)mark(s,'done');else if(i===idx)mark(s,'active');});
    if(stage==='done'){setTimeout(()=>location.reload(),900);}
  }
  const es=new EventSource('/api/scans/${scanId}/stream');
  es.onmessage=function(e){try{const d=JSON.parse(e.data);setStage(d.stage);}catch{}};
  es.onerror=function(){};
  const poll=setInterval(async function(){
    try{const r=await fetch('/api/scans/${scanId}/status');const d=await r.json();
    if(d.status==='completed'){clearInterval(poll);location.reload();}
    else if(d.status==='failed'){clearInterval(poll);location.reload();}
    else if(d.progress_stage){setStage(d.progress_stage);}
    }catch{}
  },8000);
  window.addEventListener('pagehide',function(){es.close();clearInterval(poll);});
})();
</script>
</body>
</html>`;
}

function reportPage(scan: ScanRecord) {
  if (scan.status === "pending" || scan.status === "running") return waitingPage(scan);
  const data = scan.procurement_json as ProcurementData | null;
  const sectorLens = resolveSectorFromScan(scan).label;
  const dataQuality = assessDataQuality(scan);
  const openCount = data?.contractsFinder?.open?.length || 0;
  const awardedCount = data?.contractsFinder?.awarded?.length || 0;
  const keywords = data?.keywords?.join(", ") || "Pending";
  const regions = data?.regions || "Pending";
  const scores = calcPremiumScores(scan);

  const parsedEdp = scan.report_markdown
    ? parseEdpFromMarkdown(scan.report_markdown)
    : null;

  const edpVerdict = parsedEdp?.verdict || "";
  const edpGrade = parsedEdp?.evidenceGrade || "";
  const edpCanWin = parsedEdp?.canTheyWinNow || "";
  const edpRoute = parsedEdp?.recommendedRoute || scores.route;
  const edpBestRoute = parsedEdp?.bestFirstMoneyRoute || scores.route;
  const edpFastestAction = parsedEdp?.fastestActionThisWeek || "";
  const edpMainBlocker = parsedEdp?.mainBlocker || "";

  const bodyMarkdown = scan.report_markdown
    ? stripEdpFromMarkdown(stripReportTitleFromMarkdown(scan.report_markdown))
    : null;

  const content = bodyMarkdown
    ? markdownToHtml(bodyMarkdown)
    : `<p>Status: <strong>${escapeHtml(scan.status)}</strong></p><p>${scan.error_message ? escapeHtml(scan.error_message) : "Still running. Refresh shortly."}</p>`;

  return `<!doctype html>
<html lang="en">
<head>
<meta charset="UTF-8">
  <title>${escapeHtml(scan.company_name)} &mdash; GovRevenue Scan</title>
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Newsreader:ital,opsz,wght@0,6..72,400;0,6..72,500;0,6..72,600;1,6..72,400;1,6..72,500&family=Libre+Franklin:wght@400;500;600;700&family=Spline+Sans+Mono:wght@400;500;600&display=swap');

    :root {
      --base:#05070B; --surface:#0C111A; --surface-2:#101820; --surface-3:#141E2A;
      --brand:#B4924E; --brand-hot:#C4933F; --brand-dim:rgba(180,146,78,.14);
      --info:#60A5FA; --green:#22C55E; --green-dim:#166534; --gold:#B4924E; --red:#F87171;
      --text:#ECE6D6; --text-mid:#C8C0AE; --muted:#9AA093; --faint:#6B7280;
      --border:rgba(255,255,255,.06); --border-2:rgba(255,255,255,.10); --border-3:rgba(255,255,255,.16);
      --glass:rgba(255,255,255,.04); --gbdr:rgba(255,255,255,.08);
      --sans:"Libre Franklin",system-ui,-apple-system,sans-serif;
      --mono:"Spline Sans Mono",ui-monospace,monospace;
      --serif:"Newsreader",Georgia,serif;
    }

    * { box-sizing:border-box; }

    body {
      margin:0;
      background:var(--base);
      color:var(--text);
      font-family:var(--sans);
      -webkit-font-smoothing:antialiased;
    }

    .page {
      max-width:1160px;
      margin:0 auto;
      padding:36px 24px 80px;
    }

    .topbar {
      display:flex;
      justify-content:space-between;
      align-items:center;
      gap:16px;
      margin-bottom:22px;
      flex-wrap:wrap;
      padding:14px 20px;
      background:var(--surface);
      border:1px solid var(--border-2);
    }

    .brand {
      display:flex;
      align-items:center;
      gap:9px;
      font-family:var(--serif);
      font-size:19px;
      font-weight:500;
      color:var(--text);
      text-decoration:none;
    }

    .brand-dot { width:9px;height:9px;background:var(--brand);border-radius:50%;flex-shrink:0 }

    .actions {
      display:flex;
      gap:10px;
      flex-wrap:wrap;
    }

    .btn {
      border:1px solid rgba(180,146,78,.4);
      background:rgba(180,146,78,.12);
      color:var(--brand);
      padding:9px 16px;
      font-weight:600;
      cursor:pointer;
      text-decoration:none;
      font-size:12px;
      font-family:var(--mono);
      letter-spacing:.06em;
      text-transform:uppercase;
    }

    .btn:hover { background:rgba(180,146,78,.22); }

    .btn.secondary {
      background:transparent;
      color:var(--muted);
      border-color:var(--border-3);
    }

    .btn.secondary:hover { background:var(--glass); color:var(--text); }

    .cover {
      background:linear-gradient(160deg,#0B1420 0%,#0E1A2A 60%,#102A1E 100%);
      border:1px solid var(--border-2);
      border-top:2px solid var(--brand);
      padding:48px;
      break-after:auto;
      position:relative;
      overflow:hidden;
    }

    .cover::before {
      content:'';
      position:absolute;
      top:-80px;right:-80px;
      width:360px;height:360px;
      background:radial-gradient(circle,rgba(180,146,78,.10) 0%,transparent 70%);
      pointer-events:none;
    }

    .cover-label {
      font-family:var(--mono);
      font-size:10px;
      letter-spacing:0.22em;
      text-transform:uppercase;
      color:var(--brand);
      margin-bottom:20px;
      opacity:.8;
    }

    .cover h1 {
      font-family:var(--serif);
      font-size:clamp(32px,3.8vw,50px);
      font-weight:400;
      line-height:1.05;
      margin:0 0 8px;
      letter-spacing:-0.02em;
      color:var(--text);
    }

    .cover-edp-label {
      font-family:var(--mono);
      font-size:11px;
      letter-spacing:.14em;
      text-transform:uppercase;
      color:var(--muted);
      margin-bottom:22px;
    }

    .subtitle {
      color:var(--muted);
      font-size:14px;
      font-family:var(--mono);
      letter-spacing:.04em;
      max-width:820px;
      line-height:1.6;
      margin-bottom:0;
    }

    .meta {
      display:grid;
      grid-template-columns:repeat(4, 1fr);
      gap:10px;
      margin:28px 0 0;
    }

    .metric {
      border:1px solid var(--border-2);
      border-top:2px solid rgba(180,146,78,.35);
      background:rgba(255,255,255,.04);
      padding:18px 16px 16px;
      min-height:100px;
      backdrop-filter:blur(4px);
    }

    .metric b {
      display:block;
      font-family:var(--mono);
      font-size:10px;
      color:var(--muted);
      text-transform:uppercase;
      letter-spacing:0.15em;
      margin-bottom:12px;
    }

    .metric span {
      font-family:var(--serif);
      font-size:22px;
      font-weight:500;
      color:var(--text);
      line-height:1.2;
    }

    .metric small {
      display:block;
      margin-top:8px;
      color:var(--faint);
      line-height:1.35;
      font-size:11.5px;
      font-family:var(--mono);
    }

    .data-strip {
      border-left:2px solid var(--brand);
      background:rgba(180,146,78,.06);
      padding:16px 18px;
      margin:20px 0 0;
    }

    .data-strip p {
      margin:5px 0;
      color:var(--muted);
      font-size:13px;
      font-family:var(--mono);
    }

    .section-kicker,
    .close-kicker {
      font-family:var(--mono);
      font-size:11px;
      text-transform:uppercase;
      letter-spacing:0.16em;
      color:var(--brand);
      font-weight:500;
      margin-bottom:10px;
    }

    .marketing-close h2 {
      font-family:var(--serif);
      margin:0 0 8px;
      font-size:28px;
      letter-spacing:-0.01em;
      font-weight:400;
      color:var(--text);
    }

    .marketing-close p {
      margin:0;
      color:var(--muted);
      line-height:1.65;
    }

    .report {
      margin-top:16px;
      background:var(--surface);
      border:1px solid var(--border-2);
      padding:40px;
    }

    .report h1 {
      font-family:var(--mono);
      font-size:11px;
      letter-spacing:.2em;
      text-transform:uppercase;
      color:var(--brand);
      font-weight:500;
      margin:0 0 8px;
      padding-bottom:0;
      border-bottom:none;
    }

    .report h2 {
      font-family:var(--serif);
      font-size:21px;
      font-weight:500;
      margin:40px 0 14px;
      padding-top:18px;
      border-top:1px solid var(--border-2);
      color:var(--text);
      text-align:left;
      break-after:avoid;
    }

    .report h2:first-of-type { margin-top:16px; }

    .report h3 {
      font-family:var(--mono);
      font-size:11px;
      font-weight:600;
      letter-spacing:.15em;
      text-transform:uppercase;
      margin:28px 0 10px;
      color:var(--brand);
      text-align:left;
      break-after:avoid;
    }

    .report p,
    .report li {
      font-size:15px;
      line-height:1.75;
      text-align:left;
      hyphens:auto;
      overflow-wrap:break-word;
      color:var(--text-mid);
    }

    .report ul,
    .report ol {
      padding-left:22px;
    }

    .report li {
      margin:8px 0;
      padding-right:2px;
    }

    .report li::marker {
      color:var(--brand);
    }

    .report-table {
      width:100%;
      border-collapse:collapse;
      margin:16px 0 28px;
      font-size:13px;
      table-layout:fixed;
      page-break-inside:auto;
    }

    .report-table td {
      border:1px solid var(--border-2);
      padding:10px 12px;
      vertical-align:top;
      line-height:1.5;
      word-break:normal;
      overflow-wrap:break-word;
      text-align:left;
      color:var(--text-mid);
    }

    .report-table tr:first-child td {
      background:#102A1E;
      color:#ECE6D6;
      font-family:var(--mono);
      font-size:10.5px;
      letter-spacing:.08em;
      text-transform:uppercase;
      font-weight:500;
      border-color:rgba(255,255,255,.1);
    }

    .report-table tr:nth-child(even) td {
      background:rgba(255,255,255,.02);
    }

    .report-table tr {
      break-inside:avoid;
      page-break-inside:avoid;
    }

    .report a {
      color:var(--brand);
      font-weight:600;
      word-break:break-word;
    }

    .marketing-close {
      margin-top:16px;
      background:var(--surface);
      border:1px solid var(--border-2);
      border-top:2px solid var(--brand);
      color:var(--text);
      padding:36px;
      break-inside:avoid;
    }

    .close-grid {
      display:grid;
      grid-template-columns:repeat(3, 1fr);
      gap:10px;
      margin:22px 0;
    }

    .close-grid div {
      border:1px solid var(--border-2);
      padding:18px;
      background:rgba(255,255,255,.03);
    }

    .close-grid b {
      display:block;
      font-family:var(--mono);
      font-size:10px;
      color:var(--muted);
      text-transform:uppercase;
      letter-spacing:0.14em;
      margin-bottom:8px;
    }

    .close-grid span {
      display:block;
      line-height:1.45;
      font-weight:600;
      color:var(--text);
      font-size:15px;
    }

    .close-note {
      border-top:1px solid var(--border-2);
      padding-top:18px;
    }

    .footer {
      color:var(--faint);
      font-size:11.5px;
      margin-top:18px;
      font-family:var(--mono);
      letter-spacing:.04em;
    }

    @media (max-width:900px) {
      .cover h1 { font-size:36px; }
      .meta { grid-template-columns:1fr 1fr; }
      .close-grid { grid-template-columns:1fr; }
      .report { padding:22px; }
    }

    @page { size:A4; margin:12mm; }

    @media print {
      * {
        -webkit-print-color-adjust:exact !important;
        print-color-adjust:exact !important;
      }

      :root {
        --base:#fff; --surface:#fff; --surface-2:#F9F7F2; --surface-3:#F0EDE5;
        --text:#1B1E19; --text-mid:#2B2E27; --muted:#5C6157; --faint:#86897E;
        --border:rgba(27,30,25,.10); --border-2:#ddd5c5; --border-3:#c8bfae;
        --glass:#fff; --brand:#B4924E; --green:#1d6b4f;
      }

      body { background:#fff; color:#1B1E19; }

      .page { max-width:none; padding:0; }

      .topbar, .actions, .action-tools { display:none !important; }

      .cover {
        padding:10mm;
        break-after:page;
        background:linear-gradient(160deg,#0B1420 0%,#0E1A2A 60%,#102A1E 100%) !important;
        border-color:#2A3A4A !important;
        border-top:2px solid #B4924E !important;
      }
      .cover h1 { color:#ECE6D6 !important; }
      .cover-label, .cover-edp-label { color:#B4924E !important; }
      .subtitle { color:#9AA093 !important; }
      .metric { background:rgba(255,255,255,.06) !important; border-color:rgba(255,255,255,.12) !important; border-top:1px solid rgba(180,146,78,.4) !important; }
      .metric b, .metric small { color:#9AA093 !important; }
      .metric span { color:#ECE6D6 !important; }

      .data-strip { background:rgba(180,146,78,.08) !important; border-left-color:#B4924E !important; }
      .data-strip p { color:#9AA093 !important; }

      .report {
        padding:8mm;
        background:#fff;
        border-color:#ddd5c5;
      }
      .report h1 { color:#B4924E; }
      .report h2 { color:#1B1E19; border-color:#ddd5c5; }
      .report h3 { color:#B4924E; }
      .report p,
      .report li {
        font-size:13px;
        line-height:1.62;
        color:#2B2E27;
        text-align:justify;
        text-align-last:left;
        text-justify:inter-word;
        hyphens:auto;
      }
      .report li::marker { color:#B4924E; }
      .report-table { font-size:11.5px; }
      .report-table td { color:#2B2E27; border-color:#ddd5c5; background:#fff; }
      .report-table tr:first-child td { background:#102A1E !important; color:#ECE6D6 !important; border-color:#102A1E !important; }
      .report-table tr:nth-child(even) td { background:#F9F7F2 !important; }
      .report a { color:#B4924E; text-decoration:none; }

      .marketing-close {
        padding:8mm;
        background:#f5f0e8;
        border-color:#ddd5c5;
        border-top:2px solid #B4924E;
        color:#1B1E19;
      }
      .close-grid div { background:#fff; border-color:#ddd5c5; }
      .close-grid b { color:#5C6157; }
      .close-grid span { color:#1B1E19; }
      .close-note { border-color:#ddd5c5; }

      a { color:#1B1E19; text-decoration:none; }
      .no-print { display:none !important; }
    }
    ${oppCardCss()}
    ${winBriefCss()}
    ${reportChaseNowCss()}
  </style>
</head>
<body>
  <main class="page">
    <div class="topbar">
      <a href="/" class="brand"><span class="brand-dot"></span>Gov<b>Revenue</b></a>
      <div class="actions">
        <a class="btn" href="/api/scans/${scan.id}/report.pdf">Download PDF</a>
        <button type="button" class="btn secondary" onclick="window.print()">Browser Print</button>
        <a class="btn secondary" href="/api/scans/${scan.id}/report.md">Download Markdown</a>
        <a class="btn secondary" href="/api/scans/${scan.id}/data.json">View Data</a>
        <a class="btn secondary" href="/scan/${scan.id}/compare" title="Compare with a previous scan">Compare &uarr;</a>
      </div>
      ${scan.status === "completed" ? `<div class="action-tools" style="margin-top:12px;display:flex;gap:8px;flex-wrap:wrap;border-top:1px solid var(--border-2);padding-top:12px">
        <span style="font-family:var(--mono);font-size:10px;font-weight:600;letter-spacing:.12em;text-transform:uppercase;color:var(--faint);align-self:center">Next steps</span>
        <a class="btn secondary" href="/scan/${scan.id}/capability-statement">Capability statement</a>
        <a class="btn secondary" href="/scan/${scan.id}/outreach-emails">Outreach emails</a>
        <a class="btn secondary" href="/scan/${scan.id}/frameworks">Framework pre-qual</a>
        <a class="btn secondary" href="/account">My account</a>
      </div>` : ""}
    </div>

    <section class="cover">
      <p class="cover-label">UK Public-Sector Revenue Intelligence</p>
      <h1>${escapeHtml(scan.company_name)}</h1>
      <p class="cover-edp-label">Executive Decision Panel</p>
      <p class="subtitle">${escapeHtml(sectorLens)} &middot; ${escapeHtml(regions)} &middot; ${escapeHtml(formatDate(scan.updated_at))}</p>

      <div class="meta">
        <div class="metric"><b>Verdict</b><span style="font-size:${(edpVerdict || "").length > 40 ? "14px" : "18px"};line-height:1.3">${escapeHtml(edpVerdict || "Pending")}</span><small>Commercial recommendation</small></div>
        <div class="metric"><b>Evidence Grade</b><span style="font-size:32px">${escapeHtml(edpGrade || "—")}</span><small>Source-backed evidence basis</small></div>
        <div class="metric"><b>Can they win now?</b><span style="font-size:${(edpCanWin || "").length > 30 ? "14px" : "18px"};line-height:1.3">${escapeHtml(edpCanWin || "Pending")}</span><small>Based on verified evidence</small></div>
        <div class="metric"><b>Recommended route</b><span style="font-size:${(edpRoute || "").length > 30 ? "13px" : "16px"};line-height:1.35">${escapeHtml(edpRoute || "Pending")}</span><small>Best first money route</small></div>
      </div>

      <div class="data-strip">
        ${edpBestRoute ? `<p><strong style="color:var(--brand)">First route:</strong> ${escapeHtml(edpBestRoute)}</p>` : ""}
        ${edpFastestAction ? `<p><strong style="color:var(--brand)">This week:</strong> ${escapeHtml(edpFastestAction)}</p>` : ""}
        ${edpMainBlocker ? `<p><strong style="color:var(--red)">Blocker:</strong> ${escapeHtml(edpMainBlocker)}</p>` : ""}
        <p><strong style="color:var(--muted)">Sector:</strong> ${escapeHtml(scores.sector)} &middot; <strong style="color:var(--muted)">Open:</strong> ${openCount} &middot; <strong style="color:var(--muted)">Awarded:</strong> ${awardedCount}</p>
      </div>
    </section>

    ${premiumDashboardHtml(scan)}

    <section class="report">
      ${content}
    </section>

    ${scan.report_markdown ? premiumClosingHtml(scan, parsedEdp) : ""}

    ${data ? renderIncumbentSection(data) : ""}

    ${(() => {
      if (!data || scan.status !== "completed") return "";
      const intake = scan.input_json as any;
      const allNotices = [
        ...(data.contractsFinder.open || []),
        ...(data.findTender?.notices || []),
      ];
      if (allNotices.length === 0) return "";
      const scanCtx: ScanOpportunityContext = {
        type: "scan",
        services: String(intake?.mainServices || intake?.secondaryServices || ""),
        sector: resolveSectorFromScan(scan).key,
        regions: String(data.regions || intake?.areasServed || ""),
        idealBuyers: String(intake?.idealBuyers || ""),
        keywords: data.keywords || [],
      };
      const scored = scoreAndBucketNotices(allNotices.map(normaliseFromProcurementNotice), scanCtx);
      return renderChaseNowPanel(scored, scanCtx);
    })()}

    <p class="footer">No outcome is guaranteed. This scan is commercial intelligence, not legal, procurement or financial advice. Human verification is required before bid decisions.</p>

    ${scan.status === "completed" ? `
    <div class="no-print" style="margin:40px auto;max-width:680px;padding:24px 28px;background:var(--surface-2);border:1px solid var(--border-2)">
      <h3 style="margin-top:0;font-family:var(--sans);font-weight:800;color:var(--text)">Get weekly opportunity alerts</h3>
      <p style="color:var(--muted);margin-bottom:16px">We'll re-scan Contracts Finder every 7 days and email you when new tenders match your profile.</p>
      <form id="alert-form" style="display:flex;gap:10px;flex-wrap:wrap">
        <input type="email" id="alert-email" placeholder="your@email.com" aria-label="Email address for weekly alerts" required
          style="flex:1;min-width:220px;padding:10px 14px;border:1px solid var(--border-2);font-size:15px;background:var(--surface);color:var(--text)" />
        <button type="submit"
          style="padding:10px 20px;background:var(--green);color:#fff;border:0;font-size:15px;cursor:pointer;white-space:nowrap">
          Subscribe
        </button>
      </form>
      <p id="alert-msg" style="margin-top:12px;font-size:14px;color:var(--green);display:none"></p>
      <script>
        document.getElementById("alert-form").addEventListener("submit", async function(e) {
          e.preventDefault();
          const email = document.getElementById("alert-email").value;
          const msg = document.getElementById("alert-msg");
          try {
            const r = await fetch("/api/scans/${escapeHtml(scan.id)}/subscribe", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ email })
            });
            const data = await r.json();
            msg.style.display = "block";
            msg.textContent = r.ok ? "Subscribed. We'll email you when new opportunities appear." : (data.error || "Subscription failed.");
            msg.style.color = r.ok ? "var(--green)" : "var(--red)";
            if (r.ok) document.getElementById("alert-form").style.display = "none";
          } catch {
            msg.style.display = "block";
            msg.textContent = "Subscription failed. Please try again.";
            msg.style.color = "var(--red)";
          }
        });
      </script>
    </div>` : ""}
  </main>
  <script>
    window.addEventListener("pageshow", () => {
      document.querySelectorAll("#scan-intake input, #scan-intake textarea").forEach((field) => {
        field.value = "";
        field.setAttribute("autocomplete", "off");
      });
    });
  </script>
</body>
</html>`;
}


app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    app: "govrevenue-agent",
    database: pool ? "postgres" : "memory",
    queue: scanQueue ? "redis" : "in-process",
    storage: isPdfStorageConfigured() ? "s3" : "not-configured",
    runWeb: RUN_WEB,
    runWorker: RUN_WORKER,
    sentry: SENTRY_ENABLED ? "enabled" : "disabled",
    email: isEmailConfigured() ? "enabled" : "disabled",
    opportunityBot: SLACK_WEBHOOK_URL ? "enabled" : "disabled",
    reportProvider: anthropic ? "anthropic" : "openai",
    reportModel: anthropic ? ANTHROPIC_MODEL : OPENAI_MODEL,
    model: OPENAI_MODEL
  });
});

app.post("/api/briefing", asyncRoute(async (req, res) => {
  const raw = String(req.body?.email || "").trim().toLowerCase();
  if (!raw || !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(raw)) {
    res.status(400).json({ error: "Valid email required" });
    return;
  }
  const email = raw;
  if (pool) {
    const r = await pool.query(
      `INSERT INTO briefing_subscribers (id, email, category, source, created_at)
       VALUES ($1, $2, NULL, 'homepage', NOW()) ON CONFLICT (email) DO NOTHING`,
      [makeId(), email]
    );
    res.json({ ok: true, alreadySubscribed: (r.rowCount ?? 0) === 0 });
    return;
  }
  const alreadySubscribed = briefMemStore.has(email);
  if (!alreadySubscribed) briefMemStore.set(email, { id: makeId(), email, category: null, source: "homepage", created_at: nowIso() });
  res.json({ ok: true, alreadySubscribed });
}));

app.get("/", asyncRoute(async (req, res) => {
  const homepageAuth = getAuthUser(req);
  const [count24h, samplePdfUrl, deskSignals, chartResult, chaseSignals, chaseStats] = await Promise.all([
    count24hSignals().catch(() => 0),
    findSamplePdf().catch(() => null as string | null),
    queryDeskSignals(DESK_PROFILES.filter(d => d.live).map(d => d.slug)).catch(() => new Map<string, HomepageSignal>()),
    queryChartData().catch(() => ({ points: [] as ChartDataPoint[], illustrative: true, topDesk: null })),
    queryChaseableSignals(6).catch(() => [] as HomepageSignal[]),
    queryChaseableStats().catch(() => ({ totalOpen: 0, avgValueK: null, closingThisMonth: 0, byDesk: [] }) as ChaseStats),
  ]);

  // Derive hero and ticker from current desk signals (sorted by most recently published)
  const signals = [...deskSignals.values()]
    .filter(s => s.notice_date)
    .sort((a, b) => new Date(b.notice_date!).getTime() - new Date(a.notice_date!).getTime());

  // Build chase-now teaser signals from genuine open notices sorted by soonest deadline
  const teaserSignals: HomepageTeaserSignal[] = chaseSignals.map(s => ({
    category: s.category,
    title: s.title,
    buyer: s.buyer,
    source: s.source,
    notice_date: s.notice_date,
    deadline_date: s.deadline_date || null,
    value_amount: s.value_amount,
    status: s.status,
    notice_url: s.source_url || null,
  }));
  const chaseNowHtml = renderChaseNowSection(teaserSignals, chaseStats as OppChaseStats);

  // Hero: most recently published signal from current desks only
  const heroSignal = signals[0] || null;
  const isLive = heroSignal !== null;

  // Ticker HTML — doubled for seamless CSS scroll, server-rendered
  const tickerSrc = signals.length >= 3 ? signals : null;
  const buildTickerItems = (arr: HomepageSignal[]) =>
    arr.map(s =>
      `<span><b>${escapeHtml(s.source)}</b> ${escapeHtml(s.title.slice(0, 70))}${s.buyer ? ` &middot; ${escapeHtml(s.buyer.slice(0, 40))}` : ""}</span>`
    ).join("");
  // 4× repetition keeps the seam invisible even when only 3 signals are loaded
  const tickerHtml = tickerSrc
    ? buildTickerItems(tickerSrc) + buildTickerItems(tickerSrc) + buildTickerItems(tickerSrc) + buildTickerItems(tickerSrc)
    : "<span><b>FTS</b> Illustrative signal &middot; data loads on first refresh</span>".repeat(12);

  // Hero card values
  const heroCategory = isLive
    ? (CATEGORY_LABELS[heroSignal!.category] ||
       heroSignal!.category.replace(/-/g, " ").replace(/\b\w/g, c => c.toUpperCase()))
    : "Housing maintenance";
  const heroTitle = isLive ? heroSignal!.title.slice(0, 80) : "Responsive maintenance framework — West Midlands";
  const heroUrl = isLive ? (heroSignal!.source_url || null) : null;
  const heroBuyer = isLive ? (heroSignal!.buyer || "Buyer not stated") : "Local Authority Buyer";
  const heroSource = isLive ? heroSignal!.source : "CF";
  const heroDateRaw = isLive ? (heroSignal!.notice_date || null) : null;
  const heroDate = (() => {
    if (!heroDateRaw) return "date not stated";
    const ms = Date.now() - new Date(heroDateRaw).getTime();
    const mins = Math.floor(ms / 60000);
    if (mins < 1) return "just now";
    if (mins < 60) return `${mins} min${mins !== 1 ? "s" : ""} ago`;
    const h = Math.floor(mins / 60);
    if (h < 24) return `${h}h ago`;
    const d = Math.floor(h / 24);
    return `${d}d ago`;
  })();
  const heroStatus = isLive ? (heroSignal!.status || "unknown") : "illustrative";
  const heroVal = isLive && heroSignal!.value_amount && heroSignal!.value_amount > 0
    ? (heroSignal!.value_amount >= 1_000_000
        ? `&pound;${(heroSignal!.value_amount / 1_000_000).toFixed(1)}m`
        : `&pound;${Math.round(heroSignal!.value_amount / 1000)}k`)
    : "Value not stated";

  const noticesDisplay = count24h > 0 ? String(count24h) : "—";

  const chartPoints = chartResult.illustrative
    ? [1.9, 2.1, 2.0, 2.4, 2.7, 2.6, 3.0, 3.3, 3.5, 3.8, 4.0, 4.2]
    : chartResult.points.map(p => p.total_m);
  const chartFinalVal = chartPoints.length > 0 ? chartPoints[chartPoints.length - 1] : 4.2;
  const chartMinVal = chartResult.illustrative
    ? 1.6
    : parseFloat((Math.max(0, Math.min(...chartPoints) * 0.85)).toFixed(2));
  const chartMaxVal = chartResult.illustrative
    ? 4.6
    : parseFloat((Math.max(...chartPoints) * 1.15).toFixed(2));
  // Compare 3-month trailing average vs 3-month opening average for a stable trend signal.
  const first3Avg = chartPoints.length >= 3
    ? chartPoints.slice(0, 3).reduce((a, b) => a + b, 0) / 3
    : chartPoints[0] ?? 0;
  const last3Avg = chartPoints.length >= 3
    ? chartPoints.slice(-3).reduce((a, b) => a + b, 0) / 3
    : chartFinalVal;
  const chartTrendPct = first3Avg > 0
    ? Math.round(((last3Avg - first3Avg) / first3Avg) * 100)
    : 34;
  const chartStep = parseFloat((Math.max(chartFinalVal / 35, 0.01)).toFixed(3));
  const chartTick1 = parseFloat((chartMinVal + (chartMaxVal - chartMinVal) / 3).toFixed(1));
  const chartTick2 = parseFloat((chartMinVal + 2 * (chartMaxVal - chartMinVal) / 3).toFixed(1));
  const trendLabel = chartResult.illustrative
    ? 'illustrative'
    : `${chartTrendPct >= 0 ? '+' : ''}${chartTrendPct}% · 3-month avg vs 12mo ago`;
  const topDeskLabel = chartResult.topDesk
    ? `Led by <b>${escapeHtml(chartResult.topDesk)}</b> this period`
    : 'Across all 24 active desks';
  const chartBullets = chartResult.illustrative
    ? `<li><b>Housing maintenance</b> &middot; illustrative trend +34% / 24mo</li>
        <li><b>Re-let signal</b> &middot; illustrative framework expiry cluster</li>
        <li><b>Entry window</b> &middot; illustrative 18-month renewal window</li>`
    : `<li><b>Awarded spend</b> &middot; ${trendLabel}</li>
        <li>${topDeskLabel}</li>
        <li><b>Re-let signal</b> &middot; framework expiry clusters tracked</li>`;

  const sampleLink = samplePdfUrl
    ? `<a class="btn-ghost" href="${escapeHtml(samplePdfUrl)}" target="_blank" rel="noreferrer">See a sample report &rarr;</a>`
    : `<a class="btn-ghost" href="/pricing" style="font-family:var(--mono);font-size:12px;letter-spacing:.06em;color:#aeb8c0;text-decoration:underline;text-underline-offset:4px;text-decoration-color:#ffffff30">See pricing &rarr;</a>`;

  res.type("html").send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="description" content="UK public-sector revenue intelligence. Turn Contracts Finder and Find a Tender data into a commercial decision for your firm — in minutes.">
<title>GovRevenue — Public-Sector Revenue Intelligence</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Newsreader:ital,opsz,wght@0,6..72,400;0,6..72,500;0,6..72,600;1,6..72,400;1,6..72,500&family=Libre+Franklin:wght@400;500;600;700&family=Spline+Sans+Mono:wght@400;500;600&display=swap');
:root{
  --base:#ECE7DA;--surface:#FBF9F3;--surface-2:#F6F2E8;--surface-3:#EFEADD;
  --brand:#B4924E;--brand-hot:#C4933F;--hero-cta:#102A1E;
  --info:#1d4ed8;--green:#1d6b4f;--gold:#B4924E;
  --text:#1B1E19;--text-mid:#3A3E36;--muted:#86897E;--faint:#9AA093;
  --border:rgba(27,30,25,.10);--border-2:rgba(27,30,25,.16);
  --sans:"Libre Franklin",-apple-system,BlinkMacSystemFont,system-ui,sans-serif;
  --mono:"Spline Sans Mono","SF Mono",ui-monospace,monospace;
  --serif:"Newsreader",Georgia,serif;
}
/* ── global header (pageShellHeader uses these classes) ── */
.gh{background:rgba(236,231,218,0.92);backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);border-bottom:1px solid var(--border-2);position:sticky;top:0;z-index:50}
.gh-inner{padding:0 32px;max-width:1200px;margin:0 auto}
.gh-top{display:flex;align-items:center;justify-content:space-between;height:60px;gap:24px}
.gh-brand{display:flex;align-items:center;gap:9px;flex-shrink:0}
.gh-dot{width:10px;height:10px;background:var(--brand);border-radius:50%;flex-shrink:0}
.gh-logo{font-family:var(--serif);font-weight:500;font-size:20px;letter-spacing:-0.01em;color:var(--text)}
.gh-logo b{color:var(--brand);font-weight:500}
.gh-tag{font-family:var(--mono);font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);border-left:1px solid var(--border-2);padding-left:14px;margin-left:6px}
.gh-live{display:inline-flex;align-items:center;gap:6px;font-family:var(--mono);font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:var(--green)}
.gh-live-dot{width:6px;height:6px;border-radius:50%;background:var(--green);animation:gh-pulse 2.2s infinite}
@keyframes gh-pulse{0%{box-shadow:0 0 0 0 rgba(47,138,82,.4)}70%{box-shadow:0 0 0 5px rgba(47,138,82,0)}100%{box-shadow:0 0 0 0 rgba(47,138,82,0)}}
.gh-nav{display:flex;gap:0;overflow-x:auto;scrollbar-width:none;border-top:1px solid var(--border)}
.gh-nav::-webkit-scrollbar{display:none}
.gh-nav a{font-size:13.5px;font-weight:500;color:var(--text-mid);padding:0 14px;height:38px;display:flex;align-items:center;border-bottom:2px solid transparent;white-space:nowrap;transition:color .15s}
.gh-nav a:hover{color:var(--text)}
.gh-nav a.dnav-active{color:var(--text);border-bottom-color:var(--brand)}
.gh-auth{display:flex;align-items:center;gap:16px;flex-shrink:0}
.gh-auth-name{font-family:var(--mono);font-size:10px;color:var(--muted);max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.gh-auth-link{font-size:14px;font-weight:500;color:var(--text-mid);transition:color .15s}
.gh-auth-link:hover{color:var(--text)}
.gh-auth-cta{display:inline-flex;align-items:center;background:var(--hero-cta);color:#F3EFE6;font-size:13px;font-weight:600;padding:9px 16px;letter-spacing:.01em;transition:opacity .15s}
.gh-auth-cta:hover{opacity:.85}
.gh-main-nav{display:flex;align-items:center;flex:1;padding:0 8px;overflow-x:auto;scrollbar-width:none}
.gh-main-nav::-webkit-scrollbar{display:none}
.gh-main-nav a{font-size:13px;font-weight:500;color:var(--text-mid);padding:0 11px;height:60px;display:flex;align-items:center;white-space:nowrap;transition:color .15s}
.gh-main-nav a:hover{color:var(--text)}
*{box-sizing:border-box;margin:0;padding:0}
html{scroll-behavior:smooth}
body{background:var(--base);color:var(--text);font-family:var(--sans);font-size:16px;line-height:1.55;-webkit-font-smoothing:antialiased;overflow-x:hidden;-webkit-text-size-adjust:100%}
a{color:inherit;text-decoration:none}
.wrap{padding:0 40px;max-width:1320px;margin-left:auto;margin-right:auto}
.eyebrow{font-family:var(--mono);font-size:10px;letter-spacing:.18em;text-transform:uppercase;color:var(--brand)}
.hero{position:relative;background:radial-gradient(120% 160% at 85% 0%,#16341F 0%,#0E2417 60%,#0A1C12 100%);overflow:hidden;border-bottom:1px solid rgba(27,30,25,.18);color:#ECE6D6}
#globe-canvas{position:absolute;inset:0;width:100%;height:100%;z-index:0;opacity:.55}
.hero-grad{position:absolute;inset:0;z-index:1;pointer-events:none;background:linear-gradient(90deg,#0A1C12 20%,transparent 70%)}
.hero .wrap{position:relative;z-index:2;display:grid;grid-template-columns:1.5fr .5fr;gap:32px;align-items:center;min-height:560px;padding:64px 40px}
.hero h1{font-family:var(--serif);font-weight:400;font-size:clamp(38px,4.6vw,60px);line-height:1.04;letter-spacing:-.02em;margin:14px 0 20px;color:#ECE6D6}
.hero h1 em{font-style:italic;color:var(--brand)}
.hero .lede{font-size:16px;line-height:1.65;color:#C5C9BC;max-width:32em;margin-bottom:28px}
.hero-actions{display:flex;gap:14px;align-items:center;flex-wrap:wrap}
.btn-primary{background:var(--brand);color:#10110D;font-family:var(--sans);font-size:14px;font-weight:600;letter-spacing:.01em;padding:14px 24px;transition:.18s}
.btn-primary:hover{background:var(--brand-hot)}
.btn-ghost{font-family:var(--mono);font-size:12px;letter-spacing:.06em;color:#C5C9BC;text-decoration:underline;text-underline-offset:4px;text-decoration-color:rgba(236,230,214,.25)}
.btn-ghost:hover{color:#ECE6D6}
.chips{display:flex;gap:10px;margin-top:28px;flex-wrap:wrap}
.chip{font-family:var(--mono);font-size:11px;letter-spacing:.04em;color:#B8BEB3;border:1px solid rgba(236,230,214,.18);padding:7px 12px;background:rgba(12,31,21,.6);display:flex;gap:8px;align-items:center}
.chip b{color:#ECE6D6;font-weight:600}
.chip .up{color:#6ECF97}
.record{position:relative;border:1px solid rgba(236,230,214,.18);background:rgba(10,28,18,.88);backdrop-filter:blur(12px);box-shadow:0 30px 60px -20px rgba(0,0,0,.5)}
.record .rhead{display:flex;justify-content:space-between;align-items:center;padding:8px 14px;border-bottom:1px solid rgba(236,230,214,.12)}
.record .rhead .t{font-family:var(--mono);font-size:10px;letter-spacing:.16em;text-transform:uppercase;color:#ECE6D6}
.record .rhead .src{font-family:var(--mono);font-size:10px;color:#B8BEB3}
.record .rbody{padding:4px 14px}
.rrow{display:flex;justify-content:space-between;align-items:flex-start;padding:9px 0;border-bottom:1px solid rgba(236,230,214,.10)}
.rrow:last-child{border-bottom:0}
.rrow .k{font-family:var(--mono);font-size:9.5px;letter-spacing:.1em;text-transform:uppercase;color:#8A9187;padding-top:3px}
.rrow .v{text-align:right;font-family:var(--serif);font-size:14px;font-weight:400;color:#ECE6D6;max-width:65%}
.rrow .v small{display:block;font-family:var(--mono);font-size:10px;color:#8A9187;margin-top:4px;font-weight:400}
.figure{font-family:var(--serif);font-size:26px;font-weight:500;color:#ECE6D6}
.verdict{display:inline-block;font-family:var(--mono);font-size:11px;letter-spacing:.1em;text-transform:uppercase;background:rgba(180,146,78,.18);color:#D4A955;border:1px solid rgba(180,146,78,.35);padding:5px 11px}
.caveat{padding:9px 14px 12px;font-family:var(--mono);font-size:9.5px;color:#8A9187;line-height:1.5;border-top:1px solid rgba(236,230,214,.10)}
.hc-link{color:inherit;text-decoration:underline;text-underline-offset:3px;text-decoration-color:rgba(255,255,255,.15);transition:text-decoration-color .15s}
.hc-link:hover{text-decoration-color:rgba(255,255,255,.5)}
.caveat b{color:#D4A955}
.spark{width:100%;height:46px;display:block;margin:2px 0 10px}
.ticker{background:var(--surface-2);border-bottom:1px solid var(--border);overflow:hidden}
.ticker .row{display:flex;gap:48px;white-space:nowrap;font-family:var(--mono);font-size:11.5px;letter-spacing:.06em;padding:11px 0;animation:scroll 160s linear infinite;width:max-content;color:var(--muted)}
.ticker .row span b{color:var(--brand);font-weight:600;margin-right:8px}
@keyframes scroll{from{transform:translateX(0)}to{transform:translateX(-50%)}}
.chartband{background:var(--surface);border-bottom:1px solid var(--border);padding:72px 0}
.chartband .wrap{display:grid;grid-template-columns:.85fr 1.15fr;gap:56px;align-items:center}
.chartband h2{font-family:var(--serif);font-size:28px;font-weight:400;letter-spacing:-.02em;margin:10px 0 14px;line-height:1.1;color:var(--text)}
.chartband p{color:var(--muted);font-size:15px;max-width:30em;margin-bottom:16px}
.chartband ul{list-style:none;font-family:var(--mono);font-size:12px;letter-spacing:.03em;color:var(--muted)}
.chartband li{padding:8px 0;border-bottom:1px solid var(--border)}
.chartband li b{color:var(--text-mid)}
.chartwrap{border:1px solid var(--border-2);background:var(--surface-2);padding:20px 22px 14px;position:relative}
.chartwrap .ch-head{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:6px}
.chartwrap .ch-head .lab{font-family:var(--mono);font-size:10.5px;letter-spacing:.1em;text-transform:uppercase;color:var(--muted)}
.chartwrap .ch-head .big{font-family:var(--serif);font-size:24px;font-weight:500;color:var(--text)}
.chartwrap .ch-head .big .up{color:var(--green);font-size:13px;margin-left:6px}
#growthChart{width:100%;height:220px;display:block}
.section{padding:68px 0;border-bottom:1px solid var(--border)}
.section-head{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:32px}
.section-head h2{font-family:var(--serif);font-size:26px;font-weight:400;letter-spacing:-.01em;color:var(--text)}
.section-head a{font-family:var(--mono);font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);text-decoration:underline;text-underline-offset:4px}
.section-head a:hover{color:var(--text-mid)}
#desks{background:var(--base)}
.desk-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}
.desk-card{display:flex;flex-direction:column;gap:10px;padding:18px 20px;border:1px solid var(--border-2);text-decoration:none;color:inherit;background:var(--surface);position:relative;overflow:hidden;transition:border-color .2s,background .2s}
.desk-card:hover{border-color:rgba(255,255,255,.2);background:var(--surface-2)}
.dc-top{display:flex;align-items:center;justify-content:space-between;gap:8px}
.dc-label{font-family:var(--mono);font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:var(--brand);flex-shrink:0}
.dc-chips{display:flex;gap:5px;flex-wrap:wrap;justify-content:flex-end}
.dc-chip{font-family:var(--mono);font-size:9.5px;padding:2px 7px;border-radius:2px;white-space:nowrap}
.dc-chip-value{background:rgba(29,107,79,.08);color:#1d6b4f;border:1px solid rgba(29,107,79,.22)}
.dc-chip-open{background:rgba(29,107,79,.08);color:#1d6b4f;border:1px solid rgba(29,107,79,.22)}
.dc-chip-awarded{background:var(--surface-2);color:var(--muted);border:1px solid var(--border-2)}
.dc-chip-src{background:rgba(29,78,216,.06);color:#1d4ed8;border:1px solid rgba(29,78,216,.18)}
.dc-title{font-family:var(--sans);font-size:14px;line-height:1.35;font-weight:600;color:var(--text)}
.dc-buyer{font-size:12px;color:var(--muted);line-height:1.4}
.dc-foot{display:flex;align-items:center;justify-content:space-between;margin-top:4px}
.dc-date{font-family:var(--mono);font-size:10.5px;color:var(--faint)}
.dc-cta{font-family:var(--mono);font-size:10.5px;color:var(--brand);letter-spacing:.05em}
.desk-card:hover .dc-cta{text-decoration:underline}
.reveal{opacity:0;transform:translateY(20px);transition:opacity .7s ease,transform .7s ease}
.reveal.in{opacity:1;transform:none}
.scan-strip{background:#102A1E;color:#ECE6D6;padding:48px 0}
.scan-strip .wrap{display:flex;align-items:center;justify-content:space-between;gap:40px}
.scan-strip-eyebrow{font-family:var(--mono);font-size:10px;letter-spacing:.18em;text-transform:uppercase;opacity:.65;margin-bottom:8px;color:var(--brand)}
.scan-strip-hed{font-family:var(--serif);font-size:26px;font-weight:400;line-height:1.1;margin-bottom:8px;letter-spacing:-.01em;color:#ECE6D6}
.scan-strip-sub{font-size:14px;color:#C5C9BC;max-width:38em;line-height:1.6}
.scan-strip-right{display:flex;flex-direction:column;align-items:flex-end;gap:12px;flex-shrink:0}
.scan-strip-btn{font-family:var(--sans);font-size:14px;font-weight:600;background:var(--brand);color:#10110D;padding:14px 26px;white-space:nowrap;transition:.18s}
.scan-strip-btn:hover{background:var(--brand-hot)}
.scan-strip-price{font-family:var(--mono);font-size:11px;letter-spacing:.06em;text-transform:uppercase;opacity:.65;text-decoration:underline;text-underline-offset:3px;text-align:right;color:#C5C9BC}
.scan-strip-price:hover{opacity:1}
.product{background:var(--surface);border-bottom:1px solid var(--border)}
.product .wrap{display:grid;grid-template-columns:1fr 1fr;gap:60px;padding:72px 40px;align-items:center}
.product .eyebrow{color:var(--brand)}
.product h2{font-family:var(--serif);font-size:34px;line-height:1.08;font-weight:400;letter-spacing:-.02em;margin:14px 0 18px;color:var(--text)}
.product h2 em{font-style:italic;color:var(--brand)}
.product p{color:var(--muted);max-width:34em;margin-bottom:24px;font-size:15px}
.steps{border-top:1px solid var(--border)}
.step{display:flex;gap:18px;padding:14px 0;border-bottom:1px solid var(--border);align-items:baseline}
.step .n{font-family:var(--mono);font-size:11px;color:var(--brand);min-width:28px}
.step .x b{font-weight:600;color:var(--text)}
.step .x small{display:block;font-family:var(--mono);font-size:10.5px;color:var(--muted);margin-top:3px}
.subscribe{padding:72px 0;text-align:center;background:var(--surface-2)}
.subscribe .eyebrow{margin-bottom:14px}
.subscribe h2{font-family:var(--serif);font-size:30px;font-weight:400;letter-spacing:-.01em;margin-bottom:14px;color:var(--text)}
.subscribe p{color:var(--muted);max-width:34em;margin:0 auto 28px;font-size:15px}
.subform{display:flex;max-width:460px;margin:0 auto;border:1px solid var(--border-2)}
.subform input{flex:1;border:0;padding:14px 16px;font-family:var(--sans);font-size:14px;background:var(--surface);color:var(--text)}
.subform input::placeholder{color:var(--muted)}
.subform input:focus{outline:2px solid var(--brand);outline-offset:-2px}
.subform button{background:#102A1E;color:#ECE6D6;border:0;font-family:var(--sans);font-size:13px;font-weight:600;letter-spacing:.01em;padding:0 22px;cursor:pointer;transition:.18s}
.subform button:hover{background:#0A1C12}
.subnote{font-family:var(--mono);font-size:10.5px;color:var(--muted);margin-top:14px}
footer.hp-foot{background:#102A1E;color:#9AA093;padding:54px 0 40px;font-size:13px;border-top:1px solid rgba(236,230,214,.1)}
footer.hp-foot .wrap{display:grid;grid-template-columns:2fr 1fr 1fr 1fr;gap:40px}
footer.hp-foot .logo{color:#ECE6D6;font-family:var(--serif);font-size:20px;font-weight:500;letter-spacing:-.01em;margin-bottom:12px}
footer.hp-foot .logo b{color:var(--brand)}
footer.hp-foot p.bl{max-width:26em;line-height:1.5;color:#9AA093;font-size:13px}
footer.hp-foot h4{font-family:var(--mono);font-size:10px;letter-spacing:.16em;text-transform:uppercase;color:#C5C9BC;margin-bottom:14px}
footer.hp-foot ul{list-style:none}
footer.hp-foot li{margin-bottom:9px}
footer.hp-foot a{color:#9AA093}
footer.hp-foot a:hover{color:#ECE6D6}
footer.hp-foot .legal{grid-column:1/-1;border-top:1px solid rgba(236,230,214,.1);margin-top:28px;padding-top:20px;display:flex;justify-content:space-between;font-family:var(--mono);font-size:10.5px;color:#6B6F65;flex-wrap:wrap;gap:10px}
@media(max-width:880px){
  .scan-strip .wrap{flex-direction:column;align-items:flex-start;gap:24px}
  .scan-strip-right{align-items:flex-start}
  .hero .wrap,.chartband .wrap,.product .wrap{grid-template-columns:1fr;gap:32px}
  .hero h1{font-size:36px}
  #globe-canvas{opacity:.3}
  .hero-grad{background:none}
  .desk-grid{grid-template-columns:1fr 1fr}
  footer.hp-foot .wrap{grid-template-columns:1fr 1fr}
  .section{padding:52px 0}
  .chartband{padding:52px 0}
}
@media(max-width:760px){
  .gh-tag,.gh-live,.gh-auth-name,.gh-main-nav{display:none}
  .gh-inner{padding-left:16px;padding-right:16px}
  .gh-auth{gap:10px}
  .gh-auth-cta{padding:8px 12px;font-size:12px}
  .gh-auth-link{font-size:13px}
  .desk-grid{grid-template-columns:1fr}
  .hero .wrap{min-height:auto;padding:48px 20px 40px}
  .hero h1{font-size:32px}
  .hero-actions{gap:12px}
  .chips{gap:8px}
  .chip{font-size:10px;padding:6px 10px}
}
@media(max-width:480px){
  .wrap{padding:0 16px}
  .hero .wrap{padding:36px 16px 32px}
  .hero h1{font-size:26px}
  .hero .lede{font-size:14px}
  .section{padding:40px 0}
  .chartband{padding:40px 0}
  .scan-strip{padding:36px 0}
  footer.hp-foot .wrap{grid-template-columns:1fr}
  footer.hp-foot{padding:32px 0 24px}
  footer.hp-foot .legal{flex-direction:column;gap:6px}
}
@media(prefers-reduced-motion:reduce){*{animation:none!important;scroll-behavior:auto}.reveal{opacity:1;transform:none}}
:focus-visible{outline:2px solid var(--brand);outline-offset:2px}
${chaseNowCss()}
${oppCardCss()}
</style>
</head>
<body>
${pageShellHeader(null, homepageAuth)}
<section class="hero">
  <canvas id="globe-canvas"></canvas>
  <div class="hero-grad"></div>
  <div class="wrap">
    <div>
      <div class="eyebrow">Opportunity Radar &middot; UK Public Sector</div>
      <h1>See government demand<br>before it <em>becomes a tender.</em></h1>
      <p class="lede">Public bodies already spend on what you sell. We surface the buyers, frameworks, and spend patterns that signal the next opportunity &mdash; so your firm enters with intelligence, not hope.</p>
      <div class="hero-actions">
        <a class="btn-primary" href="/scan">Run a revenue scan</a>
        ${sampleLink}
      </div>
      <div class="chips">
        <div class="chip"><b>&pound;400bn</b> annual public spend</div>
        <div class="chip"><b id="liveNotices">${noticesDisplay}</b> signals tracked &middot; 24h</div>
        <div class="chip">Live: Contracts Finder + Find a Tender</div>
      </div>
    </div>
    <div class="record">
      <div class="rhead"><span class="t" id="hc-type">${isLive ? "Live signal" : "Illustrative signal"}</span><span class="src" id="hc-src">${escapeHtml(heroSource)} &middot; public record</span></div>
      <div class="rbody">
        <svg class="spark" id="spark" viewBox="0 0 320 46" preserveAspectRatio="none"></svg>
        <div class="rrow"><span class="k">Category</span><span class="v" id="hc-cat">${escapeHtml(heroCategory)}<small id="hc-date" data-ts="${escapeHtml(heroDateRaw || "")}">${escapeHtml(heroDate)}</small></span></div>
        <div class="rrow"><span class="k">Notice</span><span class="v" style="font-size:14px;line-height:1.3">${heroUrl ? `<a id="hc-title" class="hc-link" href="${escapeHtml(heroUrl)}" target="_blank" rel="noopener noreferrer">${escapeHtml(heroTitle)}</a>` : `<span id="hc-title">${escapeHtml(heroTitle)}</span>`}</span></div>
        <div class="rrow"><span class="k">Buyer</span><span class="v" id="hc-buyer" style="font-size:16px">${escapeHtml(heroBuyer)}</span></div>
        <div class="rrow"><span class="k">Value</span><span class="v" id="hc-val">${heroVal}<small id="hc-status">${escapeHtml(heroStatus)}</small></span></div>
      </div>
      <div class="caveat" id="hc-caveat"><b>Caveat.</b> ${isLive ? "Source: public procurement record. Confidence varies by notice quality — buyer names taken verbatim, not verified." : "Illustrative sample &mdash; live data loads on first refresh (hourly)."}</div>
    </div>
  </div>
</section>
<div class="ticker" aria-hidden="true"><div class="row" id="tickerRow">${tickerHtml}</div></div>
<section class="chartband" id="chart">
  <div class="wrap">
    <div class="reveal">
      <div class="eyebrow">All-desk spend signal &middot; ${chartResult.illustrative ? 'illustrative' : 'live'}</div>
      <h2>Track the spend curve before the tender lands.</h2>
      <p>Recurring spend in a category is the leading indicator. When it climbs, re-lets and frameworks follow. We track the curve so you enter on the upswing, not after the award.</p>
      <ul>
        ${chartBullets}
      </ul>
      ${!chartResult.illustrative ? `<a href="/desks" style="display:inline-block;margin-top:18px;font-family:var(--mono);font-size:11.5px;letter-spacing:.08em;text-transform:uppercase;color:var(--brand);border-bottom:1px solid currentColor;padding-bottom:2px">Browse all desks &rarr;</a>` : ''}
    </div>
    <div class="chartwrap">
      <div class="ch-head">
        <div>
          <span class="lab">UK public-sector awarded${chartResult.illustrative ? ' <span style="font-size:9px;opacity:.5;letter-spacing:.06em">&middot; ILLUSTRATIVE</span>' : ''}</span>
          <span class="lab" style="display:block;font-size:10px;opacity:.6;margin-top:2px">Monthly totals &middot; rolling 12 months</span>
        </div>
        <span class="big" id="chartTotal"><span id="chartTotalVal">&pound;0.0m</span><span class="up">&#9650; ${Math.abs(chartTrendPct)}%</span></span>
      </div>
      <canvas id="growthChart" style="cursor:pointer" onclick="location.href='/charts'"></canvas>
      <a href="/charts" style="display:block;text-align:right;font-family:var(--mono);font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:var(--brand);border-bottom:1px solid currentColor;padding-bottom:1px;width:fit-content;margin:10px 0 0 auto">View intelligence &rarr;</a>
    </div>
  </div>
</section>
${chaseNowHtml}
<section class="section" id="desks">
  <div class="wrap">
    <div class="section-head"><h2>The desks</h2></div>
    <div class="desk-grid">
      ${DESK_PROFILES.filter(d => d.live).slice(0, 9).map(d => {
        const sig = deskSignals.get(d.slug);
        return sig
          ? renderDeskCard(sig, d.label, d.slug)
          : `<a class="desk-card reveal" href="/desk/${escapeHtml(d.slug)}"><div class="dc-top"><span class="dc-label">${escapeHtml(d.label)}</span><div class="dc-chips"><span class="dc-chip dc-chip-src">CF</span></div></div><div class="dc-title">Scanning for live notices…</div><div class="dc-buyer">Signals load on first hourly refresh.</div><div class="dc-foot"><span class="dc-date">—</span><span class="dc-cta">View desk &rarr;</span></div></a>`;
      }).join("")}
    </div>
    <div style="text-align:center;margin-top:32px">
      <a href="/desks" style="display:inline-flex;align-items:center;gap:8px;font-family:var(--mono);font-size:12px;letter-spacing:.1em;text-transform:uppercase;color:var(--text-mid);border:1px solid var(--border-2);padding:12px 28px;transition:border-color .15s,color .15s">See all ${DESK_PROFILES.filter(d => d.live).length} desks &rarr;</a>
    </div>
  </div>
</section>
<section class="scan-strip">
  <div class="wrap">
    <div class="scan-strip-left">
      <div class="scan-strip-eyebrow">Intelligence scan &middot; 2&ndash;4 minutes</div>
      <div class="scan-strip-hed">Know your position before you bid.</div>
      <div class="scan-strip-sub">Tell us your services and region. We scan the public record and return a sourced commercial verdict &mdash; buyer watchlist, route-to-revenue map, and bid readiness score.</div>
    </div>
    <div class="scan-strip-right">
      <a class="scan-strip-btn" href="/scan">Run a revenue scan &rarr;</a>
      <a class="scan-strip-price" href="/pricing">See pricing &rarr;</a>
    </div>
  </div>
</section>
<section class="product" id="product">
  <div class="wrap">
    <div>
      <div class="eyebrow">The product underneath</div>
      <h2>One profile in.<br>A <em>sourced verdict</em> out.</h2>
      <p>Submit your firm&rsquo;s services, region and contract range. The intelligence engine scans the public record, scores route-to-revenue fit, and returns a professional brief &mdash; every claim timestamped, sourced, and caveated.</p>
      <a class="btn-primary" href="/scan">Run your first scan</a>
    </div>
    <div class="steps">
      <div class="step"><span class="n">01</span><span class="x"><b>Profile</b><small>Services &middot; region &middot; contract range &middot; evidence</small></span></div>
      <div class="step"><span class="n">02</span><span class="x"><b>Scan</b><small>Contracts Finder &middot; Find a Tender &middot; LA spend &middot; awards</small></span></div>
      <div class="step"><span class="n">03</span><span class="x"><b>Score</b><small>Buyer fit &middot; evidence grade &middot; route-to-revenue</small></span></div>
      <div class="step"><span class="n">04</span><span class="x"><b>Verdict</b><small>Bid &middot; partner &middot; monitor &middot; prepare &middot; ignore</small></span></div>
    </div>
  </div>
</section>
<section class="subscribe" id="subscribe">
  <div class="wrap">
    <div class="eyebrow">Join the briefing</div>
    <h2>Intelligence before the tender.</h2>
    <p>One short note when new public money moves in your category. No daily emails. No discount codes &mdash; those are not on offer.</p>
    <form class="subform" id="briefing-form">
      <input type="email" id="briefing-email" placeholder="you@firm.co.uk" aria-label="Email address" required>
      <button type="submit">Reserve</button>
    </form>
    <div class="subnote" id="briefing-note">By subscribing you agree to our privacy notice. Unsubscribe anytime.</div>
    <script>
    document.getElementById(‘briefing-form’).addEventListener(‘submit’,function(e){
      e.preventDefault();
      const email=document.getElementById(‘briefing-email’).value;
      const note=document.getElementById(‘briefing-note’);
      fetch(‘/api/briefing’,{method:’POST’,headers:{‘Content-Type’:’application/json’},body:JSON.stringify({email})})
        .then(function(r){return r.json();})
        .then(function(d){
          document.getElementById(‘briefing-form’).style.display=’none’;
          note.textContent=d.alreadySubscribed?’You’re already on the list.’:’Done. We’ll write when the money moves.’;
          note.style.color=’#1d6b4f’;note.style.fontWeight=’600’;
        })
        .catch(function(){note.textContent=’Something went wrong — try again.’;note.style.color=’#9b2d20’;});
    });
    </script>
  </div>
</section>
<footer class="hp-foot"><div class="wrap">
  <div><div class="logo">Gov<b>Revenue</b></div><p class="bl">Bloomberg Terminal for UK public-sector revenue. We turn fragmented procurement data into one sourced commercial decision: bid, partner, monitor, prepare, or ignore.</p></div>
  <div><h4>Desks</h4><ul>${DESK_PROFILES.slice(0, 5).map(d => `<li><a href="/desk/${d.slug}">${escapeHtml(d.label)}</a></li>`).join("")}<li><a href="/desks">All desks &rarr;</a></li></ul></div>
  <div><h4>Product</h4><ul><li><a href="/scan">Intelligence Scan</a></li><li><a href="/desks">Sector Desks</a></li><li><a href="/charts">Opportunity Radar</a></li><li><a href="/pricing">Pricing</a></li></ul></div>
  <div><h4>Sources</h4><ul><li><a href="https://www.gov.uk/contracts-finder" target="_blank" rel="noopener noreferrer">Contracts Finder</a></li><li><a href="https://www.find-tender.service.gov.uk" target="_blank" rel="noopener noreferrer">Find a Tender</a></li><li><a href="https://www.gov.uk/government/publications/local-government-transparency-code-2015" target="_blank" rel="noopener noreferrer">LA transparency</a></li><li><a href="https://find-and-update.company-information.service.gov.uk" target="_blank" rel="noopener noreferrer">Companies House</a></li></ul></div>
  <div class="legal"><span>&copy; 2026 GovRevenue &middot; United Kingdom</span><span>Intelligence, not certainty. Public data only.</span></div>
</div></footer>
<script>
const reduce = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
(function(){
  const cv=document.getElementById('globe-canvas');
  if(!cv) return;
  const ctx=cv.getContext('2d');
  if(!ctx) return;
  let W=0,H=0,R=0,cx=0,cy=0,angle=0,ready=false;
  const DOTS=[];
  for(let i=0;i<300;i++){
    const phi=Math.acos(2*Math.random()-1),th=2*Math.PI*Math.random();
    DOTS.push({phi,th,r:Math.random()>.72?3:1.8,a:0.6+Math.random()*0.4});
  }
  function resize(){
    const pr=Math.min(window.devicePixelRatio||1,2);
    const hero=document.querySelector('.hero');
    const rw=hero?hero.offsetWidth:window.innerWidth;
    const rh=hero?hero.offsetHeight:600;
    if(rw<10||rh<10) return;
    W=rw; H=rh;
    cv.width=Math.round(W*pr); cv.height=Math.round(H*pr);
    ctx.setTransform(pr,0,0,pr,0,0);
    cx=W*0.73; cy=H*0.50;
    R=Math.min(W*0.31,H*0.43);
    ready=true;
  }
  window.addEventListener('resize',resize);
  function tryInit(){resize();if(!ready){setTimeout(tryInit,80);}else{frame();}}
  setTimeout(tryInit,0);
  function proj(phi,th,a){
    const x=Math.sin(phi)*Math.cos(th+a),y=Math.cos(phi),z=Math.sin(phi)*Math.sin(th+a);
    return{x:cx+R*x,y:cy-R*y*0.97,z,vis:z>-0.14};
  }
  function frame(){
    ctx.clearRect(0,0,W,H);
    /* atmosphere */
    const atm=ctx.createRadialGradient(cx,cy,R*0.8,cx,cy,R*1.7);
    atm.addColorStop(0,'rgba(180,146,78,0.18)');
    atm.addColorStop(0.6,'rgba(50,90,130,0.10)');
    atm.addColorStop(1,'rgba(6,9,15,0)');
    ctx.fillStyle=atm; ctx.beginPath(); ctx.arc(cx,cy,R*1.7,0,6.3); ctx.fill();
    /* sphere — distinctly lighter than hero bg (#0B0F14) */
    const body=ctx.createRadialGradient(cx-R*0.28,cy-R*0.26,R*0.05,cx,cy,R);
    body.addColorStop(0,'rgba(44,80,118,0.95)');
    body.addColorStop(0.5,'rgba(22,42,66,0.97)');
    body.addColorStop(1,'rgba(10,16,26,1)');
    ctx.fillStyle=body; ctx.beginPath(); ctx.arc(cx,cy,R,0,6.3); ctx.fill();
    /* grid — clipped to sphere */
    ctx.save(); ctx.beginPath(); ctx.arc(cx,cy,R,0,6.3); ctx.clip();
    for(let lat=-75;lat<=75;lat+=15){
      const phi=(90-lat)*Math.PI/180;
      ctx.strokeStyle='rgba(100,160,210,0.38)'; ctx.lineWidth=0.9;
      ctx.beginPath(); let f=true;
      for(let lon=0;lon<=363;lon+=3){const p=proj(phi,lon*Math.PI/180,angle);if(!p.vis){f=true;continue;}if(f){ctx.moveTo(p.x,p.y);f=false;}else ctx.lineTo(p.x,p.y);}
      ctx.stroke();
    }
    for(let lon=0;lon<360;lon+=20){
      ctx.strokeStyle='rgba(100,160,210,0.28)'; ctx.lineWidth=0.9;
      ctx.beginPath(); let f=true;
      for(let lat=90;lat>=-90;lat-=3){const p=proj((90-lat)*Math.PI/180,lon*Math.PI/180,angle);if(!p.vis){f=true;continue;}if(f){ctx.moveTo(p.x,p.y);f=false;}else ctx.lineTo(p.x,p.y);}
      ctx.stroke();
    }
    /* dots */
    for(const d of DOTS){
      const p=proj(d.phi,d.th,angle); if(!p.vis) continue;
      const depth=(p.z+0.14)/1.14, alpha=Math.min(d.a*depth,0.95);
      if(d.r>2){ctx.beginPath();ctx.arc(p.x,p.y,d.r*3,0,6.3);ctx.fillStyle='rgba(180,146,78,'+(alpha*0.22).toFixed(2)+')';ctx.fill();}
      ctx.beginPath(); ctx.arc(p.x,p.y,d.r,0,6.3);
      ctx.fillStyle='rgba(180,146,78,'+alpha.toFixed(2)+')'; ctx.fill();
    }
    ctx.restore();
    /* specular highlight */
    const spec=ctx.createRadialGradient(cx-R*0.30,cy-R*0.28,0,cx-R*0.16,cy-R*0.14,R*0.55);
    spec.addColorStop(0,'rgba(160,200,235,0.28)'); spec.addColorStop(1,'rgba(160,200,235,0)');
    ctx.fillStyle=spec; ctx.beginPath(); ctx.arc(cx,cy,R,0,6.3); ctx.fill();
    /* rim */
    ctx.strokeStyle='rgba(110,160,200,0.50)'; ctx.lineWidth=1.5;
    ctx.beginPath(); ctx.arc(cx,cy,R,0,6.3); ctx.stroke();
    if(!reduce) angle+=0.0032;
    requestAnimationFrame(frame);
  }
})();
(function(){
  const cv=document.getElementById('growthChart'); if(!cv) return;
  const ctx=cv.getContext('2d');
  const data=${JSON.stringify(chartPoints)};
  function fit(){const dpr=Math.min(devicePixelRatio,2);const r=cv.getBoundingClientRect();cv.width=r.width*dpr;cv.height=r.height*dpr;ctx.setTransform(dpr,0,0,dpr,0,0);return r;}
  let r=fit(); window.addEventListener('resize',()=>{r=fit();});
  const pad={l:34,r:8,t:14,b:22},max=${chartMaxVal},min=${chartMinVal};
  function X(i){return pad.l+(i/(data.length-1))*(r.width-pad.l-pad.r);}
  function Y(v){return pad.t+(1-(v-min)/(max-min))*(r.height-pad.t-pad.b);}
  let prog=0,started=false;
  function draw(){
    ctx.clearRect(0,0,r.width,r.height);
    ctx.strokeStyle='rgba(255,255,255,0.06)';ctx.lineWidth=1;ctx.font='10px monospace';ctx.fillStyle='#8893A4';
    for(const g of [${chartTick1},${chartTick2}]){const y=Y(g);ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(r.width-pad.r,y);ctx.stroke();ctx.fillText(g>=1000?'£'+(g/1000).toFixed(1)+'bn':'£'+g+'m',6,y+3);}
    const upto=prog*(data.length-1);
    ctx.beginPath();ctx.moveTo(X(0),Y(data[0]));
    for(let i=1;i<=Math.floor(upto);i++) ctx.lineTo(X(i),Y(data[i]));
    const fi=Math.floor(upto),fr=upto-fi;
    if(fi<data.length-1){const cy=data[fi]+(data[fi+1]-data[fi])*fr;ctx.lineTo(X(fi+fr),Y(cy));}
    const lastX=fi<data.length-1?X(fi+fr):X(data.length-1);
    ctx.lineTo(lastX,r.height-pad.b);ctx.lineTo(X(0),r.height-pad.b);ctx.closePath();
    const grad=ctx.createLinearGradient(0,pad.t,0,r.height-pad.b);
    grad.addColorStop(0,'rgba(180,146,78,0.18)');grad.addColorStop(1,'rgba(180,146,78,0)');ctx.fillStyle=grad;ctx.fill();
    ctx.beginPath();ctx.moveTo(X(0),Y(data[0]));
    for(let i=1;i<=Math.floor(upto);i++) ctx.lineTo(X(i),Y(data[i]));
    if(fi<data.length-1){const cy=data[fi]+(data[fi+1]-data[fi])*fr;ctx.lineTo(X(fi+fr),Y(cy));}
    ctx.strokeStyle='#B4924E';ctx.lineWidth=2.4;ctx.lineJoin='round';ctx.stroke();
    const hy=fi<data.length-1?(data[fi]+(data[fi+1]-data[fi])*fr):data[data.length-1];
    ctx.beginPath();ctx.arc(lastX,Y(hy),4.5,0,7);ctx.fillStyle='#B4924E';ctx.fill();
    ctx.beginPath();ctx.arc(lastX,Y(hy),9,0,7);ctx.fillStyle='rgba(180,146,78,0.2)';ctx.fill();
  }
  function animate(){if(prog<1){prog+=reduce?1:0.018;if(prog>1)prog=1;draw();requestAnimationFrame(animate);}else draw();}
  const io=new IntersectionObserver(es=>es.forEach(e=>{if(e.isIntersecting&&!started){started=true;animate();
    const el=document.getElementById('chartTotalVal');let v=0;
    const cfv=${chartFinalVal},cst=${chartStep};
    const ci=setInterval(()=>{v+=cst;if(v>=cfv){v=cfv;clearInterval(ci);}el.textContent=v>=1000?'£'+(v/1000).toFixed(2)+'bn':'£'+v.toFixed(1)+'m';},22);
  }}),{threshold:.4});
  io.observe(cv);
})();
(function(){
  const s=document.getElementById('spark'); if(!s) return;
  const d=${JSON.stringify(chartPoints.length >= 3 ? chartPoints : [6,9,7,12,11,16,14,20,19,26,24,32,30,40])};
  const max=Math.max(...d)*1.15||42,W=320,H=46; let pts='';
  d.forEach((v,i)=>{pts+=(i?' ':'')+( i/(d.length-1)*W).toFixed(1)+','+(H-(v/max)*H).toFixed(1);});
  s.innerHTML='<polyline points="'+pts+'" fill="none" stroke="#B4924E" stroke-width="1.6" vector-effect="non-scaling-stroke" stroke-linecap="round"/>';
})();
/* hero card value is server-rendered; no count-up needed */
/* ticker + count are server-rendered; no client fill needed */
(function(){
  const io=new IntersectionObserver(es=>es.forEach(e=>{if(e.isIntersecting){e.target.classList.add('in');io.unobserve(e.target);}}),{threshold:.05,rootMargin:'0px 0px 60px 0px'});
  document.querySelectorAll('.reveal').forEach(function(el){
    const r=el.getBoundingClientRect();
    if(r.top<window.innerHeight&&r.bottom>0){el.classList.add('in');}
    else{io.observe(el);}
  });
})();
(function(){
  if(reduce) return;
  function esc(s){const d=document.createElement('div');d.textContent=s;return d.innerHTML;}
  function g(id){return document.getElementById(id);}
  function ta(isoStr){
    if(!isoStr) return '—';
    const m=Math.floor((Date.now()-new Date(isoStr).getTime())/60000);
    if(m<1) return 'just now';
    if(m<60) return m+' min'+(m!==1?'s':'')+' ago';
    const h=Math.floor(m/60);
    if(h<24) return h+'h ago';
    return Math.floor(h/24)+'d ago';
  }
  function refreshDate(){
    const el=g('hc-date');
    if(el&&el.dataset.ts) el.textContent=ta(el.dataset.ts);
  }
  refreshDate();
  const dateT=setInterval(refreshDate,30000);
  function poll(){
    fetch('/api/signals/latest').then(function(r){return r.ok?r.json():null;}).then(function(d){
      if(!d||!d.hero) return;
      const h=d.hero;
      if(g('hc-type')) g('hc-type').textContent=h.type;
      if(g('hc-src')) g('hc-src').textContent=h.src+' · public record';
      if(g('hc-cat')&&g('hc-cat').childNodes[0]) g('hc-cat').childNodes[0].nodeValue=h.category;
      const de=g('hc-date');
      if(de){de.dataset.ts=h.date||'';de.textContent=ta(h.date);}
      const tEl=g('hc-title');
      if(tEl){tEl.textContent=h.title;if(tEl.tagName==='A'&&h.url)tEl.setAttribute('href',h.url);}
      if(g('hc-buyer')) g('hc-buyer').textContent=h.buyer;
      if(g('hc-val')&&g('hc-val').childNodes[0]) g('hc-val').childNodes[0].nodeValue=h.val;
      if(g('hc-status')) g('hc-status').textContent=h.status;
      if(g('hc-caveat')) g('hc-caveat').innerHTML='<b>Caveat.</b> '+esc(h.caveat);
      if(d.count24h&&g('liveNotices')) g('liveNotices').textContent=String(d.count24h);
    }).catch(function(){});
  }
  const t=setInterval(poll,75000);
  window.addEventListener('pagehide',function(){clearInterval(t);clearInterval(dateT);});
})();
</script>
</body>
</html>`);
}));


app.get("/api/scans/:id/status", asyncRoute(async (req, res) => {
  const scan = await getScan(req.params.id);
  if (!scan) { res.status(404).json({ error: "Not found" }); return; }
  res.json({ status: scan.status, progress_stage: scan.progress_stage || null, error_message: scan.error_message || null });
}));

app.get("/api/scans/:id/stream", (req, res) => {
  const scanId = req.params.id;
  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    "X-Accel-Buffering": "no",
    "Connection": "keep-alive"
  });
  res.write(": connected\n\n");
  let closed = false;
  function send(stage: string) {
    if (!closed) res.write(`data: ${JSON.stringify({ stage })}\n\n`);
  }
  getScan(scanId).then(scan => {
    if (!scan) { send("not_found"); res.end(); closed = true; return; }
    if (scan.status === "completed") { send("done"); res.end(); closed = true; return; }
    if (scan.status === "failed") { send("failed"); res.end(); closed = true; return; }
    if (scan.progress_stage) send(scan.progress_stage);
  }).catch(() => {});
  const listener = (stage: string) => {
    send(stage);
    if (stage === "done" || stage === "failed") { res.end(); closed = true; }
  };
  scanEvents.on(`scan:${scanId}`, listener);
  const poll = setInterval(async () => {
    if (closed) { clearInterval(poll); return; }
    const scan = await getScan(scanId).catch(() => null);
    if (!scan) return;
    if (scan.status === "completed") { send("done"); clearInterval(poll); res.end(); closed = true; }
    else if (scan.status === "failed") { send("failed"); clearInterval(poll); res.end(); closed = true; }
  }, 5000);
  req.on("close", () => {
    closed = true;
    scanEvents.off(`scan:${scanId}`, listener);
    clearInterval(poll);
  });
});

app.get("/api/signals/latest", asyncRoute(async (_req, res) => {
  const [deskSigs, count24h] = await Promise.all([
    queryDeskSignals(DESK_PROFILES.filter(d => d.live).map(d => d.slug)).catch(() => new Map<string, HomepageSignal>()),
    count24hSignals().catch(() => 0)
  ]);
  const signals = [...deskSigs.values()]
    .filter(s => s.notice_date)
    .sort((a, b) => new Date(b.notice_date!).getTime() - new Date(a.notice_date!).getTime());
  const hero = signals[0] || null;
  const ticker = signals.map(s => ({
    src: s.source,
    title: s.title.slice(0, 70),
    buyer: s.buyer ? s.buyer.slice(0, 40) : null
  }));
  const heroOut = hero ? {
    type: "Live signal",
    src: hero.source,
    category: CATEGORY_LABELS[hero.category] || hero.category,
    date: hero.notice_date || null,
    title: hero.title.slice(0, 80),
    buyer: hero.buyer || "Buyer not stated",
    val: hero.value_amount && hero.value_amount > 0
      ? (hero.value_amount >= 1_000_000
          ? `£${(hero.value_amount / 1_000_000).toFixed(1)}m`
          : `£${Math.round(hero.value_amount / 1000)}k`)
      : "Value not stated",
    url: hero.source_url || null,
    status: hero.status || "unknown",
    caveat: "Source: public procurement record. Confidence varies by notice quality — buyer names taken verbatim, not verified."
  } : null;
  res.json({ count24h, hero: heroOut, ticker });
}));

function briefingResultHtml(title: string, sub: string, ok: boolean): string {
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${escapeHtml(title)} &mdash; GovRevenue</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Newsreader:ital,opsz,wght@0,6..72,400;0,6..72,500;1,6..72,400&family=Libre+Franklin:wght@400;500;600;700&family=Spline+Sans+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root{--base:#ECE7DA;--surface:#FBF9F3;--border:rgba(27,30,25,.16);--text:#1B1E19;--muted:#86897E;--brand:#B4924E;--green:#1d6b4f}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--base);color:var(--text);font-family:"Libre Franklin",system-ui,sans-serif;display:flex;min-height:100vh;align-items:center;justify-content:center;padding:24px;-webkit-font-smoothing:antialiased}
.card{max-width:460px;width:100%;background:var(--surface);border:1px solid var(--border);padding:40px;text-align:center}
.badge{font-family:"Spline Sans Mono",monospace;font-size:11px;letter-spacing:.18em;text-transform:uppercase;color:${ok ? "var(--green)" : "var(--brand)"};margin-bottom:18px}
h1{font-family:"Newsreader",Georgia,serif;font-size:26px;font-weight:400;letter-spacing:-.01em;color:var(--text);margin-bottom:12px}
p{color:var(--muted);font-size:15px;line-height:1.6;margin-bottom:26px}
.act{display:inline-flex;gap:8px;align-items:center;background:#102A1E;color:#ECE6D6;font-weight:600;font-size:14px;text-decoration:none;padding:12px 22px;transition:background .15s}
.act:hover{background:#0A1C12}
</style></head>
<body><div class="card">
<div class="badge">${ok ? "Briefing &middot; subscribed" : "Briefing"}</div>
<h1>${escapeHtml(title)}</h1>
<p>${escapeHtml(sub)}</p>
<a class="act" href="/">Back to GovRevenue</a>
</div></body></html>`;
}

app.post("/form-submit", asyncRoute(async (req, res) => {
  // Newsletter / briefing sign-up forms (desk page, /charts, articles) post here with
  // a hidden _type=briefing. Capture the email instead of running the scan intake schema.
  if (req.body?._type === "briefing") {
    const email = String(req.body?.email || "").trim().toLowerCase();
    if (!email || !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) {
      res.status(400).type("html").send(briefingResultHtml("That email doesn’t look right", "Head back and try again with a valid work email.", false));
      return;
    }
    const source = (String(req.body?._source || "").trim().toLowerCase().replace(/[^a-z0-9_-]/g, "") || "newsletter").slice(0, 32);
    let alreadySubscribed = false;
    if (pool) {
      const r = await pool.query(
        `INSERT INTO briefing_subscribers (id, email, category, source, created_at)
         VALUES ($1, $2, NULL, $3, NOW()) ON CONFLICT (email) DO NOTHING`,
        [makeId(), email, source]
      );
      alreadySubscribed = (r.rowCount ?? 0) === 0;
    } else {
      alreadySubscribed = briefMemStore.has(email);
      if (!alreadySubscribed) briefMemStore.set(email, { id: makeId(), email, category: null, source, created_at: nowIso() });
    }
    res.type("html").send(briefingResultHtml(
      alreadySubscribed ? "You’re already on the list" : "You’re on the list",
      "We’ll send the weekly spend signal when new public money moves in your sector. No noise.",
      true
    ));
    return;
  }

  const parsed = intakeSchema.safeParse(req.body);

  if (!parsed.success) {
    const issues = parsed.error.issues.map(i => escapeHtml(i.message)).join(", ");
    res.status(400).type("html").send(`<!doctype html><html lang="en"><body style="font-family:'Libre Franklin',system-ui,sans-serif;background:#ECE7DA;color:#1B1E19;padding:40px;-webkit-font-smoothing:antialiased"><div style="max-width:600px;margin:auto;background:#FBF9F3;border:1px solid rgba(27,30,25,.16);padding:32px"><h1 style="font-family:'Newsreader',Georgia,serif;font-size:24px;font-weight:400;margin-top:0;color:#1B1E19">Submission error</h1><p style="color:#9b2d20;margin-bottom:20px;font-size:14px">${issues}</p><p><a href="/scan" style="color:#B4924E;font-weight:600">&larr; Back to the form</a></p></div></body></html>`);
    return;
  }

  const scan = await createScan(parsed.data);
  const authUser = getAuthUser(req);
  if (authUser && pool) {
    await pool.query(`UPDATE scans SET user_id=$2 WHERE id=$1`, [scan.id, authUser.userId]);
  }
  await enqueueScan(scan.id, parsed.data);

  res.redirect(302, `/scan/${scan.id}`);
}));

app.post("/api/scans", asyncRoute(async (req, res) => {
  const parsed = intakeSchema.safeParse(req.body);

  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.flatten() });
    return;
  }

  const scan = await createScan(parsed.data);
  await enqueueScan(scan.id, parsed.data);

  res.status(202).json({
    id: scan.id,
    status: scan.status,
    message: "Scan queued. Poll GET /api/scans/:id"
  });
}));

app.get("/api/scans/:id", asyncRoute(async (req, res) => {
  const scan = await getScan(req.params.id);
  if (!scan) {
    res.status(404).json({ error: "Scan not found" });
    return;
  }
  res.json(scan);
}));

app.delete("/api/scans/:id", requireAdmin, asyncRoute(async (req, res) => {
  await deleteScan(req.params.id);
  res.json({ ok: true, deleted: req.params.id });
}));

app.post("/admin/signals/rebuild", requireAdmin, asyncRoute(async (_req, res) => {
  if (pool) await pool.query("TRUNCATE TABLE homepage_signals");
  refreshHomepageSignals().catch(err => console.error("[signals] rebuild failed", err));
  res.json({ ok: true, message: "Signals table cleared. Rebuild started in background." });
}));

app.post("/admin/desks/rebuild", requireAdmin, asyncRoute(async (_req, res) => {
  if (pool) await pool.query("TRUNCATE TABLE desk_cache");
  deskCacheMemStore.clear();
  const liveDesks = DESK_PROFILES.filter(d => d.live);
  for (const profile of liveDesks) {
    compileDeskInBackground(profile).catch(err => console.error(`[desk] rebuild failed for ${profile.slug}`, err));
  }
  res.json({ ok: true, message: `Cache cleared. Rebuilding ${liveDesks.length} desks in background.` });
}));

app.post("/admin/scans/:id/delete", requireAdmin, asyncRoute(async (req, res) => {
  await deleteScan(req.params.id);
  const token = String(req.query.token || "");
  res.redirect(`/admin/scans?token=${encodeURIComponent(token)}`);
}));

app.post("/admin/scans/bulk-delete", requireAdmin, asyncRoute(async (req, res) => {
  const token = String(req.query.token || req.body?.token || "");
  const ids: string[] = Array.isArray(req.body?.ids) ? req.body.ids : [];
  await Promise.all(ids.map(id => deleteScan(id).catch(() => {})));
  res.redirect(`/admin/scans?token=${encodeURIComponent(token)}`);
}));

app.post("/admin/scans/bulk-rerun", requireAdmin, asyncRoute(async (req, res) => {
  const token = String(req.query.token || req.body?.token || "");
  const ids: string[] = Array.isArray(req.body?.ids) ? req.body.ids : [];
  const results: string[] = [];
  for (const id of ids) {
    const existing = await getScan(id).catch(() => null);
    if (!existing?.input_json) continue;
    const parsed = intakeSchema.safeParse(existing.input_json);
    if (!parsed.success) continue;
    const newScan = await createScan(parsed.data);
    await enqueueScan(newScan.id, parsed.data);
    results.push(newScan.id);
  }
  res.redirect(`/admin/scans?token=${encodeURIComponent(token)}&reran=${results.length}`);
}));


app.get("/api/scans/:id/relevance.json", asyncRoute(async (req, res) => {
  const scan = await getScan(req.params.id);

  if (!scan || !scan.procurement_json) {
    res.status(404).json({ error: "Relevance data not found or not ready yet." });
    return;
  }

  res.json(buildTrustLayer(scan.input_json, scan.procurement_json));
}));

app.get("/api/scans/:id/data.json", asyncRoute(async (req, res) => {
  const scan = await getScan(req.params.id);
  if (!scan || !scan.procurement_json) {
    res.status(404).json({ error: "Procurement data not found or not ready yet." });
    return;
  }
  res.json({ input: scan.input_json ?? null, ...scan.procurement_json });
}));

// ── Auth routes ──────────────────────────────────────────────────────────────

app.get("/register", (req, res) => {
  const user = getAuthUser(req);
  if (user) { res.redirect("/account"); return; }
  const err = req.query.err ? escapeHtml(String(req.query.err)) : "";
  const next = req.query.next ? String(req.query.next) : "";
  const nextParam = next ? `?next=${encodeURIComponent(next)}` : "";
  res.type("html").send(`<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Create account — GovRevenue</title><style>${authCss}</style></head>
<body>
<nav class="auth-nav"><div class="auth-nav-brand"><div class="auth-nav-dot"></div><a href="/" class="auth-nav-logo">Gov<b>Revenue</b></a></div><a href="/login${nextParam}">Sign in</a></nav>
<div class="auth-wrap"><div class="auth-card">
<div class="auth-card-label">New account</div>
<h1>Create your account</h1>
<p class="sub">Get scan history, capability statements and buyer outreach tools.</p>
${err ? `<div class="err">${err}</div>` : ""}
<form method="POST" action="/register${nextParam}">
<div class="field"><label>Email address</label><input type="email" name="email" required autocomplete="email" placeholder="you@company.com"></div>
<div class="field"><label>Password</label><input type="password" name="password" required autocomplete="new-password" placeholder="At least 8 characters" minlength="8"></div>
<button class="btn-primary" type="submit">Create account</button>
</form>
<p class="auth-alt">Already have an account? <a href="/login${nextParam}">Sign in</a></p>
</div></div></body></html>`);
});

app.post("/register", asyncRoute(async (req, res) => {
  const email = String(req.body?.email || "").toLowerCase().trim();
  const password = String(req.body?.password || "");
  if (!email || !email.includes("@")) { res.redirect("/register?err=Invalid+email+address"); return; }
  if (password.length < 8) { res.redirect("/register?err=Password+must+be+at+least+8+characters"); return; }
  const existing = await getUserByEmail(email);
  if (existing) { res.redirect("/register?err=An+account+with+that+email+already+exists"); return; }
  const user = await createUser(email, password);
  const token = signToken(user);
  res.cookie("gr_token", token, { httpOnly: true, sameSite: "lax", secure: process.env.NODE_ENV === "production", maxAge: 30 * 24 * 60 * 60 * 1000 });
  const nextUrl = String(req.query.next || "");
  res.redirect(nextUrl.startsWith("/") ? nextUrl : "/account?welcome=1");
}));

app.get("/login", (req, res) => {
  const user = getAuthUser(req);
  if (user) { res.redirect("/account"); return; }
  const err = req.query.err ? escapeHtml(String(req.query.err)) : "";
  const next = req.query.next ? encodeURIComponent(String(req.query.next)) : "";
  res.type("html").send(`<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Sign in — GovRevenue</title><style>${authCss}</style></head>
<body>
<nav class="auth-nav"><div class="auth-nav-brand"><div class="auth-nav-dot"></div><a href="/" class="auth-nav-logo">Gov<b>Revenue</b></a></div><a href="/register">Create account</a></nav>
<div class="auth-wrap"><div class="auth-card">
<div class="auth-card-label">Sign in</div>
<h1>Welcome back</h1>
<p class="sub">Access your scan history and intelligence tools.</p>
${err ? `<div class="err">${err}</div>` : ""}
<form method="POST" action="/login${next ? `?next=${next}` : ""}">
<div class="field"><label>Email address</label><input type="email" name="email" required autocomplete="email"></div>
<div class="field"><label>Password</label><input type="password" name="password" required autocomplete="current-password"></div>
<button class="btn-primary" type="submit">Sign in</button>
</form>
<p class="auth-alt">No account? <a href="/register">Create one free</a></p>
</div></div></body></html>`);
});

app.post("/login", asyncRoute(async (req, res) => {
  const email = String(req.body?.email || "").toLowerCase().trim();
  const password = String(req.body?.password || "");
  const nextUrl = String(req.query.next || "/account");
  const user = await getUserByEmail(email);
  if (!user || !(await bcrypt.compare(password, user.password_hash))) {
    res.redirect("/login?err=Incorrect+email+or+password"); return;
  }
  const token = signToken(user);
  res.cookie("gr_token", token, { httpOnly: true, sameSite: "lax", secure: process.env.NODE_ENV === "production", maxAge: 30 * 24 * 60 * 60 * 1000 });
  res.redirect(nextUrl.startsWith("/") ? nextUrl : "/account");
}));

app.post("/logout", (req, res) => {
  res.clearCookie("gr_token");
  res.redirect("/");
});

// ── Account / dashboard ──────────────────────────────────────────────────────

app.get("/account", requireAuth, asyncRoute(async (req, res) => {
  const auth = getAuthUser(req)!;
  const user = await getUserById(auth.userId);
  if (!user) { res.clearCookie("gr_token"); res.redirect("/login"); return; }

  const welcome = req.query.welcome === "1";
  const upgraded = req.query.upgraded === "1";

  let userScans: any[] = [];
  let upcomingDeadlines: any[] = [];
  let hotSectors: any[] = [];
  let recentSignals: any[] = [];
  let totalOpenSignals = 0;
  let totalOpenValue = 0;
  let signalsThisWeek = 0;

  if (pool) {
    const [scansRes, deadlinesRes, hotRes, signalsRes, countRes, statsRes] = await Promise.all([
      pool.query(
        `SELECT id, created_at, status, company_name, input_json, progress_stage,
          CASE WHEN status='completed' THEN SUBSTRING(report_markdown, 1, 5000) ELSE NULL END AS report_snippet
         FROM scans WHERE user_id=$1 ORDER BY created_at DESC LIMIT 50`,
        [user.id]
      ),
      pool.query(
        `SELECT id, category, title, buyer, source_url, deadline_date, value_amount
         FROM homepage_signals
         WHERE deadline_date BETWEEN NOW() AND NOW() + INTERVAL '21 days'
           AND (LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')
         ORDER BY deadline_date ASC LIMIT 12`
      ).catch(() => ({ rows: [] as any[] })),
      pool.query(
        `SELECT category, COUNT(*) AS cnt FROM homepage_signals
         WHERE LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%'
         GROUP BY category ORDER BY cnt DESC LIMIT 8`
      ).catch(() => ({ rows: [] as any[] })),
      pool.query(
        `SELECT id, category, title, buyer, source, source_url, value_amount, status, deadline_date
         FROM homepage_signals WHERE LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%'
         ORDER BY fetched_at DESC LIMIT 12`
      ).catch(() => ({ rows: [] as any[] })),
      pool.query<{ n: string }>(
        `SELECT COUNT(*) AS n FROM homepage_signals WHERE LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%'`
      ).catch(() => ({ rows: [{ n: "0" }] })),
      pool.query<{ total_value: string; new_this_week: string }>(
        `SELECT
          COALESCE(SUM(value_amount) FILTER (WHERE value_amount > 0 AND (LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')), 0) AS total_value,
          COUNT(*) FILTER (WHERE notice_date > NOW() - INTERVAL '7 days') AS new_this_week
         FROM homepage_signals`
      ).catch(() => ({ rows: [{ total_value: "0", new_this_week: "0" }] }))
    ]);
    userScans = scansRes.rows;
    upcomingDeadlines = deadlinesRes.rows;
    hotSectors = hotRes.rows;
    recentSignals = signalsRes.rows;
    totalOpenSignals = parseInt(countRes.rows[0]?.n || "0", 10);
    totalOpenValue = parseFloat(statsRes.rows[0]?.total_value || "0");
    signalsThisWeek = parseInt(statsRes.rows[0]?.new_this_week || "0", 10);
  }

  const completedCount = userScans.filter((s: any) => s.status === "completed").length;
  const memberSince = new Date(user.created_at).toLocaleDateString("en-GB", { month: "short", year: "numeric" });
  const tierLabel: Record<UserTier, string> = { free: "Free", pro: "Pro", agency: "Agency" };
  const isPaid = user.tier !== "free";

  // ── Helpers ───────────────────────────────────────────────────────────────
  const signalTagClass = (cat: string): string => {
    if (["social-care", "health", "pharmacy"].some(k => cat.includes(k))) return "bw-tag-health";
    if (["housing", "planning"].some(k => cat.includes(k))) return "bw-tag-housing";
    if (["education", "leisure", "arts"].some(k => cat.includes(k))) return "bw-tag-edu";
    return "bw-tag-other";
  };
  const gradeColor = (g: string): string => {
    const g1 = g.trim().toUpperCase();
    if (g1.startsWith("A")) return "#1d6b4f";
    if (g1.startsWith("B")) return "#1d4ed8";
    if (g1.startsWith("C")) return "#b45309";
    if (g1.startsWith("D") || g1.startsWith("E")) return "#9b2d20";
    return "var(--muted)";
  };
  const verdictColor = (v: string): string => {
    const vl = v.toLowerCase();
    if (vl.includes("strong") || vl.includes("excellent")) return "#1d6b4f";
    if (vl.includes("possible") || vl.includes("moderate") || vl.includes("good")) return "#1d4ed8";
    if (vl.includes("weak") || vl.includes("limited")) return "#b45309";
    if (vl.includes("not viable") || vl.includes("poor") || vl.includes("avoid")) return "#9b2d20";
    return "var(--muted)";
  };
  const deadlineDaysLeft = (d: string): number =>
    Math.ceil((new Date(d).getTime() - Date.now()) / 86400000);
  const deadlineLabel = (d: string): string => {
    const days = deadlineDaysLeft(d);
    if (days <= 0) return "Closes today";
    if (days === 1) return "1 day left";
    if (days <= 7) return `${days} days left`;
    if (days <= 14) return `${days} days`;
    return new Date(d).toLocaleDateString("en-GB", { day: "2-digit", month: "short" });
  };
  const dlUrgencyClass = (d: string): string => {
    const days = deadlineDaysLeft(d);
    if (days <= 7) return "deadline-urgent";
    if (days <= 14) return "deadline-soon";
    return "deadline-ok";
  };

  // ── Scan table rows ───────────────────────────────────────────────────────
  const completedScans = userScans.filter((s: any) => s.status === "completed");
  const scanRows = userScans.length ? userScans.map((s: any) => {
    const isCompleted = s.status === "completed";
    const sectors: string[] = s.input_json?.sectors ?? [];
    const edp = isCompleted && s.report_snippet
      ? parseEdpFromMarkdown(String(s.report_snippet))
      : null;
    const sectorTag = sectors[0]
      ? `<span class="bw-tag bw-tag-other" style="margin-left:6px;vertical-align:middle">${escapeHtml(sectors[0])}</span>`
      : "";
    const verdictCell = edp?.verdict
      ? `<span style="font-size:11px;font-weight:600;color:${verdictColor(edp.verdict)}">${escapeHtml(edp.verdict.length > 22 ? edp.verdict.slice(0, 20) + "…" : edp.verdict)}</span>`
      : `<span style="font-family:var(--mono);font-size:10px;color:var(--slate)">${isCompleted ? "&#8212;" : "Pending"}</span>`;
    const gradeCell = edp?.evidenceGrade
      ? `<span style="font-family:var(--mono);font-size:12px;font-weight:700;color:${gradeColor(edp.evidenceGrade)}">${escapeHtml(edp.evidenceGrade)}</span>`
      : `<span style="font-family:var(--mono);font-size:10px;color:var(--slate)">&#8212;</span>`;
    const actionLink = (path: string, label: string): string => {
      if (!isCompleted) return `<span style="font-family:var(--mono);font-size:10px;color:var(--slate)">&#8212;</span>`;
      if (!isPaid) return `<a href="/pricing" title="Upgrade" style="font-family:var(--mono);font-size:10px;color:var(--slate);text-decoration:none;opacity:.7">&#128274;</a>`;
      return `<a href="/scan/${escapeHtml(s.id)}/${path}" style="font-family:var(--mono);font-size:10px;letter-spacing:.04em;color:var(--accent);text-decoration:none;font-weight:600">${label}</a>`;
    };
    return `<tr>
      <td style="font-weight:600;max-width:180px">
        <a href="/scan/${escapeHtml(s.id)}" style="color:var(--ink);text-decoration:none;font-size:13px">${escapeHtml(s.company_name)}</a>${sectorTag}
      </td>
      <td style="font-family:var(--mono);font-size:10px;color:var(--slate);white-space:nowrap">${new Date(s.created_at).toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" })}</td>
      <td>${verdictCell}</td>
      <td style="text-align:center">${gradeCell}</td>
      <td>${isCompleted ? `<a href="/scan/${escapeHtml(s.id)}" class="dash-btn" style="white-space:nowrap">View &rarr;</a>` : `<span class="scan-badge scan-badge-${escapeHtml(s.status)}">${escapeHtml(s.status)}</span>`}</td>
      <td>${actionLink("capability-statement", "Cap")}</td>
      <td>${actionLink("outreach-emails", "Email")}</td>
      <td>${actionLink("frameworks", "FWK")}</td>
    </tr>`;
  }).join("") : `<tr><td colspan="8" class="dash-empty">No scans yet &#8212; <a href="/scan" style="color:var(--accent);text-decoration:underline">run your first intelligence scan</a></td></tr>`;

  // ── Deadline urgency cards ────────────────────────────────────────────────
  const deadlineCards = upcomingDeadlines.length
    ? upcomingDeadlines.slice(0, 8).map((d: any) => {
        const uc = d.deadline_date ? dlUrgencyClass(String(d.deadline_date)) : "deadline-ok";
        const label = d.deadline_date ? deadlineLabel(String(d.deadline_date)) : "";
        const deskLabel = DESK_PROFILES.find(p => p.slug === d.category)?.label || d.category;
        const valNum = d.value_amount != null ? Number(d.value_amount) : null;
        return `<div class="dl-card ${uc}">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:6px;margin-bottom:6px">
            <span class="bw-tag ${signalTagClass(String(d.category || ""))}">${escapeHtml(deskLabel)}</span>
            <span class="dl-countdown ${uc}">${escapeHtml(label)}</span>
          </div>
          ${d.source_url
            ? `<a href="${escapeHtml(String(d.source_url))}" target="_blank" rel="noopener" style="font-size:12.5px;font-weight:600;color:var(--ink);text-decoration:none;line-height:1.35;display:block;margin-bottom:4px">${escapeHtml(String(d.title || "").slice(0, 90))}</a>`
            : `<div style="font-size:12.5px;font-weight:600;line-height:1.35;margin-bottom:4px">${escapeHtml(String(d.title || "").slice(0, 90))}</div>`}
          <div style="display:flex;gap:8px;flex-wrap:wrap">
            ${d.buyer ? `<span style="font-family:var(--mono);font-size:9.5px;color:var(--slate)">${escapeHtml(String(d.buyer).slice(0, 45))}</span>` : ""}
            ${valNum ? `<span style="font-family:var(--mono);font-size:9.5px;color:var(--accent);font-weight:600">${fmtMoney(valNum)}</span>` : ""}
          </div>
        </div>`;
      }).join("")
    : "";

  // ── Live signals list ─────────────────────────────────────────────────────
  const signalItems = recentSignals.length
    ? recentSignals.map((sig: any) => {
        const deskLabel = DESK_PROFILES.find(d => d.slug === sig.category)?.label || sig.category;
        const tagCls = signalTagClass(String(sig.category || ""));
        const titleStr = String(sig.title || "");
        const buyerStr = sig.buyer ? String(sig.buyer) : null;
        const valNum = sig.value_amount != null ? Number(sig.value_amount) : null;
        const url = sig.source_url ? String(sig.source_url) : null;
        const dlDays = sig.deadline_date ? deadlineDaysLeft(String(sig.deadline_date)) : null;
        const dlLbl = sig.deadline_date ? deadlineLabel(String(sig.deadline_date)) : null;
        return `<div style="padding:10px 0;border-bottom:1px solid var(--line-strong)">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:6px;margin-bottom:3px">
            <span class="bw-tag ${tagCls}">${escapeHtml(deskLabel)}</span>
            ${dlLbl && dlDays !== null && dlDays <= 21 ? `<span style="font-family:var(--mono);font-size:9px;font-weight:700;color:${dlDays <= 7 ? "#9b2d20" : "#b45309"}">${escapeHtml(dlLbl)}</span>` : ""}
          </div>
          ${url
            ? `<a href="${escapeHtml(url)}" target="_blank" rel="noopener" style="font-size:12.5px;font-weight:600;color:var(--ink);text-decoration:none;line-height:1.35;display:block;margin-bottom:3px">${escapeHtml(titleStr.length > 75 ? titleStr.slice(0, 72) + "…" : titleStr)}</a>`
            : `<span style="font-size:12.5px;font-weight:600;color:var(--ink);display:block;line-height:1.35;margin-bottom:3px">${escapeHtml(titleStr.length > 75 ? titleStr.slice(0, 72) + "…" : titleStr)}</span>`}
          <div style="display:flex;gap:10px">
            ${buyerStr ? `<span style="font-family:var(--mono);font-size:9.5px;color:var(--slate)">${escapeHtml(buyerStr.length > 35 ? buyerStr.slice(0, 32) + "…" : buyerStr)}</span>` : ""}
            ${valNum ? `<span style="font-family:var(--mono);font-size:9.5px;color:var(--accent);font-weight:600">${fmtMoney(valNum)}</span>` : ""}
          </div>
        </div>`;
      }).join("")
    : `<div style="font-family:var(--mono);font-size:12px;color:var(--slate);padding:24px 0;text-align:center">Loading&hellip;</div>`;

  // ── Hot sectors ───────────────────────────────────────────────────────────
  const maxSectorCnt = hotSectors.length > 0 ? Number(hotSectors[0].cnt) : 1;
  const hotSectorRows = hotSectors.length
    ? hotSectors.map((hs: any) => {
        const cnt = Number(hs.cnt);
        const pct = Math.round((cnt / maxSectorCnt) * 100);
        const deskLabel = DESK_PROFILES.find(d => d.slug === hs.category)?.label || String(hs.category);
        return `<div style="margin-bottom:10px">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:3px">
            <a href="/desk/${escapeHtml(String(hs.category))}" style="font-size:12px;font-weight:600;color:var(--ink);text-decoration:none">${escapeHtml(deskLabel)}</a>
            <span style="font-family:var(--mono);font-size:10px;color:var(--slate)">${cnt}</span>
          </div>
          <div style="height:4px;background:var(--paper-2);border-radius:2px;overflow:hidden">
            <div style="height:100%;width:${pct}%;background:var(--accent);border-radius:2px"></div>
          </div>
        </div>`;
      }).join("")
    : `<div style="font-family:var(--mono);font-size:12px;color:var(--slate)">Loading&hellip;</div>`;

  // ── Compare link (2+ completed scans) ────────────────────────────────────
  const compareLink = completedScans.length >= 2
    ? `<a href="/scan/${escapeHtml(completedScans[1].id)}/compare?with=${encodeURIComponent(completedScans[0].id)}" class="dash-btn" style="font-size:9px">Compare latest 2 &rarr;</a>`
    : "";

  const navLinks = DESK_PROFILES.map(d => `<a href="/desk/${d.slug}">${escapeHtml(d.label)}</a>`).join("");

  // ── CSS ───────────────────────────────────────────────────────────────────
  const dashCss = `
.pg-mast{border-bottom:1px solid var(--border-2)}
.pg-mast h1{color:var(--text)}
.pg-crumb,.pg-crumb a{color:var(--muted)}
.pg-crumb-active{color:var(--text-mid)}
.pg-stat-val{color:var(--text);font-family:var(--serif)!important}
.pg-stat-label{color:var(--muted)}
.pg-stats{border:1px solid var(--border-2)}
.pg-stat{border-right:1px solid var(--border)}
.mkt-strip{background:var(--surface-2);color:var(--text-mid);padding:12px 56px;display:flex;gap:40px;align-items:center;flex-wrap:wrap;border-bottom:1px solid var(--border)}
.mkt-stat{display:flex;align-items:baseline;gap:8px;flex-shrink:0}
.mkt-val{font-family:var(--serif);font-size:22px;font-weight:500;color:var(--text)}
.mkt-lbl{font-family:var(--mono);font-size:9px;letter-spacing:.09em;text-transform:uppercase;color:var(--muted)}
.dash-grid{display:grid;grid-template-columns:1fr 340px;gap:40px;align-items:start}
.dash-card{background:var(--surface);border:1px solid var(--border-2);margin-bottom:20px;transition:border-color .2s,box-shadow .2s}
.dash-card:hover{border-color:var(--border-3);box-shadow:0 4px 20px rgba(27,30,25,.07)}
.dash-card-head{padding:13px 18px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap}
.dash-card-title{font-family:var(--mono);font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:var(--muted)}
.dash-card-body{padding:18px}
.scan-table{width:100%;border-collapse:collapse;font-size:13px}
.scan-table th{font-family:var(--mono);font-size:9px;letter-spacing:.09em;text-transform:uppercase;color:var(--muted);padding:10px 8px 10px 0;border-bottom:1px solid var(--border);text-align:left;white-space:nowrap}
.scan-table td{padding:11px 8px 11px 0;border-bottom:1px solid var(--border);vertical-align:middle;color:var(--text-mid)}
.scan-table tr:last-child td{border-bottom:none}
.scan-badge{display:inline-block;padding:2px 7px;font-family:var(--mono);font-size:9px;font-weight:600;letter-spacing:.07em;text-transform:uppercase}
.scan-badge-completed{background:rgba(29,107,79,.08);color:#1d6b4f;border:1px solid rgba(29,107,79,.22)}
.scan-badge-pending,.scan-badge-running{background:rgba(180,83,9,.08);color:#b45309;border:1px solid rgba(180,83,9,.22)}
.scan-badge-failed{background:rgba(155,45,32,.06);color:#9b2d20;border:1px solid rgba(155,45,32,.20)}
.dash-btn{display:inline-block;padding:5px 12px;border:1px solid var(--border-2);font-family:var(--mono);font-size:9.5px;letter-spacing:.07em;text-transform:uppercase;color:var(--text-mid);text-decoration:none;background:var(--surface-2);cursor:pointer;transition:.15s}
.dash-btn:hover{background:var(--surface-3);border-color:var(--border-3)}
.dash-empty{padding:32px 0;font-family:var(--mono);font-size:12px;color:var(--muted);text-align:center}
.tier-pill{display:inline-block;padding:2px 8px;font-family:var(--mono);font-size:9px;font-weight:600;letter-spacing:.1em;text-transform:uppercase}
.tier-free{background:var(--surface-2);color:var(--muted);border:1px solid var(--border-2)}
.tier-pro{background:rgba(180,146,78,.1);color:#8B6B2A;border:1px solid rgba(180,146,78,.30)}
.tier-agency{background:rgba(29,107,79,.08);color:#1d6b4f;border:1px solid rgba(29,107,79,.22)}
.flash-ok{background:rgba(29,107,79,.07);border:1px solid rgba(29,107,79,.22);color:#1d6b4f;padding:12px 16px;margin-bottom:24px;font-family:var(--mono);font-size:11px;letter-spacing:.04em}
.tools-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.tool-card{display:block;padding:13px;border:1px solid var(--border-2);text-decoration:none;transition:.18s;background:var(--surface-2)}
.tool-card:hover{background:var(--surface-3);border-color:var(--border-3)}
.tool-card-title{font-family:var(--mono);font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:var(--text);font-weight:600;display:block;margin-bottom:4px}
.tool-card-desc{font-size:11px;color:var(--muted);display:block;line-height:1.4}
.upgrade-box{background:var(--surface-2);border:1px solid var(--border-2);color:var(--text);padding:24px;margin-bottom:24px}
.upgrade-box h3{font-family:var(--serif);font-size:20px;font-weight:400;margin-bottom:8px;color:var(--text)}
.upgrade-box p{font-size:12px;color:var(--muted);margin-bottom:14px;line-height:1.55}
.upgrade-box ul{list-style:none;margin-bottom:18px}
.upgrade-box li{font-size:12px;color:var(--text-mid);padding:4px 0 4px 16px;position:relative}
.upgrade-box li::before{content:"\\2713";position:absolute;left:0;color:var(--brand)}
.btn-upgrade{display:inline-block;background:#102A1E;color:#F3EFE6;padding:10px 20px;font-family:var(--sans);font-size:13px;font-weight:600;text-decoration:none}
.btn-upgrade:hover{background:#0A1C12}
.acct-meta{margin-bottom:14px}
.acct-meta-label{font-family:var(--mono);font-size:10px;letter-spacing:.06em;text-transform:uppercase;color:var(--muted);margin-bottom:3px}
.acct-meta-val{font-size:13px;font-weight:600;color:var(--text)}
.dl-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.dl-card{padding:12px;border-left:3px solid}
.deadline-urgent{background:rgba(155,45,32,.06);border-color:#9b2d20}
.deadline-soon{background:rgba(180,83,9,.06);border-color:#b45309}
.deadline-ok{background:var(--surface);border:1px solid var(--border-2);border-left:3px solid var(--border-2)}
.dl-countdown{font-family:var(--mono);font-size:9.5px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;white-space:nowrap;flex-shrink:0}
.deadline-urgent .dl-countdown{color:#9b2d20}
.deadline-soon .dl-countdown{color:#b45309}
.deadline-ok .dl-countdown{color:var(--muted)}
.sector-shortcuts{display:flex;flex-wrap:wrap;gap:6px}
.sector-btn{display:inline-block;padding:5px 11px;border:1px solid var(--border-2);font-family:var(--mono);font-size:9.5px;letter-spacing:.07em;text-transform:uppercase;color:var(--text-mid);text-decoration:none;transition:.15s;background:var(--surface-2)}
.sector-btn:hover{background:var(--surface-3);border-color:var(--border-3);color:var(--text)}
@media(max-width:900px){.dash-grid{grid-template-columns:1fr}.mkt-strip{padding-left:16px;padding-right:16px}}
@media(max-width:760px){.scan-table th:nth-child(n+4),.scan-table td:nth-child(n+4){display:none}.dl-grid{grid-template-columns:1fr}}
`;

  res.type("html").send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Intelligence Dashboard &#8212; GovRevenue</title>
<style>${pageShellCss()}${dashCss}</style>
</head>
<body>
<header class="gh">
  <div class="gh-inner">
    <div class="gh-top">
      <div class="gh-brand">
        <a href="/" class="gh-logo">Gov<b>Revenue</b></a>
        <span class="gh-tag">Public-sector revenue intelligence</span>
      </div>
      <div style="display:flex;align-items:center;gap:16px;flex-shrink:0">
        <span style="font-family:var(--mono);font-size:10px;color:#7a909e">${escapeHtml(user.email)}</span>
        <span class="tier-pill tier-${escapeHtml(user.tier)}">${escapeHtml(tierLabel[user.tier])}</span>
        <a href="/scan" style="font-family:var(--mono);font-size:10px;letter-spacing:.09em;text-transform:uppercase;color:#9aabb7;text-decoration:none;padding:5px 11px;border:1px solid rgba(255,255,255,.15);border-radius:2px">+ New scan</a>
        <form method="POST" action="/logout" style="display:inline"><button type="submit" style="background:none;border:none;color:#9aabb7;cursor:pointer;font-family:var(--mono);font-size:10px;letter-spacing:.09em;text-transform:uppercase;padding:0">Sign out</button></form>
      </div>
    </div>
    <nav class="gh-nav">${navLinks}</nav>
  </div>
</header>

<div class="mkt-strip">
  <div class="mkt-stat"><span class="mkt-val">${totalOpenSignals > 0 ? totalOpenSignals.toLocaleString("en-GB") : "&#8212;"}</span><span class="mkt-lbl">Open signals</span></div>
  ${totalOpenValue > 0 ? `<div class="mkt-stat"><span class="mkt-val">${fmtMoney(totalOpenValue)}</span><span class="mkt-lbl">Total open contract value</span></div>` : ""}
  <div class="mkt-stat"><span class="mkt-val">${signalsThisWeek}</span><span class="mkt-lbl">New this week</span></div>
  ${upcomingDeadlines.length > 0 ? `<div class="mkt-stat"><span class="mkt-val" style="color:#e87979">${upcomingDeadlines.length}</span><span class="mkt-lbl">Deadlines &lt;21 days</span></div>` : ""}
  <div style="margin-left:auto"><a href="/desks" style="font-family:var(--mono);font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#9aabb7;text-decoration:none">Browse all desks &rarr;</a></div>
</div>

<div class="pg-mast">
  <div class="pg-mast-inner">
    <div class="pg-crumb"><a href="/">GovRevenue</a><span class="pg-crumb-sep">&rsaquo;</span><span class="pg-crumb-active">Dashboard</span></div>
    <h1>Intelligence Dashboard</h1>
    <div class="pg-stats">
      <div class="pg-stat"><span class="pg-stat-val">${userScans.length}</span><span class="pg-stat-label">Scans run</span></div>
      <div class="pg-stat"><span class="pg-stat-val">${completedCount}</span><span class="pg-stat-label">Reports complete</span></div>
      <div class="pg-stat"><span class="pg-stat-val">${upcomingDeadlines.length > 0 ? upcomingDeadlines.length : "&#8212;"}</span><span class="pg-stat-label">Live deadlines</span></div>
      <div class="pg-stat"><span class="pg-stat-val">${memberSince}</span><span class="pg-stat-label">Member since</span></div>
    </div>
  </div>
</div>

<div class="pg-body">
<div class="pg-body-inner">
${welcome ? `<div class="flash-ok">Account created &#8212; welcome to GovRevenue. Run your first intelligence scan to get started.</div>` : ""}
${upgraded ? `<div class="flash-ok">Subscription active. Full intelligence suite unlocked.</div>` : ""}

<div class="dash-grid">

  <!-- ── MAIN COLUMN ──────────────────────────────────────────────── -->
  <div>

    ${upcomingDeadlines.length > 0 ? `
    <div class="dash-card">
      <div class="dash-card-head">
        <span class="dash-card-title" style="color:#9b2d20">&#9888;&nbsp; Upcoming deadlines</span>
        <span style="font-family:var(--mono);font-size:10px;color:#9b2d20;font-weight:700">${upcomingDeadlines.length} closing within 21 days</span>
      </div>
      <div class="dash-card-body"><div class="dl-grid">${deadlineCards}</div></div>
    </div>
    ` : ""}

    <div class="dash-card">
      <div class="dash-card-head">
        <span class="dash-card-title">Intelligence scan history</span>
        <div style="display:flex;gap:8px;align-items:center">${compareLink}<a href="/scan" class="dash-btn">+ New scan</a></div>
      </div>
      <div style="padding:0 20px;overflow-x:auto">
        <table class="scan-table" style="min-width:620px">
          <thead><tr>
            <th>Company / Sector</th><th>Date</th><th>Verdict</th><th style="text-align:center">Grade</th>
            <th>Report</th><th>Cap</th><th>Email</th><th>FWK</th>
          </tr></thead>
          <tbody>${scanRows}</tbody>
        </table>
      </div>
    </div>

    <div class="dash-card">
      <div class="dash-card-head">
        <span class="dash-card-title">Intelligence tools</span>
        ${!isPaid ? `<a href="/pricing" class="dash-btn" style="color:var(--accent);border-color:var(--accent)">Unlock with Pro &rarr;</a>` : ""}
      </div>
      <div class="dash-card-body">
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px">
          <div style="padding:16px;border:1px solid var(--line-strong);border-radius:2px${!isPaid ? ";opacity:.7" : ""}">
            <div style="font-family:var(--mono);font-size:9.5px;letter-spacing:.08em;text-transform:uppercase;font-weight:700;margin-bottom:6px">Capability Statement</div>
            <div style="font-size:12px;color:var(--slate);line-height:1.5">LLM-generated 2-page statement tailored to your profile and sector. PQQ and framework-ready.</div>
            ${!isPaid ? `<div style="margin-top:8px"><a href="/pricing" style="font-family:var(--mono);font-size:9px;color:var(--accent);text-decoration:none;font-weight:700;text-transform:uppercase">Unlock &rarr;</a></div>` : ""}
          </div>
          <div style="padding:16px;border:1px solid var(--line-strong);border-radius:2px${!isPaid ? ";opacity:.7" : ""}">
            <div style="font-family:var(--mono);font-size:9.5px;letter-spacing:.08em;text-transform:uppercase;font-weight:700;margin-bottom:6px">Buyer Outreach Emails</div>
            <div style="font-size:12px;color:var(--slate);line-height:1.5">3 personalised emails to top-ranked buyers from your scan. Introduce your firm, request pre-market meetings.</div>
            ${!isPaid ? `<div style="margin-top:8px"><a href="/pricing" style="font-family:var(--mono);font-size:9px;color:var(--accent);text-decoration:none;font-weight:700;text-transform:uppercase">Unlock &rarr;</a></div>` : ""}
          </div>
          <div style="padding:16px;border:1px solid var(--line-strong);border-radius:2px${!isPaid ? ";opacity:.7" : ""}">
            <div style="font-family:var(--mono);font-size:9.5px;letter-spacing:.08em;text-transform:uppercase;font-weight:700;margin-bottom:6px">Framework Pre-Qualification</div>
            <div style="font-size:12px;color:var(--slate);line-height:1.5">Identifies open frameworks, assesses your eligibility, produces a prioritised application checklist.</div>
            ${!isPaid ? `<div style="margin-top:8px"><a href="/pricing" style="font-family:var(--mono);font-size:9px;color:var(--accent);text-decoration:none;font-weight:700;text-transform:uppercase">Unlock &rarr;</a></div>` : ""}
          </div>
        </div>
        ${completedScans.length >= 2 ? `
        <div style="margin-top:10px;padding:14px;background:var(--paper-2);border:1px solid var(--line-strong);border-radius:2px;display:flex;align-items:center;justify-content:space-between;gap:12px">
          <div>
            <div style="font-family:var(--mono);font-size:9.5px;letter-spacing:.08em;text-transform:uppercase;font-weight:700;margin-bottom:3px">Scan Comparison</div>
            <div style="font-size:12px;color:var(--slate)">Track how verdict, grade and buyer landscape have shifted between your scans.</div>
          </div>
          ${compareLink}
        </div>` : ""}
      </div>
    </div>

    <div class="dash-card">
      <div class="dash-card-head"><span class="dash-card-title">Launch a targeted sector scan</span></div>
      <div class="dash-card-body">
        <div style="font-size:12px;color:var(--slate);margin-bottom:14px;line-height:1.5">Click any sector to pre-fill the scan form with that desk&rsquo;s context &#8212; your profile against their live procurement data.</div>
        <div class="sector-shortcuts">
          ${DESK_PROFILES.map(d => `<a href="/scan?desk=${escapeHtml(d.slug)}" class="sector-btn">${escapeHtml(d.label)}</a>`).join("")}
        </div>
      </div>
    </div>

  </div>

  <!-- ── SIDEBAR ──────────────────────────────────────────────────── -->
  <div>

    <div class="dash-card">
      <div class="dash-card-head"><span class="dash-card-title">Account</span></div>
      <div class="dash-card-body">
        <div class="acct-meta"><div class="acct-meta-label">Email</div><div class="acct-meta-val" style="word-break:break-all;font-size:12px">${escapeHtml(user.email)}</div></div>
        <div class="acct-meta">
          <div class="acct-meta-label">Plan</div>
          <div style="display:flex;align-items:center;gap:10px;margin-top:4px">
            <span class="tier-pill tier-${escapeHtml(user.tier)}">${escapeHtml(tierLabel[user.tier])}</span>
            <span style="font-size:12px;color:var(--slate)">${user.tier === "free" ? "Free" : user.tier === "pro" ? "&pound;79/month" : "&pound;249/month"}</span>
          </div>
        </div>
        <div class="acct-meta"><div class="acct-meta-label">Member since</div><div class="acct-meta-val">${memberSince}</div></div>
        ${user.tier !== "free" && user.stripe_customer_id ? `
        <form method="POST" action="/billing/portal" style="margin-top:8px">
          <button type="submit" class="dash-btn" style="width:100%;text-align:center">Manage billing &rarr;</button>
        </form>` : ""}
      </div>
    </div>

    ${user.tier === "free" && stripe ? `
    <div class="upgrade-box">
      <h3>Unlock the full suite</h3>
      <p>Free accounts run scans and read reports. Pro turns every scan into action with three intelligence tools.</p>
      <ul>
        <li>Capability statement generator</li>
        <li>Buyer outreach email kit (3 per scan)</li>
        <li>Framework pre-qualification checker</li>
        <li>Unlimited scan history</li>
        <li>Weekly opportunity alert emails</li>
        <li>Evidence grade tracking</li>
      </ul>
      <a href="/billing/checkout?plan=pro" class="btn-upgrade">Upgrade to Pro &mdash; &pound;79/month</a>
      <div style="margin-top:10px"><a href="/billing/checkout?plan=agency" style="font-family:var(--mono);font-size:9px;color:#7a909e;text-decoration:underline;letter-spacing:.05em">Agency plan &pound;249/month (unlimited users) &rarr;</a></div>
    </div>
    ` : ""}

    <div class="dash-card">
      <div class="dash-card-head">
        <span class="dash-card-title">Hot sectors right now</span>
        <a href="/desks" style="font-family:var(--mono);font-size:9.5px;color:var(--accent);text-decoration:none;text-transform:uppercase;letter-spacing:.06em">All 24 &rarr;</a>
      </div>
      <div class="dash-card-body">${hotSectorRows}</div>
    </div>

    <div class="dash-card">
      <div class="dash-card-head">
        <span class="dash-card-title">Live open signals</span>
        ${totalOpenSignals > 0 ? `<span style="font-family:var(--mono);font-size:10px;color:var(--accent);font-weight:600">${totalOpenSignals.toLocaleString("en-GB")}</span>` : ""}
      </div>
      <div style="padding:0 20px">
        ${signalItems}
        <div style="padding:14px 0 6px"><a href="/desks" class="dash-btn">Browse all desks &rarr;</a></div>
      </div>
    </div>

    <div class="dash-card">
      <div class="dash-card-head"><span class="dash-card-title">Quick access</span></div>
      <div class="dash-card-body">
        <div class="tools-grid">
          <a href="/scan" class="tool-card"><span class="tool-card-title">New scan</span><span class="tool-card-desc">Run an intelligence scan for any company</span></a>
          <a href="/desks" class="tool-card"><span class="tool-card-title">Sector desks</span><span class="tool-card-desc">24 live desks with procurement signals</span></a>
          <a href="/pricing" class="tool-card"><span class="tool-card-title">Pricing</span><span class="tool-card-desc">Free, Pro &amp; Agency plans compared</span></a>
          <a href="/" class="tool-card"><span class="tool-card-title">Home</span><span class="tool-card-desc">Homepage with live market signals</span></a>
        </div>
      </div>
    </div>

  </div>
</div>
</div>
</div>
${pageShellFoot()}
</body>
</html>`);
}));

// ── Stripe billing ────────────────────────────────────────────────────────────

app.get("/billing/checkout", requireAuth, asyncRoute(async (req, res) => {
  if (!stripe) { res.redirect("/pricing"); return; }
  const auth = getAuthUser(req)!;
  const plan = String(req.query.plan || "pro");
  const priceId = plan === "agency" ? STRIPE_AGENCY_PRICE_ID : STRIPE_PRO_PRICE_ID;
  if (!priceId) { res.redirect("/pricing?err=Stripe+not+configured"); return; }
  const session = await stripe.checkout.sessions.create({
    mode: "subscription",
    payment_method_types: ["card"],
    customer_email: auth.email,
    line_items: [{ price: priceId, quantity: 1 }],
    success_url: `${BASE_URL}/account?upgraded=1`,
    cancel_url: `${BASE_URL}/pricing`,
    metadata: { userId: auth.userId, plan },
  });
  res.redirect(session.url!);
}));

app.post("/billing/portal", requireAuth, asyncRoute(async (req, res) => {
  if (!stripe) { res.redirect("/account"); return; }
  const auth = getAuthUser(req)!;
  const user = await getUserById(auth.userId);
  if (!user?.stripe_customer_id) { res.redirect("/account"); return; }
  const session = await stripe.billingPortal.sessions.create({
    customer: user.stripe_customer_id,
    return_url: `${BASE_URL}/account`,
  });
  res.redirect(session.url);
}));

// Raw body needed for Stripe webhook signature verification
app.post("/billing/webhook", express.raw({ type: "application/json" }), asyncRoute(async (req, res) => {
  if (!stripe || !STRIPE_WEBHOOK_SECRET) { res.json({ received: true }); return; }
  let event: Stripe.Event;
  try {
    event = stripe.webhooks.constructEvent(req.body, req.headers["stripe-signature"] as string, STRIPE_WEBHOOK_SECRET);
  } catch {
    res.status(400).json({ error: "Webhook signature failed" }); return;
  }
  if (event.type === "checkout.session.completed") {
    const session = event.data.object as Stripe.Checkout.Session;
    const userId = session.metadata?.userId;
    const plan = (session.metadata?.plan || "pro") as UserTier;
    if (userId) await updateUserTier(userId, plan, session.customer as string, session.subscription as string, "active");
  } else if (event.type === "customer.subscription.deleted" || event.type === "customer.subscription.updated") {
    const sub = event.data.object as Stripe.Subscription;
    if (sub.status !== "active" && sub.status !== "trialing") {
      if (pool) {
        const r = await pool.query<UserRecord>(`SELECT * FROM users WHERE stripe_subscription_id=$1`, [sub.id]);
        if (r.rows[0]) await updateUserTier(r.rows[0].id, "free", undefined, undefined, sub.status);
      }
    }
  }
  res.json({ received: true });
}));

// ── Action layer: capability statement ───────────────────────────────────────

app.get("/scan/:id/capability-statement", asyncRoute(async (req, res) => {
  const regen = req.query.regen === "1";
  if (regen && pool) await pool.query(`UPDATE scans SET capability_statement=NULL WHERE id=$1`, [req.params.id]);
  const scan = await getScan(req.params.id);
  if (!scan || scan.status !== "completed") { res.redirect(`/scan/${req.params.id}`); return; }

  // Serve cached if available
  if (scan.capability_statement && !regen) {
    res.type("html").send(scan.capability_statement); return;
  }

  const data = scan.procurement_json as ProcurementData | null;
  const edp = scan.report_markdown ? parseEdpFromMarkdown(scan.report_markdown) : null;
  const topBuyers = (data?.contractsFinder?.awarded || [])
    .filter(n => n.buyer && n.buyer !== "Not stated" && !isAggregatorBuyer(n.buyer))
    .sort((a, b) => (b.awardedValue ?? 0) - (a.awardedValue ?? 0))
    .slice(0, 5).map(n => n.buyer).filter(Boolean).join(", ");

  const prompt = `You are a UK public sector bid consultant writing a one-page capability statement for an SME.

Company: ${escapeHtml(scan.company_name)}
Services: ${escapeHtml(String(scan.input_json?.mainServices || ""))}
Target buyers: ${escapeHtml(String(scan.input_json?.idealBuyers || ""))}
Recommended route: ${escapeHtml(edp?.recommendedRoute || "")}
Top active buyers in their sector: ${escapeHtml(topBuyers)}

Write a professional, specific capability statement with these sections:
1. About [Company Name] (2-3 sentences, what they do and for whom)
2. Our Services (4-6 bullet points, specific to public sector)
3. Why We Win (3 differentiators framed as buyer value)
4. Recent Relevant Experience (2-3 placeholder examples in the right format, marked as "[TO FILL]")
5. Accreditations & Standards (relevant placeholders)
6. Contact (placeholder)

Write in British English. Be specific, not generic. Frame everything from the buyer's perspective.
Return clean text/markdown only. No JSON.`;

  let capStatement = "";
  try {
    capStatement = await callLlmReport(prompt);
  } catch (err: any) {
    capStatement = `Error generating capability statement: ${err?.message}`;
  }

  const html = `<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Capability Statement — ${escapeHtml(scan.company_name)}</title>
<style>
${authCss}
body{display:block;background:var(--cream)}
.cs-wrap{max-width:800px;margin:0 auto;padding:40px 24px 80px}
.cs-topbar{display:flex;align-items:center;justify-content:space-between;margin-bottom:32px;flex-wrap:wrap;gap:12px}
.cs-topbar a{color:var(--gold);font-size:13px;font-weight:600;text-decoration:none}
.cs-card{background:var(--paper);border:1px solid var(--line);border-radius:8px;padding:36px 40px}
.cs-card h1{font-family:var(--serif);font-size:28px;margin-bottom:4px}
.cs-card .eyebrow{font-size:12px;letter-spacing:.1em;text-transform:uppercase;color:#6f5b50;margin-bottom:28px}
.cs-body h2{font-family:var(--serif);font-size:19px;margin-top:28px;margin-bottom:10px;color:var(--ink)}
.cs-body p{font-size:15px;line-height:1.7;margin-bottom:14px;color:#2a2218}
.cs-body ul{margin:0 0 14px 20px;padding:0}
.cs-body li{font-size:15px;line-height:1.7;margin-bottom:6px;color:#2a2218}
.cs-actions{display:flex;gap:12px;margin-top:28px;flex-wrap:wrap}
.btn-print{background:var(--ink);color:#fff;padding:10px 20px;border-radius:6px;font-size:13px;font-weight:700;text-decoration:none;cursor:pointer;border:none}
.btn-ghost{background:transparent;color:var(--ink);padding:10px 20px;border-radius:6px;font-size:13px;font-weight:700;text-decoration:none;border:1px solid var(--line)}
@media print{.cs-topbar,.cs-actions{display:none}.cs-card{border:none;padding:0}}
</style>
</head><body>
<nav class="auth-nav"><div class="auth-nav-brand"><div class="auth-nav-dot"></div><a href="/" class="auth-nav-logo">Gov<b>Revenue</b></a></div><a href="/scan/${escapeHtml(scan.id)}">← Back to scan</a></nav>
<div class="cs-wrap">
<div class="cs-topbar">
  <div><div style="font-size:20px;font-weight:700">${escapeHtml(scan.company_name)}</div><div style="font-size:13px;color:#6f5b50;margin-top:4px">Capability Statement</div></div>
  <div style="display:flex;gap:10px"><a class="btn-ghost" href="/scan/${escapeHtml(scan.id)}/outreach-emails">Outreach emails →</a><a class="btn-ghost" href="/scan/${escapeHtml(scan.id)}/frameworks">Framework pre-qual →</a></div>
</div>
<div class="cs-card">
  <div class="cs-body">${markdownToHtml(capStatement)}</div>
  <div class="cs-actions">
    <button class="btn-print" onclick="window.print()">Print / Save PDF</button>
    <a class="btn-ghost" href="/scan/${escapeHtml(scan.id)}/capability-statement?regen=1">Regenerate</a>
  </div>
</div>
</div></body></html>`;

  await updateScanCachedField(scan.id, "capability_statement", html);
  res.type("html").send(html);
}));

// Force regeneration
// ── Action layer: outreach emails ────────────────────────────────────────────

app.get("/scan/:id/outreach-emails", asyncRoute(async (req, res) => {
  const scan = await getScan(req.params.id);
  if (!scan || scan.status !== "completed") { res.redirect(`/scan/${req.params.id}`); return; }

  let regen = req.query.regen === "1";
  if (regen && pool) await pool.query(`UPDATE scans SET outreach_emails=NULL WHERE id=$1`, [scan.id]);

  const fresh = await getScan(req.params.id);
  if (fresh?.outreach_emails && !regen) { res.type("html").send(fresh.outreach_emails); return; }

  const data = scan.procurement_json as ProcurementData | null;
  const edp = scan.report_markdown ? parseEdpFromMarkdown(scan.report_markdown) : null;

  const topBuyersRaw = (data?.contractsFinder?.awarded || [])
    .filter(n => n.buyer && n.buyer !== "Not stated" && !isAggregatorBuyer(n.buyer))
    .sort((a, b) => (b.awardedValue ?? 0) - (a.awardedValue ?? 0))
    .slice(0, 3);

  const buyerContext = topBuyersRaw.map((n, i) =>
    `Buyer ${i+1}: ${n.buyer} — contract: "${n.title}", value: ${formatMoney(n.awardedValue ?? null)}`
  ).join("\n");

  const prompt = `You are a UK public sector business development expert writing cold outreach emails for an SME.

Company: ${scan.company_name}
Services: ${String(scan.input_json?.mainServices || "")}
Recommended route: ${edp?.recommendedRoute || "direct award / framework"}

Top 3 buyers who have recently spent money in this sector:
${buyerContext || "Major public sector buyers in this sector"}

Write 3 separate cold outreach emails, one for each buyer. Each email should:
- Be addressed to "Dear [Procurement Lead]" (acknowledge we don't have the contact name)
- Reference the buyer's actual recent spending/contract activity in their sector
- Be specific about what the company offers and why it fits that buyer's needs
- Include a clear, low-pressure ask (a 15-minute call or to be added to their supplier list)
- Be 150-200 words max
- Sound like it was written by a real person, not a template
- End with: [Your name] | ${scan.company_name}

Format as:
## Email 1: [Buyer Name]
Subject: [Subject line]

[Email body]

---

## Email 2: [Buyer Name]
...

Write in British English. Be specific and warm, not corporate.`;

  let emailsContent = "";
  try {
    emailsContent = await callLlmReport(prompt);
  } catch (err: any) {
    emailsContent = `Error generating outreach emails: ${err?.message}`;
  }

  const html = `<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Outreach Emails — ${escapeHtml(scan.company_name)}</title>
<style>
${authCss}
body{display:block;background:var(--cream)}
.oe-wrap{max-width:800px;margin:0 auto;padding:40px 24px 80px}
.oe-topbar{display:flex;align-items:center;justify-content:space-between;margin-bottom:32px;flex-wrap:wrap;gap:12px}
.oe-card{background:var(--paper);border:1px solid var(--line);border-radius:8px;padding:36px 40px}
.oe-body h2{font-family:"Spectral",Georgia,serif;font-size:19px;margin-top:32px;margin-bottom:4px;color:var(--ink);padding-bottom:8px;border-bottom:1px solid var(--line)}
.oe-body h2:first-child{margin-top:0}
.oe-body p{font-size:15px;line-height:1.7;margin-bottom:12px;color:#2a2218;white-space:pre-wrap}
.oe-body hr{border:none;border-top:1px dashed var(--line);margin:24px 0}
.oe-body strong{font-weight:700}
.oe-actions{display:flex;gap:12px;margin-top:28px;flex-wrap:wrap}
.btn-copy{background:var(--ink);color:#fff;padding:10px 20px;border-radius:6px;font-size:13px;font-weight:700;border:none;cursor:pointer}
.btn-ghost{background:transparent;color:var(--ink);padding:10px 20px;border-radius:6px;font-size:13px;font-weight:700;text-decoration:none;border:1px solid var(--line)}
.note{background:var(--cream);border:1px solid var(--line);border-radius:6px;padding:14px 16px;font-size:13px;color:#6f5b50;margin-bottom:24px}
</style>
</head><body>
<nav class="auth-nav"><div class="auth-nav-brand"><div class="auth-nav-dot"></div><a href="/" class="auth-nav-logo">Gov<b>Revenue</b></a></div><a href="/scan/${escapeHtml(scan.id)}">← Back to scan</a></nav>
<div class="oe-wrap">
<div class="oe-topbar">
  <div><div style="font-size:20px;font-weight:700">${escapeHtml(scan.company_name)}</div><div style="font-size:13px;color:#6f5b50;margin-top:4px">Buyer Outreach Emails</div></div>
  <div style="display:flex;gap:10px"><a class="btn-ghost" href="/scan/${escapeHtml(scan.id)}/capability-statement">Capability statement →</a><a class="btn-ghost" href="/scan/${escapeHtml(scan.id)}/frameworks">Framework pre-qual →</a></div>
</div>
<div class="oe-card">
<div class="note">These emails are drafted based on your scan data. Find the right contact at each buyer on LinkedIn or their procurement portal before sending. Edit freely — they're a starting point.</div>
<div class="oe-body">${markdownToHtml(emailsContent)}</div>
<div class="oe-actions">
  <a class="btn-ghost" href="/scan/${escapeHtml(scan.id)}/outreach-emails?regen=1">Regenerate</a>
</div>
</div>
</div></body></html>`;

  await updateScanCachedField(scan.id, "outreach_emails", html);
  res.type("html").send(html);
}));

// ── Action layer: framework pre-qualification ─────────────────────────────────

app.get("/scan/:id/frameworks", asyncRoute(async (req, res) => {
  const scan = await getScan(req.params.id);
  if (!scan || scan.status !== "completed") { res.redirect(`/scan/${req.params.id}`); return; }

  let regen = req.query.regen === "1";
  if (regen && pool) await pool.query(`UPDATE scans SET frameworks_assessment=NULL WHERE id=$1`, [scan.id]);

  const fresh = await getScan(req.params.id);
  if (fresh?.frameworks_assessment && !regen) { res.type("html").send(fresh.frameworks_assessment); return; }

  const edp = scan.report_markdown ? parseEdpFromMarkdown(scan.report_markdown) : null;
  const sector = resolveSectorFromScan(scan).label;

  const prompt = `You are a UK public sector procurement specialist helping an SME identify relevant frameworks.

Company: ${scan.company_name}
Services: ${String(scan.input_json?.mainServices || "")}
Sector: ${sector}
Recommended route: ${edp?.recommendedRoute || ""}
Target buyers: ${String(scan.input_json?.idealBuyers || "")}

Identify 5-8 real, currently open UK public sector frameworks or Dynamic Purchasing Systems (DPS) that this company should apply to join.

For each framework, provide:
- Framework name and managing body (e.g. Crown Commercial Service, ESPO, YPO, Scape, etc.)
- Reference number if known (e.g. RM6187)
- What it covers
- Who can use it (council, NHS, etc.)
- Typical contract values
- Eligibility requirements (turnover, accreditations, experience)
- Whether the company likely qualifies based on their profile
- Next steps to apply

Format as a structured table first (Name | Body | Covers | Open to | Min turnover | Likely qualify), then detailed notes for each.

Focus on frameworks that are currently accepting new suppliers or have regular refresh windows.
Be specific — name real frameworks, not placeholder ones. Write in British English.`;

  let frameworksContent = "";
  try {
    frameworksContent = await callLlmReport(prompt);
  } catch (err: any) {
    frameworksContent = `Error generating framework assessment: ${err?.message}`;
  }

  const html = `<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Framework Pre-Qualification — ${escapeHtml(scan.company_name)}</title>
<style>
${authCss}
body{display:block;background:var(--cream)}
.fw-wrap{max-width:900px;margin:0 auto;padding:40px 24px 80px}
.fw-topbar{display:flex;align-items:center;justify-content:space-between;margin-bottom:32px;flex-wrap:wrap;gap:12px}
.fw-card{background:var(--paper);border:1px solid var(--line);border-radius:8px;padding:36px 40px}
.fw-body h2{font-family:"Spectral",Georgia,serif;font-size:19px;margin-top:32px;margin-bottom:10px;color:var(--ink)}
.fw-body h2:first-child{margin-top:0}
.fw-body p{font-size:15px;line-height:1.7;margin-bottom:12px;color:#2a2218}
.fw-body ul{margin:0 0 14px 20px}
.fw-body li{font-size:15px;line-height:1.7;margin-bottom:6px}
.fw-body table{width:100%;border-collapse:collapse;margin-bottom:28px;font-size:14px}
.fw-body th{text-align:left;padding:8px;background:var(--cream);border:1px solid var(--line);font-weight:700;font-size:12px;text-transform:uppercase;letter-spacing:.04em}
.fw-body td{padding:8px;border:1px solid var(--line);vertical-align:top}
.fw-body hr{border:none;border-top:1px dashed var(--line);margin:24px 0}
.fw-actions{display:flex;gap:12px;margin-top:28px;flex-wrap:wrap}
.btn-ghost{background:transparent;color:var(--ink);padding:10px 20px;border-radius:6px;font-size:13px;font-weight:700;text-decoration:none;border:1px solid var(--line)}
.note{background:#fef9ec;border:1px solid #e8d18a;border-radius:6px;padding:14px 16px;font-size:13px;color:#7a5c00;margin-bottom:24px}
</style>
</head><body>
<nav class="auth-nav"><div class="auth-nav-brand"><div class="auth-nav-dot"></div><a href="/" class="auth-nav-logo">Gov<b>Revenue</b></a></div><a href="/scan/${escapeHtml(scan.id)}">← Back to scan</a></nav>
<div class="fw-wrap">
<div class="fw-topbar">
  <div><div style="font-size:20px;font-weight:700">${escapeHtml(scan.company_name)}</div><div style="font-size:13px;color:#6f5b50;margin-top:4px">Framework Pre-Qualification</div></div>
  <div style="display:flex;gap:10px"><a class="btn-ghost" href="/scan/${escapeHtml(scan.id)}/capability-statement">Capability statement →</a><a class="btn-ghost" href="/scan/${escapeHtml(scan.id)}/outreach-emails">Outreach emails →</a></div>
</div>
<div class="fw-card">
<div class="note">Framework windows open and close — verify each one directly with the managing body before applying. Reference numbers let you find them on Contracts Finder or the managing body's portal.</div>
<div class="fw-body">${markdownToHtml(frameworksContent)}</div>
<div class="fw-actions">
  <a class="btn-ghost" href="/scan/${escapeHtml(scan.id)}/frameworks?regen=1">Regenerate</a>
</div>
</div>
</div></body></html>`;

  await updateScanCachedField(scan.id, "frameworks_assessment", html);
  res.type("html").send(html);
}));

app.get("/pricing", (_req, res) => {
  res.type("html").send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Pricing — GovRevenue</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Newsreader:ital,opsz,wght@0,6..72,400;0,6..72,500;1,6..72,400&family=Libre+Franklin:wght@400;500;600;700&family=Spline+Sans+Mono:wght@400;500;600&display=swap');
:root{
  --base:#ECE7DA;--surface:#FBF9F3;--surface-2:#F6F2E8;
  --brand:#B4924E;--text:#1B1E19;--text-mid:#3A3E36;--muted:#86897E;--faint:#9AA093;
  --border:rgba(27,30,25,.10);--border-2:rgba(27,30,25,.16);--border-3:rgba(27,30,25,.22);
  --sans:"Libre Franklin",system-ui,sans-serif;
  --serif:"Newsreader",Georgia,serif;
  --mono:"Spline Sans Mono",ui-monospace,monospace;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--base);color:var(--text);font-family:var(--sans);font-size:16px;line-height:1.55;-webkit-font-smoothing:antialiased}
a{color:inherit;text-decoration:none}
.wrap{padding:0 32px;max-width:960px;margin:0 auto}
header{background:rgba(236,231,218,0.92);backdrop-filter:blur(10px);border-bottom:1px solid var(--border-2);padding:0 32px;height:60px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:50}
.logo{display:flex;align-items:center;gap:9px;font-family:var(--serif);font-size:20px;font-weight:500;color:var(--text)}
.logo-dot{width:9px;height:9px;background:var(--brand);border-radius:50%}
.logo b{color:var(--brand)}
.back{font-family:var(--mono);font-size:12px;letter-spacing:.08em;color:var(--muted)}
.back:hover{color:var(--text-mid)}
.hero{padding:64px 0 48px;text-align:center}
.eyebrow{font-family:var(--mono);font-size:11px;letter-spacing:.18em;text-transform:uppercase;color:var(--brand);margin-bottom:14px}
h1{font-family:var(--serif);font-size:clamp(32px,4vw,44px);font-weight:400;letter-spacing:-.02em;line-height:1.1;margin-bottom:16px;color:var(--text)}
.sub{font-size:17px;color:var(--muted);max-width:36em;margin:0 auto}
.plans{display:grid;grid-template-columns:repeat(3,1fr);gap:20px;padding:0 0 80px}
.plan{border:1px solid var(--border-2);padding:36px 32px;background:var(--surface);position:relative;transition:border-color .2s,box-shadow .2s;display:flex;flex-direction:column}
.plan:hover{border-color:var(--border-3);box-shadow:0 6px 24px rgba(27,30,25,.08)}
.plan.featured{border-color:rgba(180,146,78,.4);box-shadow:0 0 0 1px rgba(180,146,78,.2)}
.plan.featured:hover{border-color:rgba(180,146,78,.6)}
.plan-badge{position:absolute;top:-1px;left:50%;transform:translateX(-50%);background:var(--brand);color:#10110D;font-family:var(--mono);font-size:10px;letter-spacing:.1em;text-transform:uppercase;padding:5px 16px;white-space:nowrap;font-weight:600}
.plan-name{font-family:var(--mono);font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--muted);margin-bottom:10px}
.plan-price{font-family:var(--serif);font-size:42px;font-weight:400;letter-spacing:-.02em;line-height:1;margin-bottom:6px;color:var(--text)}
.plan-price sup{font-size:22px;vertical-align:top;margin-top:6px}
.plan-period{font-family:var(--mono);font-size:12px;color:var(--muted);margin-bottom:24px}
.plan-desc{font-size:14px;color:var(--muted);line-height:1.6;margin-bottom:24px;padding-bottom:24px;border-bottom:1px solid var(--border)}
.plan ul{list-style:none;margin-bottom:28px;flex:1}
.plan .btn{margin-top:auto}
.plan li{font-size:14px;color:var(--text-mid);padding:7px 0;border-bottom:1px solid var(--border);display:flex;align-items:baseline;gap:8px}
.plan li:last-child{border-bottom:none}
.tick{color:var(--brand);font-size:12px;flex-shrink:0}
.dash{color:var(--faint);font-size:12px;flex-shrink:0}
.btn{display:block;text-align:center;font-family:var(--sans);font-size:14px;font-weight:600;padding:14px 20px;transition:.18s;letter-spacing:.01em}
.btn-primary{background:#102A1E;color:#F3EFE6;border:1px solid #102A1E}
.btn-primary:hover{background:#0A1C12}
.btn-outline{border:1px solid var(--border-3);color:var(--text-mid)}
.btn-outline:hover{background:var(--surface-2);border-color:var(--brand)}
.faq{padding:0 0 80px}
.faq h2{font-family:var(--serif);font-size:26px;font-weight:400;margin-bottom:32px;color:var(--text)}
.faq-item{border-top:1px solid var(--border);padding:20px 0}
.faq-item:last-child{border-bottom:1px solid var(--border)}
.faq-q{font-weight:600;font-size:15px;margin-bottom:8px;color:var(--text)}
.faq-a{font-size:14px;color:var(--muted);line-height:1.7}
.caveat{padding:32px 0;border-top:1px solid var(--border);text-align:center;font-family:var(--mono);font-size:11px;letter-spacing:.06em;color:var(--faint)}
@media(max-width:760px){.plans{grid-template-columns:1fr}.hero{padding:48px 0 40px}h1{font-size:28px}}
</style>
</head>
<body>
<header>
  <a href="/" class="logo"><span class="logo-dot"></span>Gov<b>Revenue</b></a>
  <a href="/" class="back">&larr; Back to home</a>
</header>
<main>
<div class="wrap">
  <div class="hero">
    <div class="eyebrow">Pricing</div>
    <h1>One scan or a standing desk.</h1>
    <p class="sub">Pay per scan or subscribe for continuous intelligence across every desk that matters to your firm.</p>
  </div>
  <div class="plans">
    <div class="plan">
      <div class="plan-name">Pay as you go</div>
      <div class="plan-price"><sup>£</sup>29</div>
      <div class="plan-period">per scan</div>
      <div class="plan-desc">A single full scan. Get the 10-section report, buyer watchlist, and PDF — no subscription needed.</div>
      <ul>
        <li><span class="tick">&#10003;</span> Full 10-section intelligence report</li>
        <li><span class="tick">&#10003;</span> Buyer watchlist &amp; route-to-revenue map</li>
        <li><span class="tick">&#10003;</span> PDF export</li>
        <li><span class="tick">&#10003;</span> Evidence grade &amp; verdict</li>
        <li><span class="dash">–</span> Weekly opportunity alerts</li>
        <li><span class="dash">–</span> All 24 intelligence desks</li>
        <li><span class="dash">–</span> Multiple firm profiles</li>
      </ul>
      <a href="/register" class="btn btn-outline">Get started &rarr;</a>
    </div>
    <div class="plan featured">
      <div class="plan-badge">Most popular</div>
      <div class="plan-name">Pro</div>
      <div class="plan-price"><sup>£</sup>79</div>
      <div class="plan-period">per month &middot; cancel anytime</div>
      <div class="plan-desc">Unlimited scans plus weekly alerts when new contracts land in your categories. Built for firms actively chasing public sector.</div>
      <ul>
        <li><span class="tick">&#10003;</span> Unlimited scans</li>
        <li><span class="tick">&#10003;</span> Weekly opportunity alerts (email)</li>
        <li><span class="tick">&#10003;</span> All 24 intelligence desks</li>
        <li><span class="tick">&#10003;</span> Full reports &amp; PDF exports</li>
        <li><span class="tick">&#10003;</span> Buyer watchlist monitoring</li>
        <li><span class="dash">–</span> Multiple firm profiles</li>
        <li><span class="dash">–</span> Team access</li>
      </ul>
      <a href="/billing/checkout?plan=pro" class="btn btn-primary">Get started &rarr;</a>
    </div>
    <div class="plan">
      <div class="plan-name">Agency</div>
      <div class="plan-price"><sup>£</sup>249</div>
      <div class="plan-period">per month</div>
      <div class="plan-desc">For consultancies and bid writers managing multiple clients. Run scans across different firm profiles and desks.</div>
      <ul>
        <li><span class="tick">&#10003;</span> Everything in Pro</li>
        <li><span class="tick">&#10003;</span> Up to 10 firm profiles</li>
        <li><span class="tick">&#10003;</span> Team access (3 seats)</li>
        <li><span class="tick">&#10003;</span> Client-ready PDF reports</li>
        <li><span class="tick">&#10003;</span> Priority support</li>
        <li><span class="tick">&#10003;</span> Dedicated desk monitoring</li>
        <li><span class="tick">&#10003;</span> Custom alert frequency</li>
      </ul>
      <a href="mailto:hello@govrevenue.co.uk" class="btn btn-outline">Contact us &rarr;</a>
    </div>
  </div>
  <div class="faq">
    <h2>Common questions</h2>
    <div class="faq-item">
      <div class="faq-q">What does a scan actually produce?</div>
      <div class="faq-a">A 10-section commercial intelligence report covering: executive verdict, evidence grade, intelligence dashboard, source-backed evidence, money map (best revenue routes), buyer watchlist, bid readiness score, contracts to avoid, 30-day activation plan, and QA notes. Every claim is sourced to a public record.</div>
    </div>
    <div class="faq-item">
      <div class="faq-q">Where does the data come from?</div>
      <div class="faq-a">Contracts Finder, Find a Tender, and Local Authority transparency data — all UK public record. We do not use insider information or paid databases. Every figure in the report links back to a public notice.</div>
    </div>
    <div class="faq-item">
      <div class="faq-q">How long does a scan take?</div>
      <div class="faq-a">Typically 2–4 minutes. The agent searches both procurement databases, scores route-to-revenue fit, and generates the full report. You get an HTML report immediately and a PDF download link.</div>
    </div>
    <div class="faq-item">
      <div class="faq-q">What are the weekly alerts?</div>
      <div class="faq-a">After your first scan, you can subscribe to alerts. Each week the agent re-checks the public record for new contracts matching your firm's profile and emails you only the new ones — no noise, no re-sending contracts you've already seen.</div>
    </div>
    <div class="faq-item">
      <div class="faq-q">Is this a guarantee I'll win contracts?</div>
      <div class="faq-a">No. GovRevenue is intelligence, not certainty. We surface what the public record shows and give you a structured assessment of fit. Winning still depends on your bid quality, track record, and pricing. We help you stop chasing the wrong ones.</div>
    </div>
  </div>
</div>
</main>
<footer style="border-top:1px solid var(--line-strong);padding:32px">
  <div class="caveat">Public record only &middot; Intelligence, not certainty &middot; <a href="/" style="text-decoration:underline">GovRevenue</a></div>
</footer>
</body>
</html>`);
});

app.get("/scan", (req, res) => {
  if (!getAuthUser(req)) {
    res.redirect(`/register?next=${encodeURIComponent(req.url)}`);
    return;
  }
  const deskParam = typeof req.query.desk === "string" ? req.query.desk.slice(0, 60) : "";
  const noticeIdParam = typeof req.query.noticeId === "string" ? req.query.noticeId.slice(0, 80) : "";
  const servicesParam = typeof req.query.services === "string" ? req.query.services.slice(0, 300) : "";
  const buyerParam = typeof req.query.buyer === "string" ? req.query.buyer.slice(0, 120) : "";
  const deskProfile = deskParam ? DESK_PROFILES.find(d => d.slug === deskParam) : null;

  const contextBannerHtml = deskProfile
    ? `<div style="background:var(--surface-2);border:1px solid var(--border-2);padding:14px 18px;margin-bottom:28px;font-size:13.5px;line-height:1.6">
        <strong style="font-family:var(--mono);font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:var(--brand)">From the ${escapeHtml(deskProfile.label)} desk</strong>
        ${buyerParam ? `<br>Buyer context: <strong>${escapeHtml(buyerParam)}</strong>` : ""}
        ${noticeIdParam ? `<br><span style="font-family:var(--mono);font-size:11px;color:var(--muted)">Notice ref: ${escapeHtml(noticeIdParam)}</span>` : ""}
        <br><span style="font-size:12px;color:var(--muted)">Fill in your firm details below for a personalised fit check against this desk&rsquo;s opportunities.</span>
       </div>`
    : "";

  const mainServicesValue = servicesParam ? escapeHtml(servicesParam) : "";
  const mainServicesPlaceholder = deskProfile
    ? `e.g. ${deskProfile.pinnedProfile.mainServices.split(" ").slice(0, 4).join(", ")}`
    : "e.g. facilities management, reactive maintenance, cleaning";

  res.type("html").send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="description" content="Submit your company profile and get a structured commercial intelligence report against UK public procurement data in minutes.">
<title>Run a Scan &mdash; GovRevenue</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Newsreader:ital,opsz,wght@0,6..72,400;0,6..72,500;1,6..72,400&family=Libre+Franklin:wght@400;500;600;700&family=Spline+Sans+Mono:wght@400;500;600&display=swap');
:root{
  --base:#ECE7DA;--surface:#FBF9F3;--surface-2:#F6F2E8;
  --brand:#B4924E;--text:#1B1E19;--text-mid:#3A3E36;--muted:#86897E;--faint:#9AA093;
  --border:rgba(27,30,25,.12);--border-2:rgba(27,30,25,.18);
  --sans:"Libre Franklin",system-ui,sans-serif;
  --serif:"Newsreader",Georgia,serif;
  --mono:"Spline Sans Mono",ui-monospace,monospace;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--base);color:var(--text);font-family:var(--sans);font-size:16px;line-height:1.55;-webkit-font-smoothing:antialiased}
a{color:inherit;text-decoration:none}
.topstrip{background:#102A1E;color:#ECE6D6;font-family:var(--mono);font-size:11px;letter-spacing:.14em;text-transform:uppercase;padding:0 32px;display:flex;justify-content:space-between;align-items:center;height:34px}
.topstrip a{color:#C5C9BC;opacity:.9}
.topstrip a:hover{opacity:1;color:#ECE6D6}
header{background:rgba(236,231,218,0.92);backdrop-filter:blur(10px);border-bottom:1px solid var(--border-2);padding:0 32px;position:sticky;top:0;z-index:50}
.mast{display:flex;align-items:center;justify-content:space-between;padding:16px 0}
.logo{display:flex;align-items:center;gap:9px;font-family:var(--serif);font-size:20px;font-weight:500;color:var(--text)}
.logo-dot{width:9px;height:9px;background:var(--brand);border-radius:50%}
.logo b{color:var(--brand)}
.back{font-family:var(--mono);font-size:12px;letter-spacing:.1em;color:var(--muted)}
.back:hover{color:var(--text-mid)}
.page{max-width:760px;margin:0 auto;padding:56px 32px 80px}
.page-head{margin-bottom:40px;padding-bottom:28px;border-bottom:1px solid var(--border)}
.eyebrow{font-family:var(--mono);font-size:11px;letter-spacing:.22em;text-transform:uppercase;color:var(--brand);margin-bottom:14px}
h1{font-family:var(--serif);font-size:clamp(28px,3.5vw,38px);font-weight:400;letter-spacing:-.02em;line-height:1.1;margin-bottom:12px;color:var(--text)}
.sub{color:var(--muted);font-size:15px;line-height:1.6}
.form-grid{display:grid;gap:0;background:var(--surface-2);border:1px solid var(--border-2);padding:0 32px}
.field{padding:18px 0;border-bottom:1px solid var(--border)}
.field:last-of-type{border-bottom:0}
.field label{display:block;font-family:var(--mono);font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:#5C6157;margin-bottom:9px}
.field label span{color:var(--brand)}
.field input,.field textarea,.field select{width:100%;border:1px solid var(--border-2);background:var(--surface);padding:13px 15px;font-family:var(--sans);font-size:15px;color:var(--text);transition:.15s;resize:vertical;outline:none}
.field input:focus,.field textarea:focus,.field select:focus{border-color:var(--brand)}
.field textarea{min-height:76px}
.field .hint{font-family:var(--mono);font-size:10.5px;color:var(--muted);margin-top:7px;line-height:1.5}
.section-label{font-family:var(--mono);font-size:10px;letter-spacing:.2em;text-transform:uppercase;color:var(--brand);background:var(--surface-2);padding:12px 0;margin:0;border-top:1px solid var(--border);border-bottom:1px solid var(--border)}
.submit-row{margin-top:36px;display:flex;align-items:center;gap:20px;flex-wrap:wrap}
.btn-submit{background:#102A1E;color:#F3EFE6;font-family:var(--sans);font-size:15px;font-weight:600;letter-spacing:.01em;padding:15px 32px;border:0;cursor:pointer;transition:.18s}
.btn-submit:hover{background:#0A1C12}
.submit-note{font-family:var(--mono);font-size:11px;color:var(--muted);line-height:1.5}
</style>
</head>
<body>
<div class="topstrip">
  <span>Intelligence scan &middot; UK public sector</span>
  <span>Public record &middot; updated continuously</span>
</div>
<header><div class="mast">
  <a class="logo" href="/"><span class="logo-dot"></span>Gov<b>Revenue</b></a>
  <a class="back" href="/">&#8592; Back to home</a>
</div></header>
<main class="page">
  <div class="page-head">
    <div class="eyebrow">Intelligence scan</div>
    <h1>Tell us about your firm.</h1>
    <p class="sub">The more context you give, the sharper the signal. We scan Contracts Finder, Find a Tender and LA spend data, then return a sourced verdict on where the money is and how to reach it.</p>
  </div>
  ${contextBannerHtml}
  <form method="POST" action="/form-submit" autocomplete="off" class="form-grid">
    ${deskParam ? `<input type="hidden" name="_deskContext" value="${escapeHtml(deskParam)}">` : ""}
    ${noticeIdParam ? `<input type="hidden" name="_noticeContext" value="${escapeHtml(noticeIdParam)}">` : ""}

    <div class="section-label">Your firm</div>

    <div class="field">
      <label for="companyName">Company name <span>*</span></label>
      <input id="companyName" name="companyName" required placeholder="e.g. Apex Facilities Ltd">
    </div>
    <div class="field">
      <label for="website">Website</label>
      <input id="website" name="website" placeholder="e.g. https://apexfacilities.co.uk">
    </div>
    <div class="field">
      <label for="location">Location / base</label>
      <input id="location" name="location" placeholder="e.g. Birmingham, West Midlands">
    </div>
    <div class="field">
      <label for="teamSize">Team size</label>
      <input id="teamSize" name="teamSize" placeholder="e.g. 12 FTE">
    </div>

    <div class="section-label">Services &amp; scope</div>

    <div class="field">
      <label for="mainServices">Main services <span>*</span></label>
      <textarea id="mainServices" name="mainServices" required placeholder="${mainServicesPlaceholder}">${mainServicesValue}</textarea>
      <div class="hint">Be specific &mdash; these become the search terms we use against the public record.</div>
    </div>
    <div class="field">
      <label for="secondaryServices">Secondary services</label>
      <textarea id="secondaryServices" name="secondaryServices" placeholder="e.g. grounds maintenance, pest control"></textarea>
    </div>
    <div class="field">
      <label for="areasServed">Areas / regions served</label>
      <textarea id="areasServed" name="areasServed" placeholder="e.g. West Midlands, East Midlands, national frameworks"></textarea>
    </div>
    <div class="field">
      <label for="excludedServices">Services you do NOT want</label>
      <textarea id="excludedServices" name="excludedServices" placeholder="e.g. residential, defence, high-security sites"></textarea>
    </div>

    <div class="section-label">Contract appetite</div>

    <div class="field">
      <label for="idealContractSize">Ideal contract size</label>
      <input id="idealContractSize" name="idealContractSize" placeholder="e.g. £100k &ndash; £500k per year">
    </div>
    <div class="field">
      <label for="maximumContractSize">Maximum contract size</label>
      <input id="maximumContractSize" name="maximumContractSize" placeholder="e.g. £2m">
    </div>
    <div class="field">
      <label for="idealBuyers">Ideal public-sector buyers</label>
      <textarea id="idealBuyers" name="idealBuyers" placeholder="e.g. NHS trusts, local authorities, housing associations"></textarea>
    </div>
    <div class="field">
      <label for="regionsToScan">Regions to scan first</label>
      <textarea id="regionsToScan" name="regionsToScan" placeholder="e.g. West Midlands priority, then national frameworks"></textarea>
    </div>

    <div class="section-label">Track record &amp; credentials</div>

    <div class="field">
      <label for="publicSectorExperience">Public-sector experience</label>
      <input id="publicSectorExperience" name="publicSectorExperience" placeholder="e.g. 3 years, 6 active public contracts">
    </div>
    <div class="field">
      <label for="lastPublicContract">Last public contract won</label>
      <textarea id="lastPublicContract" name="lastPublicContract" placeholder="e.g. 2yr cleaning contract, Birmingham City Council, £180k/yr, ended 2024"></textarea>
      <div class="hint">Most recent win &mdash; buyer name, value, and date if known. Helps us assess your evidence grade.</div>
    </div>
    <div class="field">
      <label for="caseStudies">Case studies or proof</label>
      <textarea id="caseStudies" name="caseStudies" placeholder="e.g. Delivered responsive repairs for housing association 2022&ndash;24, 94% satisfaction score"></textarea>
    </div>
    <div class="field">
      <label for="certifications">Certifications / accreditations</label>
      <textarea id="certifications" name="certifications" placeholder="e.g. ISO 9001, Constructionline Gold, Living Wage employer"></textarea>
    </div>
    <div class="field">
      <label for="frameworkStatus">Framework access</label>
      <textarea id="frameworkStatus" name="frameworkStatus" placeholder="e.g. On Crown Commercial Service RM6187, YPO cleaning framework — or none yet"></textarea>
      <div class="hint">Framework memberships unlock fast-track contract routes. List any you hold or are applying for.</div>
    </div>

    <div class="section-label">Goals &amp; context</div>

    <div class="field">
      <label for="mainGoal">Main business goal</label>
      <textarea id="mainGoal" name="mainGoal" placeholder="e.g. Win first NHS contract within 12 months"></textarea>
    </div>
    <div class="field">
      <label for="preferredOutput">Preferred output</label>
      <textarea id="preferredOutput" name="preferredOutput" placeholder="e.g. Focus on frameworks we can get on now, not long tender processes"></textarea>
      <div class="hint">Tell us what kind of results matter most &mdash; this shapes the report focus.</div>
    </div>
    <div class="field">
      <label for="biggestConcern">Biggest concern</label>
      <textarea id="biggestConcern" name="biggestConcern" placeholder="e.g. We keep losing to incumbents on price"></textarea>
    </div>

    <div class="submit-row">
      <button type="submit" class="btn-submit">Run GovRevenue Scan &rarr;</button>
      <span class="submit-note">Takes 2&ndash;4 minutes &middot; HTML &amp; PDF report</span>
    </div>
  </form>
</main>
<script>
window.addEventListener("pageshow", () => {
  document.querySelectorAll("input:not([type=hidden]), textarea").forEach(f => {
    if (!f.value) f.setAttribute("autocomplete","off");
  });
});
</script>
</body>
</html>`);
});

app.get("/scan/:id", asyncRoute(async (req, res) => {
  const scan = await getScan(req.params.id);

  if (!scan) {
    res.status(404).type("html").send(notFoundHtml("Scan not found", getAuthUser(req)));
    return;
  }

  res.type("html").send(reportPage(scan));
}));

app.get("/signals", asyncRoute(async (req, res) => {
  const catFilter = typeof req.query.cat === "string" ? req.query.cat : null;
  const statusFilter = typeof req.query.status === "string" ? req.query.status : "all";
  const srcFilter = typeof req.query.src === "string" ? req.query.src : null;
  const page = Math.max(1, parseInt(String(req.query.page ?? "1"), 10) || 1);
  const PER_PAGE = 50;
  const offset = (page - 1) * PER_PAGE;

  const fmtDate = (d: string | null) => d ? new Date(d).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" }) : "—";
  const fmtVal = (v: number | null) => !v ? null : v >= 1_000_000 ? `£${(v / 1_000_000).toFixed(1)}m` : v >= 1000 ? `£${Math.round(v / 1000)}k` : `£${v}`;
  const categoryLabel = (slug: string) => DESK_PROFILES.find(d => d.slug === slug)?.label || slug;

  type SigStats = { total: string; open_cnt: string; total_val: string; closing14: string; closing7: string };
  let stats: SigStats = { total: "0", open_cnt: "0", total_val: "0", closing14: "0", closing7: "0" };
  let signals: HomepageSignal[] = [];
  let totalFiltered = 0;

  if (pool) {
    // Stats: deduplicate by (title, buyer, category) — collapses lot-variants of the same notice
    const [statsRow] = (await pool.query<SigStats>(`
      WITH deduped AS (
        SELECT DISTINCT ON (LOWER(title), COALESCE(buyer,''), category)
          id, status, value_amount, deadline_date
        FROM homepage_signals
        ORDER BY LOWER(title), COALESCE(buyer,''), category, deadline_date ASC NULLS LAST, notice_date DESC NULLS LAST
      )
      SELECT
        COUNT(*)::text AS total,
        COUNT(*) FILTER (WHERE LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')::text AS open_cnt,
        COALESCE(SUM(value_amount) FILTER (WHERE value_amount > 0), 0)::text AS total_val,
        COUNT(*) FILTER (WHERE deadline_date IS NOT NULL AND deadline_date BETWEEN NOW() AND NOW() + INTERVAL '14 days')::text AS closing14,
        COUNT(*) FILTER (WHERE deadline_date IS NOT NULL AND deadline_date BETWEEN NOW() AND NOW() + INTERVAL '7 days')::text AS closing7
      FROM deduped
    `)).rows;
    if (statsRow) stats = statsRow;

    const conds: string[] = [];
    const params: (string | number)[] = [];
    if (catFilter) { params.push(catFilter); conds.push(`category = $${params.length}`); }
    if (statusFilter === "open") conds.push(`(LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')`);
    if (srcFilter) { params.push(srcFilter); conds.push(`source = $${params.length}`); }
    const innerWhere = conds.length ? `WHERE ${conds.join(" AND ")}` : "";

    // Deduplicate by (title, buyer, category) keeping the row with the earliest deadline (most urgent)
    const countRow = await pool.query<{ n: string }>(
      `SELECT COUNT(*)::text AS n FROM (
         SELECT DISTINCT ON (LOWER(title), COALESCE(buyer,''), category) id
         FROM homepage_signals ${innerWhere}
         ORDER BY LOWER(title), COALESCE(buyer,''), category, deadline_date ASC NULLS LAST, notice_date DESC NULLS LAST
       ) sub`,
      params
    );
    totalFiltered = parseInt(countRow.rows[0]?.n || "0", 10);

    const pageParams = [...params, PER_PAGE, offset];
    const r = await pool.query<HomepageSignal>(
      `SELECT * FROM (
         SELECT DISTINCT ON (LOWER(title), COALESCE(buyer,''), category) *
         FROM homepage_signals ${innerWhere}
         ORDER BY LOWER(title), COALESCE(buyer,''), category, deadline_date ASC NULLS LAST, notice_date DESC NULLS LAST
       ) deduped
       ORDER BY notice_date DESC NULLS LAST
       LIMIT $${pageParams.length - 1} OFFSET $${pageParams.length}`,
      pageParams
    );
    signals = r.rows;
  } else {
    const seen = new Set<string>();
    const all = [...sigMemStore.values()]
      .filter(s => !catFilter || s.category === catFilter)
      .filter(s => statusFilter !== "open" || /open|active/i.test(s.status || ""))
      .filter(s => !srcFilter || s.source === srcFilter)
      .sort((a, b) => (a.deadline_date || "9999").localeCompare(b.deadline_date || "9999") || (b.notice_date || "").localeCompare(a.notice_date || ""))
      .filter(s => { const key = `${s.title.toLowerCase()}|${s.buyer || ""}|${s.category}`; if (seen.has(key)) return false; seen.add(key); return true; })
      .sort((a, b) => (b.notice_date || "").localeCompare(a.notice_date || ""));
    totalFiltered = all.length;
    signals = all.slice(offset, offset + PER_PAGE);
    const allUniq = new Map<string, HomepageSignal>(); [...sigMemStore.values()].forEach(s => { if (!allUniq.has(s.id)) allUniq.set(s.id, s); });
    stats.total = String(allUniq.size);
    stats.open_cnt = String([...allUniq.values()].filter(s => /open|active/i.test(s.status || "")).length);
  }

  const totalPages = Math.ceil(totalFiltered / PER_PAGE);
  const totalVal = parseFloat(stats.total_val) || 0;
  const fmtBigVal = (v: number) => v >= 1_000_000_000 ? `£${(v / 1_000_000_000).toFixed(1)}bn` : v >= 1_000_000 ? `£${(v / 1_000_000).toFixed(0)}m` : `£${Math.round(v / 1000)}k`;

  const nowMs = Date.now();
  const rows = signals.map(s => {
    const isOpen = /open|active/i.test(s.status || "");
    const val = fmtVal(s.value_amount);
    const isHighVal = (s.value_amount || 0) >= 1_000_000;
    const deadlineMs = s.deadline_date ? new Date(s.deadline_date).getTime() : null;
    const daysLeft = deadlineMs ? Math.ceil((deadlineMs - nowMs) / 86_400_000) : null;
    let urgencyClass = "";
    let urgencyBadge = "";
    if (daysLeft !== null && daysLeft >= 0) {
      if (daysLeft <= 7) { urgencyClass = "row-urgent"; urgencyBadge = `<span class="dl-badge dl-red">${daysLeft}d left</span>`; }
      else if (daysLeft <= 14) { urgencyClass = "row-warn"; urgencyBadge = `<span class="dl-badge dl-amber">${daysLeft}d left</span>`; }
      else { urgencyBadge = `<span class="dl-badge dl-grey">${daysLeft}d</span>`; }
    }
    return `<tr class="${urgencyClass}">
      <td><a href="/desk/${escapeHtml(s.category)}" class="sig-cat">${escapeHtml(categoryLabel(s.category))}</a></td>
      <td class="sig-title"><a href="${escapeHtml(s.source_url)}" target="_blank" rel="noopener">${escapeHtml(s.title)}</a></td>
      <td class="sig-buyer">${escapeHtml(s.buyer || "—")}</td>
      <td class="sig-val ${isHighVal ? "sig-val-high" : ""}">${val ? escapeHtml(val) : '<span class="sig-nil">—</span>'}</td>
      <td><span class="sig-status ${isOpen ? "sig-open" : "sig-awarded"}">${isOpen ? "Open" : "Awarded"}</span></td>
      <td class="sig-date">${fmtDate(s.notice_date)}</td>
      <td class="sig-dl">${urgencyBadge || (s.deadline_date ? fmtDate(s.deadline_date) : '<span class="sig-nil">—</span>')}</td>
      <td class="sig-src"><span class="src-badge src-${escapeHtml(s.source.toLowerCase())}">${escapeHtml(s.source)}</span></td>
    </tr>`;
  }).join("");

  const allCategories = DESK_PROFILES.filter(d => d.live).map(d => d.slug);
  const catOptions = allCategories.map(c =>
    `<option value="${escapeHtml(c)}" ${catFilter === c ? "selected" : ""}>${escapeHtml(categoryLabel(c))}</option>`
  ).join("");

  const qs = (overrides: Record<string, string | number | null>) => {
    const base: Record<string, string> = {};
    if (catFilter) base.cat = catFilter;
    if (statusFilter !== "all") base.status = statusFilter;
    if (srcFilter) base.src = srcFilter;
    Object.entries(overrides).forEach(([k, v]) => { if (v !== null) base[k] = String(v); else delete base[k]; });
    const s = new URLSearchParams(base).toString();
    return s ? `/signals?${s}` : "/signals";
  };

  const pagerLink = (p: number, label: string, disabled = false) =>
    disabled
      ? `<span class="pg-link pg-dis">${label}</span>`
      : `<a href="${qs({ page: p })}" class="pg-link">${label}</a>`;

  const pager = totalPages <= 1 ? "" : `<div class="pager">
    ${pagerLink(page - 1, "← Prev", page <= 1)}
    <span class="pg-info">Page ${page} of ${totalPages} &mdash; ${totalFiltered.toLocaleString()} signals</span>
    ${pagerLink(page + 1, "Next →", page >= totalPages)}
  </div>`;

  const hasFilter = !!(catFilter || statusFilter !== "all" || srcFilter);

  res.type("html").send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Live Signals — GovRevenue</title>
<style>${pageShellCss()}
/* ── signals page ── */
.sig-band{display:grid;grid-template-columns:1fr 280px;gap:20px;align-items:start;margin-bottom:24px}
.sig-stats{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.sc{background:var(--surface);border:1px solid var(--border-2);padding:18px 16px;display:flex;flex-direction:column;gap:4px}
.sc-lbl{font-family:var(--mono);font-size:9px;letter-spacing:.14em;text-transform:uppercase;color:var(--muted)}
.sc-val{font-family:var(--mono);font-size:22px;font-weight:600;letter-spacing:-.02em;color:var(--text);line-height:1;margin:6px 0 2px}
.sc-sub{font-family:var(--mono);font-size:9px;color:var(--faint);line-height:1.35}
.sc.sc-green .sc-val{color:var(--green)}
.sc.sc-brand .sc-val{color:var(--brand)}
.sc.sc-warn .sc-val{color:#b45309}
/* page head */
.page-head{padding:36px 0 28px;display:flex;align-items:flex-end;justify-content:space-between;gap:24px}
.eyebrow{font-family:var(--mono);font-size:10px;letter-spacing:.18em;text-transform:uppercase;color:var(--brand);margin-bottom:8px}
h1.sig-h1{font-family:var(--sans);font-size:28px;font-weight:800;letter-spacing:-.02em;line-height:1.1;margin-bottom:6px;color:var(--text)}
.sub{font-size:13.5px;color:var(--muted)}
/* chart frame */
.chart-frame-wrap{border:1px solid var(--border-2);background:var(--surface)}
/* filters */
.filter-bar{display:flex;gap:8px;align-items:center;flex-wrap:wrap;padding:14px 0;border-top:1px solid var(--border);border-bottom:1px solid var(--border);margin-bottom:0}
.filter-bar select{font-family:var(--mono);font-size:10.5px;letter-spacing:.05em;border:1px solid var(--border-2);background:var(--surface-2);color:var(--text);padding:8px 32px 8px 10px;cursor:pointer;appearance:none;-webkit-appearance:none;background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6'%3E%3Cpath d='M0 0l5 6 5-6z' fill='%238893A4'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 10px center}
.filter-bar select:focus{outline:none;border-color:var(--brand)}
.filter-bar button{font-family:var(--mono);font-size:10.5px;letter-spacing:.1em;text-transform:uppercase;border:none;background:var(--brand);color:#fff;padding:9px 16px;cursor:pointer;border-radius:2px}
.filter-bar button:hover{background:var(--brand-hot)}
.filter-clear{font-family:var(--mono);font-size:10.5px;letter-spacing:.06em;text-transform:uppercase;color:var(--muted);text-decoration:underline;padding:4px 0}
.sig-count{font-family:var(--mono);font-size:10.5px;letter-spacing:.06em;color:var(--muted);margin-left:auto;white-space:nowrap}
/* wrap constraint */
.wrap{max-width:1120px;margin:0 auto;padding:0 32px}
/* table */
.tbl-wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;min-width:780px}
thead{background:var(--surface-2)}
thead th{font-family:var(--mono);font-size:9px;letter-spacing:.14em;text-transform:uppercase;color:var(--muted);padding:10px;border-bottom:1px solid var(--border-2);text-align:left;white-space:nowrap}
table th:nth-child(1){width:110px}
table th:nth-child(6),table th:nth-child(7){padding-left:6px;padding-right:6px}
table td:nth-child(6),table td:nth-child(7){padding-left:6px;padding-right:6px}
tbody tr{border-bottom:1px solid var(--border);transition:background .12s}
tbody tr:hover{background:var(--surface-2)}
tbody tr.row-urgent{background:rgba(239,68,68,.05)}
tbody tr.row-urgent:hover{background:rgba(239,68,68,.09)}
tbody tr.row-warn{background:rgba(245,158,11,.03)}
td{padding:11px 10px;font-size:13px;vertical-align:top}
.sig-cat{font-family:var(--mono);font-size:9px;letter-spacing:.07em;text-transform:uppercase;color:var(--brand);background:rgba(180,146,78,.1);padding:3px 7px;white-space:nowrap;display:inline-block;border-radius:2px}
.sig-cat:hover{background:rgba(180,146,78,.18)}
.sig-title{max-width:320px}
.sig-title a{color:var(--text);font-weight:500;line-height:1.35;display:block}
.sig-title a:hover{color:var(--brand);text-decoration:underline}
.sig-buyer{max-width:180px;font-size:12.5px;color:var(--muted)}
.sig-val{font-family:var(--mono);font-size:12px;font-weight:600;white-space:nowrap;color:var(--text)}
.sig-val.sig-val-high{color:#4ade80}
.sig-nil{color:var(--faint)}
.sig-status{font-family:var(--mono);font-size:9px;letter-spacing:.07em;text-transform:uppercase;padding:3px 8px;white-space:nowrap;display:inline-block;border-radius:2px}
.sig-open{color:#4ade80;background:rgba(34,197,94,.1)}
.sig-awarded{color:var(--muted);background:var(--surface-2)}
.sig-date,.sig-dl{font-family:var(--mono);font-size:10.5px;color:var(--muted);white-space:nowrap}
.dl-badge{font-family:var(--mono);font-size:9px;letter-spacing:.06em;text-transform:uppercase;padding:3px 7px;display:inline-block;font-weight:700;border-radius:2px}
.dl-red{color:#fff;background:#dc2626}
.dl-amber{color:#fff;background:#d97706}
.dl-grey{color:var(--muted);background:var(--surface-2)}
.src-badge{font-family:var(--mono);font-size:9px;letter-spacing:.08em;text-transform:uppercase;padding:3px 7px;border-radius:2px}
.src-cf{color:#93c5fd;background:rgba(59,130,246,.12)}
.src-fts{color:#c4b5fd;background:rgba(167,139,250,.12)}
/* pager */
.pager{display:flex;align-items:center;gap:14px;padding:24px 0 48px}
.pg-link{font-family:var(--mono);font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:var(--text);border:1px solid var(--border-2);padding:8px 16px}
.pg-link:hover{background:var(--brand);color:#fff;border-color:var(--brand)}
.pg-dis{font-family:var(--mono);font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:var(--faint);border:1px solid var(--border);padding:8px 16px;pointer-events:none}
.pg-info{font-family:var(--mono);font-size:10.5px;color:var(--muted);margin:0 auto}
.empty{padding:72px 0;text-align:center;color:var(--muted);font-family:var(--mono);font-size:12px;letter-spacing:.1em;text-transform:uppercase}
@media(max-width:900px){h1.sig-h1{font-size:22px}.page-head{flex-direction:column;align-items:flex-start}.sig-band{grid-template-columns:1fr}.sig-stats{grid-template-columns:repeat(2,1fr)}}
@media(max-width:600px){.wrap{padding:0 16px}.page-head{padding:24px 0 20px}.sig-stats{grid-template-columns:1fr 1fr}.sig-buyer,.sig-dl,.sig-src{display:none}thead th:nth-child(3),thead th:nth-child(7),thead th:nth-child(8){display:none}.filter-bar{flex-wrap:wrap;gap:8px}.filter-bar select{flex:1 1 calc(50% - 4px);min-width:0}.filter-bar button{flex:0 0 auto}.tbl-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch}.pager{flex-wrap:wrap;gap:10px;padding:20px 0 36px}.pg-info{order:3;width:100%;text-align:center;margin:0}}
</style>
</head>
<body>
${pageShellHeader(null, null)}
<main>
<div class="wrap">
  <div class="page-head">
    <div class="page-head-left">
      <div class="eyebrow">Opportunity Radar &middot; Live signals</div>
      <h1 class="sig-h1">The public record, in real time.</h1>
      <p class="sub">Every contract notice tracked across Contracts Finder and Find a Tender &mdash; updated hourly. 50 per page.</p>
    </div>
  </div>
  <div class="sig-band">
    <div class="chart-frame-wrap">
      <iframe src="/charts/embed" id="charts-frame" scrolling="no" frameborder="0"
        style="width:100%;height:360px;display:block;border:none"></iframe>
    </div>
    <div class="sig-stats">
      <div class="sc">
        <span class="sc-lbl">Signals tracked</span>
        <span class="sc-val">${parseInt(stats.total).toLocaleString()}</span>
        <span class="sc-sub">Across all 24 desks</span>
      </div>
      <div class="sc sc-green">
        <span class="sc-lbl">Open / live</span>
        <span class="sc-val">${parseInt(stats.open_cnt).toLocaleString()}</span>
        <span class="sc-sub">Active tenders &amp; opportunities</span>
      </div>
      <div class="sc sc-brand">
        <span class="sc-lbl">Total value tracked</span>
        <span class="sc-val">${fmtBigVal(totalVal)}</span>
        <span class="sc-sub">Public spend indexed</span>
      </div>
      <div class="sc sc-warn">
        <span class="sc-lbl">Closing within 7 days</span>
        <span class="sc-val">${parseInt(stats.closing7).toLocaleString()}</span>
        <span class="sc-sub">${parseInt(stats.closing14).toLocaleString()} closing within 14 days</span>
      </div>
    </div>
  </div>
  <form class="filter-bar" method="get" action="/signals">
    <select name="cat">
      <option value="">All desks</option>
      ${catOptions}
    </select>
    <select name="status">
      <option value="all" ${statusFilter === "all" ? "selected" : ""}>All statuses</option>
      <option value="open" ${statusFilter === "open" ? "selected" : ""}>Open / active only</option>
    </select>
    <select name="src">
      <option value="" ${!srcFilter ? "selected" : ""}>All sources</option>
      <option value="CF" ${srcFilter === "CF" ? "selected" : ""}>Contracts Finder (CF)</option>
      <option value="FTS" ${srcFilter === "FTS" ? "selected" : ""}>Find a Tender (FTS)</option>
    </select>
    <button type="submit">Apply</button>
    ${hasFilter ? `<a href="/signals" class="filter-clear">Clear filters</a>` : ""}
    <span class="sig-count">${totalFiltered.toLocaleString()} signal${totalFiltered !== 1 ? "s" : ""} · page ${page} of ${totalPages}</span>
  </form>
  <div class="tbl-wrap">
    <table>
      <thead><tr>
        <th>Desk</th><th>Notice</th><th>Buyer</th><th>Value</th><th>Status</th><th>Published</th><th>Deadline</th><th>Src</th>
      </tr></thead>
      <tbody>
        ${rows || `<tr><td colspan="8" class="empty">No signals found — try adjusting filters</td></tr>`}
      </tbody>
    </table>
  </div>
  ${pager}
</div>
</main>
${pageShellFoot()}
</body>
</html>`);
}));

app.get("/articles", (_req, res) => {
  res.type("html").send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Articles — GovRevenue</title>
<style>${pageShellCss()}
/* ── articles page ── */
.art-page-head{padding:52px 0 40px;border-bottom:1px solid var(--border)}
.art-eyebrow{font-family:var(--mono);font-size:10px;letter-spacing:.18em;text-transform:uppercase;color:var(--brand);margin-bottom:12px}
h1.art-h1{font-family:var(--sans);font-size:36px;font-weight:800;letter-spacing:-.025em;line-height:1.1;margin-bottom:12px;color:var(--text)}
.art-sub{font-size:15px;color:var(--muted);max-width:42em}
.articles-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:20px;padding:40px 0 72px}
.article-card{border:1px solid var(--border-2);padding:28px;background:var(--surface);display:flex;flex-direction:column;gap:10px;transition:border-color .2s,background .2s}
.article-card:hover{border-color:rgba(255,255,255,.2);background:var(--surface-2)}
.art-tag{font-family:var(--mono);font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:var(--brand)}
.art-title{font-family:var(--sans);font-size:18px;font-weight:700;line-height:1.25;letter-spacing:-.01em;color:var(--text)}
.art-desc{font-size:13.5px;color:var(--muted);line-height:1.65;flex:1}
.art-meta{font-family:var(--mono);font-size:10.5px;color:var(--muted);display:flex;gap:16px}
.art-cta{font-family:var(--mono);font-size:10.5px;letter-spacing:.08em;text-transform:uppercase;color:var(--brand);border-bottom:1px solid currentColor;padding-bottom:1px;align-self:flex-start}
.coming-banner{background:var(--surface-2);border:1px solid var(--border-2);padding:44px 40px;text-align:center;margin:44px 0}
.coming-banner h2{font-family:var(--sans);font-size:24px;font-weight:700;letter-spacing:-.02em;color:var(--text);margin-bottom:10px}
.coming-banner p{font-size:14px;color:var(--muted);max-width:36em;margin:0 auto 24px}
.sub-form{display:flex;gap:0;max-width:440px;margin:0 auto;border:1px solid var(--border-2)}
.sub-form input{flex:1;font-family:var(--mono);font-size:13px;padding:12px 14px;border:0;background:var(--surface);color:var(--text);outline:none}
.sub-form input::placeholder{color:var(--muted)}
.sub-form input:focus{outline:2px solid var(--brand);outline-offset:-2px}
.sub-form button{font-family:var(--mono);font-size:10.5px;letter-spacing:.1em;text-transform:uppercase;padding:0 20px;background:var(--brand);color:#fff;border:0;cursor:pointer}
.sub-form button:hover{background:var(--brand-hot)}
.art-wrap{max-width:1280px;padding:0 40px;margin:0 auto}
@media(max-width:700px){.articles-grid{grid-template-columns:1fr}h1.art-h1{font-size:26px}.art-wrap{padding:0 16px}}
</style>
</head>
<body>
${pageShellHeader(null, null)}
<main>
<div class="art-wrap">
  <div class="art-page-head">
    <div class="art-eyebrow">Intelligence Briefings</div>
    <h1 class="art-h1">Read before you bid.</h1>
    <p class="art-sub">Practical intelligence on procurement strategy, buyer mapping, framework access, and how to read the public record before your competitors do.</p>
  </div>
  <div class="articles-grid">
    <div class="article-card">
      <span class="art-tag">Strategy</span>
      <div class="art-title">How to read a procurement record before the tender drops</div>
      <div class="art-desc">Most firms wait for the ITT. The buyers who win consistently start six months earlier &mdash; and the public record tells you exactly when to move.</div>
      <div class="art-meta"><span>Coming soon</span></div>
    </div>
    <div class="article-card">
      <span class="art-tag">Bid writing</span>
      <div class="art-title">Evidence grade: why your bid loses before the scoring starts</div>
      <div class="art-desc">Evaluators can tell within two pages whether a supplier has done the homework. Here&rsquo;s what separates a Bronze bid from a Gold one.</div>
      <div class="art-meta"><span>Coming soon</span></div>
    </div>
    <div class="article-card">
      <span class="art-tag">Framework access</span>
      <div class="art-title">The framework shortcut: pre-qualification without the open tender</div>
      <div class="art-desc">Over 60% of public sector spend flows through frameworks. Most SMEs aren&rsquo;t on them &mdash; and most frameworks are easier to join than they look.</div>
      <div class="art-meta"><span>Coming soon</span></div>
    </div>
    <div class="article-card">
      <span class="art-tag">Buyer intelligence</span>
      <div class="art-title">Mapping the buyer: who really controls the budget</div>
      <div class="art-desc">The contracting authority on the notice is rarely the person you need to speak to. The procurement record tells you who has commissioned before.</div>
      <div class="art-meta"><span>Coming soon</span></div>
    </div>
  </div>
  <div class="coming-banner">
    <h2>First briefings publishing soon.</h2>
    <p>Enter your email and we&rsquo;ll send you each piece as it goes live &mdash; plus a weekly digest of the open contracts most relevant to your sector.</p>
    <form class="sub-form" action="/form-submit" method="post">
      <input type="hidden" name="_type" value="briefing">
      <input type="hidden" name="_source" value="articles">
      <input type="email" name="email" placeholder="your@email.com" required>
      <button type="submit">Notify me</button>
    </form>
  </div>
</div>
</main>
${pageShellFoot()}
</body>
</html>`);
});

app.get("/charts", asyncRoute(async (req, res) => {
  type DetailPoint = { label: string; total_m: number; open_m: number; notice_count: number; open_count: number };
  type DeskBreak = { label: string; total_m: number; count: number };

  const OUTLIER_CAP = 2_000_000_000;
  const fmtBn = (m: number) => m >= 1000 ? `£${(m / 1000).toFixed(2)}bn` : `£${m.toFixed(0)}m`;
  const fmtBnShort = (m: number) => m >= 1000 ? `£${(m / 1000).toFixed(1)}bn` : `£${Math.round(m)}m`;

  let monthPoints: DetailPoint[] = [];
  let weekPoints: DetailPoint[] = [];
  let deskBreak: DeskBreak[] = [];

  type BuyerRow = { buyer: string; cnt: string; total_val: string };
  type SourceRow = { source: string; cnt: string };
  type PipeRow = { closing_30: string; closing_60: string; open_count: string };
  type DeskPointRow = { mlabel: string; category: string; total_m: number };
  let topBuyers: BuyerRow[] = [];
  let sourceSplit: SourceRow[] = [];
  let closing30 = 0, closing60 = 0, totalOpenCount = 0;
  let monthDeskMap: Record<string, { label: string; total_m: number }[]> = {};
  let weekDeskMap: Record<string, { label: string; total_m: number }[]> = {};

  if (pool) {
    const [mR, wR, dR, buyerR, sourceR, pipeR, mdDeskR, wdDeskR] = await Promise.all([
      pool.query<DetailPoint>(`
        SELECT to_char(date_trunc('month', notice_date), 'Mon ''YY') AS label,
               ROUND(SUM(value_amount) FILTER (WHERE value_amount > 0 AND value_amount < ${OUTLIER_CAP}) / 1e6::numeric, 2)::float AS total_m,
               ROUND(COALESCE(SUM(value_amount) FILTER (WHERE (LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%') AND value_amount > 0 AND value_amount < ${OUTLIER_CAP}), 0) / 1e6::numeric, 2)::float AS open_m,
               COUNT(*)::int AS notice_count,
               COUNT(*) FILTER (WHERE LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')::int AS open_count
        FROM homepage_signals
        WHERE notice_date > NOW() - INTERVAL '13 months' AND notice_date <= NOW() AND notice_date IS NOT NULL
        GROUP BY date_trunc('month', notice_date)
        ORDER BY date_trunc('month', notice_date)`),
      pool.query<DetailPoint>(`
        SELECT to_char(date_trunc('week', notice_date), 'DD Mon') AS label,
               ROUND(SUM(value_amount) FILTER (WHERE value_amount > 0 AND value_amount < ${OUTLIER_CAP}) / 1e6::numeric, 2)::float AS total_m,
               ROUND(COALESCE(SUM(value_amount) FILTER (WHERE (LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%') AND value_amount > 0 AND value_amount < ${OUTLIER_CAP}), 0) / 1e6::numeric, 2)::float AS open_m,
               COUNT(*)::int AS notice_count,
               COUNT(*) FILTER (WHERE LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')::int AS open_count
        FROM homepage_signals
        WHERE notice_date > NOW() - INTERVAL '14 weeks' AND notice_date <= NOW() AND notice_date IS NOT NULL
        GROUP BY date_trunc('week', notice_date)
        ORDER BY date_trunc('week', notice_date)`),
      pool.query<{ category: string; total_val: string; cnt: string }>(`
        SELECT category, SUM(value_amount)::text AS total_val, COUNT(*)::text AS cnt
        FROM homepage_signals
        WHERE notice_date > NOW() - INTERVAL '13 months' AND notice_date IS NOT NULL AND value_amount > 0 AND value_amount < ${OUTLIER_CAP}
        GROUP BY category ORDER BY SUM(value_amount) DESC LIMIT 6`),
      pool.query<BuyerRow>(`
        SELECT buyer,
               COUNT(*)::text AS cnt,
               ROUND(SUM(value_amount) FILTER (WHERE value_amount > 0 AND value_amount < ${OUTLIER_CAP}) / 1e6::numeric, 1)::text AS total_val
        FROM homepage_signals
        WHERE notice_date > NOW() - INTERVAL '13 months' AND buyer IS NOT NULL AND buyer <> '' AND notice_date IS NOT NULL
        GROUP BY buyer ORDER BY SUM(value_amount) DESC NULLS LAST LIMIT 5`),
      pool.query<SourceRow>(`
        SELECT source, COUNT(*)::text AS cnt
        FROM homepage_signals
        WHERE notice_date > NOW() - INTERVAL '13 months' AND notice_date IS NOT NULL
        GROUP BY source ORDER BY COUNT(*) DESC`),
      pool.query<PipeRow>(`
        SELECT
          COUNT(*) FILTER (WHERE deadline_date BETWEEN NOW() AND NOW() + INTERVAL '30 days')::text AS closing_30,
          COUNT(*) FILTER (WHERE deadline_date BETWEEN NOW() AND NOW() + INTERVAL '60 days')::text AS closing_60,
          COUNT(*) FILTER (WHERE LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')::text AS open_count
        FROM homepage_signals WHERE deadline_date IS NOT NULL`),
      pool.query<DeskPointRow>(`
        SELECT to_char(date_trunc('month', notice_date), 'Mon ''YY') AS mlabel,
               category,
               ROUND(SUM(value_amount) FILTER (WHERE value_amount > 0 AND value_amount < ${OUTLIER_CAP}) / 1e6::numeric, 1)::float AS total_m
        FROM homepage_signals
        WHERE notice_date > NOW() - INTERVAL '13 months' AND notice_date IS NOT NULL
        GROUP BY date_trunc('month', notice_date), category
        ORDER BY date_trunc('month', notice_date), SUM(value_amount) DESC NULLS LAST`),
      pool.query<DeskPointRow>(`
        SELECT to_char(date_trunc('week', notice_date), 'DD Mon') AS mlabel,
               category,
               ROUND(SUM(value_amount) FILTER (WHERE value_amount > 0 AND value_amount < ${OUTLIER_CAP}) / 1e6::numeric, 1)::float AS total_m
        FROM homepage_signals
        WHERE notice_date > NOW() - INTERVAL '14 weeks' AND notice_date IS NOT NULL
        GROUP BY date_trunc('week', notice_date), category
        ORDER BY date_trunc('week', notice_date), SUM(value_amount) DESC NULLS LAST`),
    ]);
    monthPoints = mR.rows.map(r => ({ ...r, total_m: r.total_m || 0, open_m: r.open_m || 0 }));
    weekPoints = wR.rows.map(r => ({ ...r, total_m: r.total_m || 0, open_m: r.open_m || 0 }));
    deskBreak = dR.rows.map(r => ({
      label: DESK_PROFILES.find(d => d.slug === r.category)?.label || r.category,
      total_m: parseFloat(r.total_val) / 1e6,
      count: parseInt(r.cnt),
    }));
    topBuyers = buyerR.rows;
    sourceSplit = sourceR.rows;
    const pR = pipeR.rows[0];
    if (pR) { closing30 = parseInt(pR.closing_30) || 0; closing60 = parseInt(pR.closing_60) || 0; totalOpenCount = parseInt(pR.open_count) || 0; }
    for (const r of mdDeskR.rows) {
      if (!r.total_m || r.total_m <= 0) continue;
      const lbl = DESK_PROFILES.find(d => d.slug === r.category)?.label || r.category;
      (monthDeskMap[r.mlabel] = monthDeskMap[r.mlabel] || []).push({ label: lbl, total_m: r.total_m });
    }
    for (const r of wdDeskR.rows) {
      if (!r.total_m || r.total_m <= 0) continue;
      const lbl = DESK_PROFILES.find(d => d.slug === r.category)?.label || r.category;
      (weekDeskMap[r.mlabel] = weekDeskMap[r.mlabel] || []).push({ label: lbl, total_m: r.total_m });
    }
  }

  const totalAnnualM = monthPoints.reduce((s, p) => s + p.total_m, 0);
  const avgMonthlyM = monthPoints.length > 0 ? totalAnnualM / monthPoints.length : 0;
  const peakPoint = monthPoints.reduce((best, p) => p.total_m > best.total_m ? p : best, monthPoints[0] || { label: "—", total_m: 0, open_m: 0, notice_count: 0, open_count: 0 });
  const troughPoint = monthPoints.reduce((low, p) => p.total_m > 0 && p.total_m < low.total_m ? p : low, monthPoints.find(p => p.total_m > 0) || peakPoint);
  const totalNotices = monthPoints.reduce((s, p) => s + p.notice_count, 0);
  const openPipelineM = monthPoints.reduce((s, p) => s + p.open_m, 0);
  const first3M = monthPoints.length >= 3 ? monthPoints.slice(0, 3).reduce((s, p) => s + p.total_m, 0) / 3 : avgMonthlyM;
  const last3M = monthPoints.length >= 3 ? monthPoints.slice(-3).reduce((s, p) => s + p.total_m, 0) / 3 : avgMonthlyM;
  const trendPct = first3M > 0 ? Math.round(((last3M - first3M) / first3M) * 100) : 0;
  const topDesk = deskBreak[0];
  const topDeskSharePct = totalAnnualM > 0 && topDesk ? Math.round((topDesk.total_m / totalAnnualM) * 100) : 0;
  const peakVsAvgPct = avgMonthlyM > 0 ? Math.round(((peakPoint.total_m - avgMonthlyM) / avgMonthlyM) * 100) : 0;
  const hasData = monthPoints.length >= 3;
  const reportDate = new Date().toLocaleDateString("en-GB", { day: "numeric", month: "long", year: "numeric" });
  const reportMonthRange = monthPoints.length >= 2 ? `${monthPoints[0].label} to ${monthPoints[monthPoints.length - 1].label}` : "last 12 months";

  const cfRow = sourceSplit.find(s => s.source === 'CF');
  const ftsRow = sourceSplit.find(s => s.source === 'FTS');
  const cfCount = cfRow ? parseInt(cfRow.cnt) : 0;
  const ftsCount = ftsRow ? parseInt(ftsRow.cnt) : 0;
  const totalSrcCount = (cfCount + ftsCount) || 1;
  const cfPct = Math.round((cfCount / totalSrcCount) * 100);
  const ftsPct = 100 - cfPct;
  const srcSplitLabel = cfCount > 0 && ftsCount > 0
    ? `${cfPct}% from Contracts Finder and ${ftsPct}% from the Find a Tender Service`
    : `from Contracts Finder and the Find a Tender Service`;
  const top3DesksText = deskBreak.length >= 3
    ? `<strong>${escapeHtml(deskBreak[0].label)}</strong> (${fmtBnShort(deskBreak[0].total_m)}), <strong>${escapeHtml(deskBreak[1].label)}</strong> (${fmtBnShort(deskBreak[1].total_m)}) and <strong>${escapeHtml(deskBreak[2].label)}</strong> (${fmtBnShort(deskBreak[2].total_m)})`
    : topDesk ? `<strong>${escapeHtml(topDesk.label)}</strong> (${fmtBnShort(topDesk.total_m)})` : '';
  const topBuyerPara = topBuyers.length > 0
    ? `The highest-volume contracting authority by indexed spend was <strong>${escapeHtml(topBuyers[0].buyer)}</strong>, with ${topBuyers[0].cnt} notices totalling £${topBuyers[0].total_val}m over the period (GovRevenue, 2026).${topBuyers.length >= 3 ? ` Alongside ${escapeHtml(topBuyers[1].buyer)} and ${escapeHtml(topBuyers[2].buyer)}, a compact cohort of high-volume authorities drives a disproportionate share of category spend — consistent with the framework-incumbent dynamics documented by the National Audit Office (2023). Buyer concentration data of this resolution transforms undifferentiated market signals into targetable accounts.` : ''}`
    : `Buyer-level data was not resolvable for the observation period; a minimum signal volume across multiple desks is required for statistically robust buyer ranking.`;

  res.type("html").send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>UK Public Sector Procurement Spend Analysis 2026 — GovRevenue</title>
<meta name="description" content="Live intelligence on ${fmtBnShort(totalAnnualM)} in UK public procurement. ${totalNotices.toLocaleString()} notices tracked across 24 sector desks. Open pipeline ${fmtBnShort(openPipelineM)}. Updated hourly from Contracts Finder and Find a Tender.">
<meta name="robots" content="index, follow">
<meta property="og:type" content="article">
<meta property="og:title" content="${fmtBnShort(totalAnnualM)} in UK Public Contracts — Live Procurement Intelligence">
<meta property="og:description" content="${totalNotices.toLocaleString()} procurement notices tracked. ${fmtBnShort(openPipelineM)} open pipeline. ${closing30} contracts closing in the next 30 days.">
<meta property="og:url" content="https://govrevenue-agent-production.up.railway.app/charts">
<link rel="canonical" href="https://govrevenue-agent-production.up.railway.app/charts">
<script type="application/ld+json">{"@context":"https://schema.org","@type":"Dataset","name":"UK Public Sector Procurement Spend Intelligence 2026","description":"Live spend signal across 24 procurement sector desks. ${totalNotices.toLocaleString()} notices. ${fmtBnShort(totalAnnualM)} awarded value.","url":"https://govrevenue-agent-production.up.railway.app/charts","provider":{"@type":"Organization","name":"GovRevenue","url":"https://govrevenue-agent-production.up.railway.app"},"temporalCoverage":"${escapeHtml(reportMonthRange)}","keywords":["UK public procurement","government contracts 2026","contracts finder","find a tender","public sector spend","procurement intelligence","awarded contracts UK"]}<\/script>
<style>${pageShellCss()}
/* ── charts / intelligence page ── */
strong{font-weight:700}
.hero-ch{position:relative;background:var(--base);overflow:hidden;border-bottom:1px solid var(--border);min-height:48vh;display:flex;align-items:center}
.hero-orb{position:absolute;border-radius:50%;filter:blur(80px);pointer-events:none}
.orb1{width:600px;height:600px;top:-120px;right:-60px;background:radial-gradient(circle,rgba(180,146,78,0.1) 0%,transparent 70%);animation:fl1 11s ease-in-out infinite}
.orb2{width:380px;height:380px;bottom:-40px;left:8%;background:radial-gradient(circle,rgba(34,197,94,0.05) 0%,transparent 70%);animation:fl2 15s ease-in-out infinite}
@keyframes fl1{0%,100%{transform:translateY(0)}50%{transform:translateY(-36px)}}
@keyframes fl2{0%,100%{transform:translateY(0)}50%{transform:translateY(28px)}}
.hero-inner{position:relative;z-index:1;max-width:1320px;margin:0 auto;padding:56px 40px 48px;width:100%}
.hero-badge{display:inline-flex;align-items:center;gap:8px;background:rgba(180,146,78,.1);border:1px solid rgba(180,146,78,.3);padding:5px 14px 5px 10px;font-family:var(--mono);font-size:10px;letter-spacing:.13em;text-transform:uppercase;color:var(--brand);margin-bottom:22px}
.hero-dot{width:6px;height:6px;border-radius:50%;background:var(--green);animation:pdot 2s infinite}
@keyframes pdot{0%,100%{opacity:1}50%{opacity:.25}}
.hero-h1{font-family:var(--sans);font-size:clamp(28px,4vw,52px);font-weight:800;line-height:1.06;letter-spacing:-.04em;color:var(--text);margin-bottom:16px;max-width:780px}
.hero-h1 em{font-style:normal;color:var(--brand)}
.hero-sub{font-size:16px;color:var(--muted);max-width:520px;line-height:1.65;margin-bottom:30px}
.hero-ctas{display:flex;gap:14px;align-items:center;flex-wrap:wrap}
.btn-p{background:var(--brand);color:#fff;font-size:13px;font-weight:700;padding:12px 24px;transition:opacity .15s;border-radius:2px}.btn-p:hover{opacity:.88}
.btn-s{border:1px solid var(--border-2);color:var(--text);font-size:13px;padding:11px 20px;background:transparent;transition:border-color .15s;border-radius:2px}.btn-s:hover{border-color:rgba(255,255,255,.35)}
.kpi-band{padding:48px 0;background:var(--surface);border-bottom:1px solid var(--border)}
.kpi-row{max-width:1320px;margin:0 auto;padding:0 40px;display:grid;grid-template-columns:repeat(5,1fr);gap:10px}
.kcard{background:var(--surface-2);border:1px solid var(--border-2);border-top:3px solid var(--border-2);padding:22px 24px 20px;transition:border-color .2s,transform .2s;cursor:default;position:relative;overflow:hidden}
.kcard:hover{border-color:rgba(255,255,255,.22);transform:translateY(-2px)}
.kcard.gold{border-top-color:#B4924E}.kcard.grn{border-top-color:#22C55E}.kcard.warn{border-top-color:#EF4444}.kcard.blue{border-top-color:#3B82F6}.kcard.purple{border-top-color:#A78BFA}
.kcard-label{font-family:var(--mono);font-size:9px;letter-spacing:.18em;text-transform:uppercase;color:var(--faint);margin-bottom:12px;display:block}
.kcard-val{font-family:var(--mono);font-size:30px;font-weight:700;letter-spacing:-.04em;line-height:1;display:block;color:var(--text)}
.kcard-val.c-gold{color:#C8A96B}.kcard-val.c-grn{color:#4ade80}.kcard-val.c-red{color:#f87171}.kcard-val.c-blue{color:#60A5FA}.kcard-val.c-purple{color:#C4B5FD}
.kcard-sub{font-family:var(--mono);font-size:9.5px;color:var(--faint);margin-top:10px;display:block;line-height:1.5}
.kcard-glow{position:absolute;bottom:-24px;right:-24px;width:90px;height:90px;border-radius:50%;opacity:.12;filter:blur(24px);pointer-events:none}
.kcard-glow.gold{background:#B4924E}.kcard-glow.grn{background:#22C55E}.kcard-glow.red{background:#EF4444}
/* pipeline urgency */
.pipe-band{padding:40px 0;background:#0A1C12;border-bottom:1px solid rgba(236,230,214,.08)}
.pipe-inner{max-width:1320px;margin:0 auto;padding:0 40px;display:flex;align-items:center;gap:48px;flex-wrap:wrap}
.pipe-label{font-family:var(--mono);font-size:10px;letter-spacing:.22em;text-transform:uppercase;color:#5a8a6a;flex-shrink:0}
.pipe-cells{display:flex;gap:4px;flex:1}
.pipe-cell{flex:1;background:rgba(255,255,255,.04);border:1px solid rgba(236,230,214,.08);padding:20px 24px}
.pipe-val{font-family:var(--mono);font-size:42px;font-weight:700;letter-spacing:-.05em;color:#ECE6D6;line-height:1}
.pipe-val.urgent{color:#f87171}
.pipe-sub{font-family:var(--mono);font-size:10px;color:#5a8a6a;letter-spacing:.06em;text-transform:uppercase;margin-top:8px}
.pipe-bar{height:2px;background:rgba(236,230,214,.08);margin-top:14px}
.pipe-bar-fill{height:100%;background:#22C55E}
.pipe-cta{flex-shrink:0}
.pipe-cta a{display:inline-block;font-family:var(--mono);font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:#ECE6D6;border:1px solid rgba(236,230,214,.25);padding:12px 20px;transition:border-color .15s}
.pipe-cta a:hover{border-color:#ECE6D6}
.sec-eye{font-family:var(--mono);font-size:10px;letter-spacing:.2em;text-transform:uppercase;color:var(--brand);margin-bottom:12px}
.sec-h{font-family:var(--sans);font-size:clamp(22px,2.8vw,36px);font-weight:800;letter-spacing:-.03em;line-height:1.1;color:var(--text)}
.sec-sub{font-size:14.5px;color:var(--muted);max-width:50em;margin-top:10px;line-height:1.65}
.chart-sec{padding:64px 0;border-bottom:1px solid var(--border);background:var(--base)}
.chart-wrap{max-width:1320px;margin:0 auto;padding:0 40px}
.chart-head{display:flex;align-items:flex-start;justify-content:space-between;gap:24px;margin-bottom:28px;flex-wrap:wrap}
.tog-grp{display:flex;border:1px solid var(--border-2)}
.tog{font-family:var(--mono);font-size:10.5px;letter-spacing:.08em;text-transform:uppercase;padding:8px 18px;color:var(--muted);background:transparent;border:none;cursor:pointer;transition:background .15s,color .15s}
.tog.tog-active{background:var(--brand);color:#fff}
.chart-legend{display:flex;gap:20px;align-items:center;margin-top:10px}
.leg{display:flex;align-items:center;gap:6px;font-family:var(--mono);font-size:10.5px;color:var(--muted)}
.leg-line{width:22px;height:2px;background:var(--brand)}
.leg-dash{width:22px;height:0;border-bottom:2px dashed var(--green)}
.chart-box{background:var(--surface-2);border:1px solid var(--border-2);overflow:hidden;position:relative}
canvas#detailChart{display:block;width:100%}
.chart-tip{position:absolute;background:var(--surface-3);color:var(--text);border:1px solid var(--border-2);padding:13px 16px;font-family:var(--mono);font-size:11px;pointer-events:none;display:none;z-index:10;width:226px;box-shadow:0 8px 24px rgba(27,30,25,.12)}
.tip-label{font-size:12px;font-weight:700;color:var(--text);margin-bottom:8px;text-transform:uppercase;letter-spacing:.06em}
.tip-row{display:flex;align-items:center;gap:7px;margin-top:4px}
.tip-dot{width:7px;height:7px;border-radius:50%;flex-shrink:0}
.sectors-sec{padding:64px 0;border-bottom:1px solid var(--border);background:var(--surface)}
.sectors-wrap{max-width:1320px;margin:0 auto;padding:0 40px;display:grid;grid-template-columns:1fr 1fr;gap:64px;align-items:start}
.sectors-list{margin-top:0;border:1px solid var(--border-2)}
.sector-head-row{background:var(--surface-3);padding:10px 18px;border-bottom:1px solid var(--border);font-family:var(--mono);font-size:9px;letter-spacing:.18em;text-transform:uppercase;color:var(--muted);display:flex;justify-content:space-between}
.srow{display:flex;align-items:center;gap:14px;padding:14px 18px;border-bottom:1px solid var(--border);transition:background .15s}
.srow:last-child{border-bottom:none}
.srow:hover{background:var(--surface-3)}
.srank{font-family:var(--mono);font-size:10px;color:var(--faint);width:18px;flex-shrink:0;text-align:right}
.sname{font-size:13px;font-weight:600;color:var(--text);flex:1;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.sbar-wrap{flex:1.5;height:4px;background:var(--border-2);border-radius:2px;overflow:hidden}
.sbar-fill{height:100%;background:var(--brand);border-radius:2px;width:0;transition:width .9s ease}
.sval{font-family:var(--mono);font-size:11px;font-weight:700;color:var(--text);white-space:nowrap;width:68px;text-align:right}
.scnt{font-family:var(--mono);font-size:10px;color:var(--faint);width:52px;text-align:right}
/* buyers table */
.buyers-panel{margin-top:0}
.buyers-head-row{background:var(--surface-3);padding:10px 18px;border-bottom:1px solid var(--border);font-family:var(--mono);font-size:9px;letter-spacing:.18em;text-transform:uppercase;color:var(--muted);display:flex;justify-content:space-between;border:1px solid var(--border-2)}
.brow{display:flex;align-items:center;gap:12px;padding:14px 18px;border-bottom:1px solid var(--border);border-left:1px solid var(--border-2);border-right:1px solid var(--border-2);transition:background .15s}
.brow:last-child{border-bottom:1px solid var(--border-2)}
.brow:hover{background:var(--surface-3)}
.brank{font-family:var(--mono);font-size:11px;font-weight:700;color:var(--brand);width:22px;flex-shrink:0}
.bname{font-size:13px;font-weight:500;color:var(--text);flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.bval{font-family:var(--mono);font-size:11px;font-weight:700;color:var(--text);white-space:nowrap;width:70px;text-align:right}
.bcnt{font-family:var(--mono);font-size:10px;color:var(--faint);width:60px;text-align:right;white-space:nowrap}
.brief-sec{padding:72px 0;border-bottom:1px solid var(--border);background:var(--base)}
.brief-wrap{max-width:1160px;margin:0 auto;padding:0 40px}
.brief-intro{max-width:680px;margin-bottom:56px}
.brief-art{display:grid;grid-template-columns:100px 1fr;gap:0 52px;padding:52px 0;border-top:1px solid var(--border)}
.brief-num{font-family:var(--mono);font-size:11px;color:var(--muted);padding-top:3px}
.brief-num span{display:block;font-family:var(--sans);font-size:56px;font-weight:800;color:rgba(255,255,255,.04);letter-spacing:-.04em;line-height:1;margin-bottom:3px}
.brief-tag{font-family:var(--mono);font-size:9px;letter-spacing:.2em;text-transform:uppercase;color:var(--brand);margin-bottom:14px}
.brief-h{font-family:var(--sans);font-size:clamp(18px,2vw,24px);font-weight:700;letter-spacing:-.02em;color:var(--text);margin-bottom:16px;line-height:1.25}
.brief-body{font-family:var(--sans);font-size:15.5px;line-height:1.85;color:var(--muted)}
.brief-body p+p{margin-top:16px}
.brief-body strong{color:var(--text)}
.brief-body em{font-style:italic}
.pull{margin:24px 0;padding:18px 22px 18px 18px;border-left:3px solid var(--brand);background:rgba(180,146,78,.06)}
.pull p{font-family:var(--sans);font-size:16px;color:var(--text);line-height:1.55;letter-spacing:-.01em}
.brief-refs{margin-top:44px;padding-top:18px;border-top:1px solid var(--border)}
.nl-sec{padding:52px 0;background:var(--surface-2);border-top:1px solid var(--border)}
.nl-wrap{max-width:540px;margin:0 auto;padding:0 32px;text-align:center}
.nl-card{background:var(--surface);border:1px solid var(--border-2);padding:36px 40px;position:relative;overflow:hidden}
.nl-card::before{content:'';position:absolute;top:-50px;left:50%;transform:translateX(-50%);width:260px;height:260px;background:radial-gradient(circle,rgba(180,146,78,0.06) 0%,transparent 70%);pointer-events:none}
.nl-eye{font-family:var(--mono);font-size:10px;letter-spacing:.18em;text-transform:uppercase;color:var(--brand);margin-bottom:10px}
.nl-h{font-family:var(--sans);font-size:clamp(18px,2.2vw,24px);font-weight:700;letter-spacing:-.02em;color:var(--text);margin-bottom:8px;line-height:1.2}
.nl-sub{font-size:14px;color:var(--muted);margin-bottom:20px;max-width:380px;margin-left:auto;margin-right:auto}
.nl-form{display:flex;max-width:400px;margin:0 auto 10px;overflow:hidden;border:1px solid var(--border-2)}
.nl-in{flex:1;background:var(--surface-2);border:none;padding:11px 14px;font-family:var(--mono);font-size:13px;color:var(--text);outline:none}
.nl-in::placeholder{color:var(--muted)}
.nl-in:focus{outline:2px solid var(--brand);outline-offset:-2px}
.nl-btn{background:var(--brand);color:#fff;border:none;padding:11px 20px;font-family:var(--mono);font-size:10.5px;letter-spacing:.09em;text-transform:uppercase;cursor:pointer;white-space:nowrap;font-weight:700;transition:opacity .15s}.nl-btn:hover{opacity:.87}
.nl-note{font-family:var(--mono);font-size:10px;color:var(--muted);letter-spacing:.04em}
.nl-proof{display:flex;justify-content:center;gap:24px;margin-top:20px;padding-top:16px;border-top:1px solid var(--border);flex-wrap:wrap}
.nl-pv{display:block;font-family:var(--mono);font-size:18px;font-weight:600;color:var(--text);letter-spacing:-.025em}
.nl-pl{display:block;font-family:var(--mono);font-size:9px;letter-spacing:.13em;text-transform:uppercase;color:var(--muted);margin-top:3px}
.rv{opacity:0;transform:translateY(20px);transition:opacity .6s ease,transform .6s ease}.rv.in{opacity:1;transform:none}
@media(max-width:1100px){
  .kpi-row{grid-template-columns:repeat(3,1fr);gap:8px}
  .hero-inner,.chart-wrap,.sectors-wrap,.brief-wrap,.nl-wrap{padding-left:24px;padding-right:24px}
}
@media(max-width:1024px){
  .sectors-wrap{grid-template-columns:1fr;gap:32px}
}
@media(max-width:768px){
  .kpi-row{grid-template-columns:repeat(2,1fr);padding:0 16px;gap:8px}
  .kpi-band{padding:32px 0}
  .hero-inner,.chart-wrap,.sectors-wrap,.brief-wrap,.nl-wrap{padding-left:16px;padding-right:16px}
  .hero-ch{min-height:auto}
  .hero-inner{padding:40px 16px 36px}
  .hero-h1{font-size:28px}
  .hero-sub{font-size:15px}
  .pipe-band{padding:28px 0}
  .pipe-inner{padding:0 16px;gap:16px;flex-direction:column}
  .pipe-label{display:none}
  .pipe-cells{gap:6px}
  .pipe-cell{padding:16px 18px}
  .pipe-val{font-size:28px}
  .pipe-cta{width:100%}
  .pipe-cta a{display:block;text-align:center;padding:12px}
  .brief-art{grid-template-columns:1fr;gap:0}.brief-num{display:none}
  .nl-card{padding:24px 20px}
  .tog-grp{display:flex}
  .chart-head{flex-direction:column;gap:14px}
}
@media(max-width:480px){
  .kpi-row{grid-template-columns:1fr 1fr;padding:0 12px;gap:6px}
  .kcard{padding:16px 14px}
  .kcard-val{font-size:22px}
  .hero-h1{font-size:22px}
  .pipe-cells{flex-direction:column}
  .pipe-cell{padding:12px 14px}
  .pipe-val{font-size:24px}
  .nl-form{flex-direction:column}.nl-in,.nl-btn{width:100%}
  .sec-h{font-size:22px}
}
</style>
</head>
<body>
${pageShellHeader(null, getAuthUser(req))}

<main>

<section class="hero-ch" aria-label="UK Procurement Intelligence Overview">
  <div class="hero-orb orb1" aria-hidden="true"></div>
  <div class="hero-orb orb2" aria-hidden="true"></div>
  <div class="hero-inner">
    <div class="hero-badge" role="status">
      <span class="hero-dot" aria-hidden="true"></span>
      Live procurement intelligence &middot; Updated hourly
    </div>
    <h1 class="hero-h1">
      <em>${fmtBnShort(totalAnnualM)}+</em> in UK public contracts.<br>See where the money moves.
    </h1>
    <p class="hero-sub">Public bodies publish every contract. Searchable, predictable, and entirely pre-tender. This is what the spend curve looks like when you actually read it.</p>
    <div class="hero-ctas">
      <a href="/scan" class="btn-p">Find your contracts &rarr;</a>
      <a href="/signals" class="btn-s">Browse open notices</a>
    </div>
  </div>
</section>

<section class="kpi-band" aria-label="Key procurement metrics">
  <div class="kpi-row">
    <div class="kcard gold rv">
      <span class="kcard-label">12-month awarded</span>
      <span class="kcard-val c-gold">${fmtBnShort(totalAnnualM)}</span>
      <span class="kcard-sub">${escapeHtml(reportMonthRange)} &middot; all 24 desks</span>
      <div class="kcard-glow gold" aria-hidden="true"></div>
    </div>
    <div class="kcard grn rv">
      <span class="kcard-label">Open pipeline now</span>
      <span class="kcard-val c-grn">${fmtBnShort(openPipelineM)}</span>
      <span class="kcard-sub">${totalOpenCount.toLocaleString()} live tenders accepting bids</span>
      <div class="kcard-glow grn" aria-hidden="true"></div>
    </div>
    <div class="kcard warn rv">
      <span class="kcard-label">Closing in 30 days</span>
      <span class="kcard-val c-red">${closing30.toLocaleString()}</span>
      <span class="kcard-sub">${closing60.toLocaleString()} within 60 days</span>
      <div class="kcard-glow red" aria-hidden="true"></div>
    </div>
    <div class="kcard blue rv">
      <span class="kcard-label">Monthly average</span>
      <span class="kcard-val c-blue">${fmtBnShort(avgMonthlyM)}</span>
      <span class="kcard-sub">Per month over the tracked period</span>
    </div>
    <div class="kcard purple rv">
      <span class="kcard-label">Momentum</span>
      <span class="kcard-val c-purple">${trendPct >= 0 ? "▲" : "▼"} ${Math.abs(trendPct)}%</span>
      <span class="kcard-sub">3-month trailing vs opening &middot; peak ${escapeHtml(peakPoint.label)}</span>
    </div>
  </div>
</section>

<section class="pipe-band" aria-label="Pipeline urgency">
  <div class="pipe-inner">
    <span class="pipe-label">Active window</span>
    <div class="pipe-cells">
      <div class="pipe-cell">
        <div class="pipe-val c-grn">${totalOpenCount.toLocaleString()}</div>
        <div class="pipe-sub">Open now</div>
        <div class="pipe-bar"><div class="pipe-bar-fill" style="width:100%"></div></div>
      </div>
      <div class="pipe-cell">
        <div class="pipe-val">${closing30.toLocaleString()}</div>
        <div class="pipe-sub">Closing ≤ 30 days</div>
        <div class="pipe-bar"><div class="pipe-bar-fill" style="width:${totalOpenCount > 0 ? Math.round((closing30 / totalOpenCount) * 100) : 50}%;background:#B4924E"></div></div>
      </div>
      <div class="pipe-cell">
        <div class="pipe-val urgent">${closing60.toLocaleString()}</div>
        <div class="pipe-sub">Closing ≤ 60 days</div>
        <div class="pipe-bar"><div class="pipe-bar-fill" style="width:${totalOpenCount > 0 ? Math.round((closing60 / totalOpenCount) * 100) : 70}%;background:#f87171"></div></div>
      </div>
      <div class="pipe-cell">
        <div class="pipe-val" style="color:#C8A96B">${fmtBnShort(openPipelineM)}</div>
        <div class="pipe-sub">Pipeline value</div>
        <div class="pipe-bar"><div class="pipe-bar-fill" style="width:85%;background:#C8A96B"></div></div>
      </div>
    </div>
    <div class="pipe-cta"><a href="/signals?status=open">Browse open &rarr;</a></div>
  </div>
</section>

<section class="chart-sec" aria-label="Spend trend chart">
  <div class="chart-wrap">
    <div class="chart-head">
      <div>
        <div class="sec-eye">Spend Signal &middot; ${escapeHtml(reportMonthRange)}</div>
        <h2 class="sec-h" style="font-size:clamp(22px,2.6vw,34px)">Awarded spend and open pipeline</h2>
        <div class="chart-legend">
          <div class="leg"><div class="leg-line" aria-hidden="true"></div>Awarded value</div>
          <div class="leg"><div class="leg-dash" aria-hidden="true"></div>Open pipeline</div>
        </div>
      </div>
      <div class="tog-grp" role="group" aria-label="Chart period toggle">
        <button class="tog tog-active" id="tog-month" aria-pressed="true">12 Months</button>
        <button class="tog" id="tog-week" aria-pressed="false">12 Weeks</button>
      </div>
    </div>
    <div class="chart-box">
      <canvas id="detailChart" role="img" aria-label="UK procurement spend chart"></canvas>
      <div class="chart-tip" id="chartTip" role="tooltip" aria-live="polite"></div>
    </div>
    <p style="font-family:var(--mono);font-size:10px;color:var(--t3);margin-top:14px;letter-spacing:.04em">Source: Contracts Finder (CF) &middot; Find a Tender Service (FTS) &middot; Notices above &pound;2bn excluded as outliers &middot; Updated hourly</p>
  </div>
</section>

${deskBreak.length > 0 ? `
<section class="sectors-sec rv" aria-label="Sector spend breakdown">
  <div class="sectors-wrap">
    <div>
      <div class="sec-eye">Where the money is concentrated</div>
      <h2 class="sec-h">Top sectors by awarded spend</h2>
      <p class="sec-sub" style="margin-bottom:28px">Public procurement is not evenly distributed. ${topDesk ? `<strong style="color:var(--text)">${escapeHtml(topDesk.label)}</strong> alone accounts for ${topDeskSharePct}% of indexed value.` : ""} These are the categories driving the most government contract value right now.</p>
      <div class="sectors-list">
        <div class="sector-head-row"><span>Sector desk</span><span>Notices &middot; Awarded</span></div>
        ${(() => {
          const maxV = deskBreak[0]?.total_m || 1;
          return deskBreak.map((d, i) => {
            const pct = Math.round((d.total_m / maxV) * 100);
            return `<div class="srow">
              <span class="srank">${i + 1}</span>
              <span class="sname" title="${escapeHtml(d.label)}">${escapeHtml(d.label)}</span>
              <div class="sbar-wrap" aria-hidden="true"><div class="sbar-fill" data-pct="${pct}"></div></div>
              <span class="scnt">${d.count}</span>
              <span class="sval">${fmtBnShort(d.total_m)}</span>
            </div>`;
          }).join("");
        })()}
      </div>
    </div>
    <div>
      <div class="sec-eye">High-frequency contracting authorities</div>
      <h2 class="sec-h">Top buyers by spend</h2>
      <p class="sec-sub" style="margin-bottom:28px">A small cohort of contracting authorities drives a disproportionate share of UK public contract value. These are the most active buyers in the indexed period.</p>
      ${topBuyers.length > 0 ? `
      <div class="buyers-panel">
        <div class="buyers-head-row"><span>Contracting authority</span><span>Notices &middot; Value</span></div>
        ${topBuyers.map((b, i) => `<div class="brow">
          <span class="brank">${i + 1}</span>
          <span class="bname" title="${escapeHtml(b.buyer)}">${escapeHtml(b.buyer)}</span>
          <span class="bcnt">${b.cnt} notices</span>
          <span class="bval">£${b.total_val}m</span>
        </div>`).join("")}
      </div>
      <p style="font-family:var(--mono);font-size:9.5px;color:var(--faint);margin-top:12px;line-height:1.6">Top 5 authorities by awarded value &middot; ${escapeHtml(reportMonthRange)} &middot; Aggregators and frameworks excluded</p>
      ` : `<p style="font-family:var(--mono);font-size:12px;color:var(--faint);padding:40px 0">Buyer data builds once sufficient signals are indexed across desks.</p>`}
    </div>
  </div>
</section>
` : ""}

<section class="brief-sec" aria-labelledby="brief-heading">
  <div class="brief-wrap">
    <div class="brief-intro rv">
      <div class="sec-eye">Market Intelligence Brief &middot; ${escapeHtml(reportDate)}</div>
      <h2 class="sec-h" id="brief-heading">What the UK government is buying right now and what it means for your business</h2>
      <p class="sec-sub">UK public procurement is the most transparent large-scale market in the world. Every major contract is published. Every buyer is named. Every award is a matter of public record. Most companies never look. Here is what the data says.</p>
    </div>

    ${hasData ? `
    <article>

      <div class="brief-art rv">
        <div class="brief-num" aria-hidden="true"><span>01</span>Overview</div>
        <div>
          <div class="brief-tag">Market Snapshot</div>
          <h3 class="brief-h">Here is where UK government money is going right now</h3>
          <div class="brief-body">
            <p>The UK public sector awarded <strong>${fmtBn(totalAnnualM)}</strong> in contracts over the past 12 months — ${totalNotices.toLocaleString()} procurement notices indexed and scored by GovRevenue (2026) in real time from Contracts Finder and Find a Tender. That works out to <strong>${fmtBnShort(avgMonthlyM)} every single month</strong> leaving government and flowing into businesses across every category from construction to digital services.</p>
            <div class="pull"><p>Right now, ${fmtBnShort(openPipelineM)} in contracts are open and accepting bids. These are not historic deals. They are live opportunities with deadlines this week and next month.</p></div>
            <p>Across ${totalOpenCount.toLocaleString()} active tenders, the immediately addressable commercial opportunity in UK public sector procurement is concrete and measurable. GovRevenue scores each notice by sector, value band, and buyer profile every hour — surfacing what matters before the deadline closes.</p>
          </div>
        </div>
      </div>

      <div class="brief-art rv">
        <div class="brief-num" aria-hidden="true"><span>02</span>Momentum</div>
        <div>
          <div class="brief-tag">Momentum Signal</div>
          <h3 class="brief-h">Spend is ${trendPct >= 0 ? "rising" : "contracting"} and that matters more than the headline number</h3>
          <div class="brief-body">
            <p>The directional trend over this period is <strong>${trendPct >= 0 ? "+" : ""}${trendPct}%</strong>, comparing the three-month opening average against the three-month trailing average. ${trendPct > 5 ? "That is a meaningful upswing. In procurement, rising awarded spend is a leading indicator of future open tenders. Frameworks extend, new lots open, and re-let activity accelerates. The time to position is before the volume peaks, not after." : trendPct < -5 ? "Spend contractions often precede consolidation phases where buyers are preparing larger, longer-term frameworks. Firms that map buyer intent during quiet periods are the ones that win when activity returns." : "Stable spend indicates predictable budget cycles and recurring opportunity windows. That rewards firms who plan six months ahead."}</p>
            <div class="pull"><p>Awarded spend peaked at ${fmtBnShort(peakPoint.total_m)} in ${escapeHtml(peakPoint.label)} — ${peakVsAvgPct}% above the period average. Spikes like this signal budget-year end activity, framework renewals, or large multi-lot contracts that break into multiple winnable pieces.</p></div>
          </div>
        </div>
      </div>

      <div class="brief-art rv">
        <div class="brief-num" aria-hidden="true"><span>03</span>Sectors</div>
        <div>
          <div class="brief-tag">Where the Money Is</div>
          <h3 class="brief-h">The sectors dominating UK procurement spend</h3>
          <div class="brief-body">
            <p>Public procurement is not uniformly distributed. Three sectors consistently account for the majority of UK government contract value. ${top3DesksText ? `Over this period, spend was led by ${top3DesksText}.` : topDesk ? `The leading category was <strong>${escapeHtml(topDesk.label)}</strong> at ${fmtBnShort(topDesk.total_m)}.` : ""}</p>
            ${topDesk ? `<p>The leading desk alone — <strong>${escapeHtml(topDesk.label)}</strong> — represented approximately <strong>${topDeskSharePct}% of total tracked spend</strong>. If your firm operates in this category, you are in the highest-volume segment of the market. If you do not, the sector data above shows exactly where adjacent opportunity exists.</p>` : ""}
            <div class="pull"><p>Knowing which sector is growing and which buyer is driving that growth is the difference between chasing tenders and being ready when they drop.</p></div>
          </div>
        </div>
      </div>

      <div class="brief-art rv">
        <div class="brief-num" aria-hidden="true"><span>04</span>Buyers</div>
        <div>
          <div class="brief-tag">The Buyer Map</div>
          <h3 class="brief-h">The contracting authorities spending the most right now</h3>
          <div class="brief-body">
            <p>${topBuyers.length > 0 ? `Not all buyers are equal. <strong>${escapeHtml(topBuyers[0].buyer)}</strong> generated ${topBuyers[0].cnt} procurement notices totalling &pound;${topBuyers[0].total_val}m over this period alone.${topBuyers.length >= 3 ? ` Alongside <strong>${escapeHtml(topBuyers[1].buyer)}</strong> and <strong>${escapeHtml(topBuyers[2].buyer)}</strong>, a compact group of high-frequency buyers drives a disproportionate share of total UK public spend.` : ""}` : "Buyer-level intelligence builds as procurement signals accumulate. Once established, it identifies which contracting authorities are most active in your sector."}</p>
            <p>Buyer behaviour is predictable. A contracting authority that spent heavily this year will re-procure. A buyer that awarded a framework in 2023 is approaching renewal now. Knowing <em>who</em> is buying in your category and when they last procured is the most underutilised competitive advantage in public sector business development.</p>
            <div class="pull"><p>The public record tells you exactly who is spending, how much, and when they will need to buy again. You just need to know where to look.</p></div>
          </div>
        </div>
      </div>

      <div class="brief-art rv">
        <div class="brief-num" aria-hidden="true"><span>05</span>Act</div>
        <div>
          <div class="brief-tag">The Window Right Now</div>
          <h3 class="brief-h">${closing30.toLocaleString()} contracts are closing in the next 30 days</h3>
          <div class="brief-body">
            <p>The near-term window is concrete: <strong>${closing30.toLocaleString()} notices closing within 30 days</strong> and <strong>${closing60.toLocaleString()} within 60 days</strong>. These are live procurement opportunities with published deadlines, buyer contact details, and submission requirements available in public right now.</p>
            <p>GovRevenue scans this data every hour and scores each notice against your company profile — surfacing the ones worth bidding, the buyers worth calling, and the frameworks worth getting onto before the next round closes. The open pipeline is <strong>${fmtBnShort(openPipelineM)} across ${totalOpenCount.toLocaleString()} active tenders</strong>. Your scan takes two minutes.</p>
            <div class="pull"><p>${closing30.toLocaleString()} open contracts. 30 days. The window is concrete and it is closing.</p></div>
          </div>
        </div>
      </div>

    </article>
    ` : `<p style="color:var(--t3);font-style:italic;text-align:center;padding:60px 0">Intelligence data is building. Check back after the first hourly refresh.</p>`}

    <div class="brief-refs">
      <p style="font-family:var(--mono);font-size:10px;color:var(--t3);letter-spacing:.04em;line-height:1.9">
        GovRevenue (2026) <em>UK Procurement Spend Signal — ${escapeHtml(reportMonthRange)}</em>. ${totalNotices.toLocaleString()} notices indexed across 24 sector desks. Available at: govrevenue-agent-production.up.railway.app/charts &middot; Contracts Finder (Crown Commercial Service, 2026) &middot; Find a Tender Service (Cabinet Office, 2026) &middot; National Audit Office (2023) <em>Government&rsquo;s management of its commercial relationships</em> &middot; Arrowsmith, S. (2014) <em>The Law of Public and Utilities Procurement</em>. 3rd ed. London: Sweet &amp; Maxwell.
      </p>
    </div>
  </div>
</section>

<section class="nl-sec" aria-labelledby="nl-heading">
  <div class="nl-wrap">
    <div class="nl-card">
      <div class="nl-eye">Weekly procurement intelligence</div>
      <h2 class="nl-h" id="nl-heading">Get the spend signal before your competitors do</h2>
      <p class="nl-sub">Every week: the contracts opening in your sector, the buyers spending the most, and the frameworks closing soon. Free. No noise.</p>
      <form class="nl-form" action="/form-submit" method="post" aria-label="Newsletter sign-up">
        <input type="hidden" name="_type" value="briefing">
        <input type="hidden" name="_source" value="charts">
        <input class="nl-in" type="email" name="email" placeholder="your@email.com" required autocomplete="email" aria-label="Email address">
        <button type="submit" class="nl-btn">Get the brief</button>
      </form>
      <p class="nl-note">No spam. Unsubscribe any time. Weekly only.</p>
      <div class="nl-proof">
        <div><span class="nl-pv">${totalNotices.toLocaleString()}+</span><span class="nl-pl">Notices tracked</span></div>
        <div><span class="nl-pv">24</span><span class="nl-pl">Sector desks</span></div>
        <div><span class="nl-pv">Hourly</span><span class="nl-pl">Data refresh</span></div>
        <div><span class="nl-pv">${fmtBnShort(openPipelineM)}</span><span class="nl-pl">Open pipeline</span></div>
      </div>
    </div>
  </div>
</section>

</main>

${pageShellFoot()}

<script>
(function(){
  const MD=${JSON.stringify(monthPoints)};
  const WD=${JSON.stringify(weekPoints)};
  const MDK=${JSON.stringify(monthDeskMap)};
  const WDK=${JSON.stringify(weekDeskMap)};
  let cur=MD;
  const cv=document.getElementById('detailChart');
  const tip=document.getElementById('chartTip');
  let mx=null;

  function fmt(v){return v>=1000?'£'+(v/1000).toFixed(2)+'bn':'£'+v.toFixed(0)+'m';}
  function fmts(v){return v>=1000?'£'+(v/1000).toFixed(1)+'bn':'£'+Math.round(v)+'m';}

  function draw(){
    const dpr=window.devicePixelRatio||1;
    const W=cv.parentElement.clientWidth;
    const H=Math.max(340,Math.min(480,W*0.42));
    cv.width=W*dpr;cv.height=H*dpr;
    cv.style.width=W+'px';cv.style.height=H+'px';
    const ctx=cv.getContext('2d');
    ctx.scale(dpr,dpr);ctx.clearRect(0,0,W,H);

    const data=cur;
    if(!data.length)return;
    const pad={t:52,r:24,b:96,l:84};
    const cw=W-pad.l-pad.r, ch=H-pad.t-pad.b;

    const aVals=data.map(d=>d.total_m).filter(v=>v>0);
    const oVals=data.map(d=>d.open_m).filter(v=>v>0);
    const allV=[...aVals,...oVals];
    const yMin=0;
    const yMax=Math.max(...allV)*1.18||10;
    const yRange=yMax-yMin;

    const X=i=>pad.l+(data.length>1?i/(data.length-1):0.5)*cw;
    const Y=v=>pad.t+ch-((v-yMin)/yRange)*ch;

    const yTicks=5;
    for(let i=0;i<=yTicks;i++){
      const v=yMin+(yRange/yTicks)*i;
      const y=Y(v);
      ctx.strokeStyle=i===0?'rgba(255,255,255,.12)':'rgba(255,255,255,.05)';
      ctx.lineWidth=i===0?1.5:1;
      ctx.setLineDash(i===0?[]:[3,3]);
      ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(W-pad.r,y);ctx.stroke();
      ctx.setLineDash([]);
      ctx.font='10.5px "Spline Sans Mono",monospace';
      ctx.fillStyle='#566273';ctx.textAlign='right';
      ctx.fillText(fmts(v),pad.l-10,y+4);
    }

    data.forEach((_,i)=>{
      const x=X(i);
      ctx.strokeStyle='rgba(255,255,255,.03)';ctx.lineWidth=1;ctx.setLineDash([]);
      ctx.beginPath();ctx.moveTo(x,pad.t);ctx.lineTo(x,H-pad.b);ctx.stroke();
    });

    ctx.strokeStyle='rgba(255,255,255,.12)';ctx.lineWidth=1.5;
    ctx.beginPath();ctx.moveTo(pad.l,H-pad.b);ctx.lineTo(W-pad.r,H-pad.b);ctx.stroke();

    ctx.font='10px "Spline Sans Mono",monospace';ctx.fillStyle='#566273';
    data.forEach((d,i)=>{
      const x=X(i);
      ctx.save();ctx.translate(x,H-pad.b+10);ctx.rotate(-Math.PI/4);
      ctx.textAlign='right';ctx.fillText(d.label,0,0);
      ctx.restore();
    });

    ctx.save();ctx.translate(14,H/2);ctx.rotate(-Math.PI/2);
    ctx.font='9.5px "Spline Sans Mono",monospace';ctx.fillStyle='#566273';ctx.textAlign='center';
    ctx.fillText('AWARDED VALUE (£)',0,0);ctx.restore();

    const opData=data.filter(d=>d.open_m>0);
    if(opData.length>=2){
      ctx.strokeStyle='#1d6b4f';ctx.lineWidth=1.8;ctx.setLineDash([5,4]);
      ctx.beginPath();
      let pw=false;
      data.forEach((d,i)=>{if(d.open_m>0){const x=X(i),y=Y(d.open_m);pw?ctx.lineTo(x,y):ctx.moveTo(x,y);pw=true;}else{pw=false;}});
      ctx.stroke();ctx.setLineDash([]);
    }

    ctx.beginPath();
    data.forEach((d,i)=>{i===0?ctx.moveTo(X(i),Y(d.total_m)):ctx.lineTo(X(i),Y(d.total_m));});
    ctx.lineTo(X(data.length-1),H-pad.b);ctx.lineTo(X(0),H-pad.b);ctx.closePath();
    ctx.fillStyle='rgba(180,146,78,0.12)';ctx.fill();

    ctx.strokeStyle='#B4924E';ctx.lineWidth=2.5;
    ctx.beginPath();
    data.forEach((d,i)=>{i===0?ctx.moveTo(X(i),Y(d.total_m)):ctx.lineTo(X(i),Y(d.total_m));});
    ctx.stroke();

    const peakI=data.reduce((pi,d,i)=>d.total_m>data[pi].total_m?i:pi,0);
    const validLow=data.filter(d=>d.total_m>0);
    const troughI=validLow.length?data.indexOf(validLow.reduce((l,d)=>d.total_m<l.total_m?d:l)):-1;

    data.forEach((d,i)=>{
      const x=X(i),y=Y(d.total_m);
      const isPeak=i===peakI,isTrough=i===troughI;
      ctx.beginPath();ctx.arc(x,y,isPeak||isTrough?6:3.5,0,7);
      ctx.fillStyle=isPeak?'#B4924E':isTrough?'#9AA093':'rgba(251,249,243,.9)';ctx.fill();
      ctx.strokeStyle='#B4924E';ctx.lineWidth=2;ctx.stroke();
      if(isPeak){
        ctx.font='500 10px "Spline Sans Mono",monospace';ctx.fillStyle='#B4924E';ctx.textAlign='center';
        ctx.fillText(fmts(d.total_m)+'+',x,y-12);
        ctx.font='bold 9px "Spline Sans Mono",monospace';
        ctx.fillText('▲ PEAK',x,y-24);
      }
      if(isTrough&&troughI!==peakI){
        ctx.fillStyle='#86897E';ctx.font='9px "Spline Sans Mono",monospace';ctx.textAlign='center';
        ctx.fillText('▼ LOW',x,y+20);
      }
    });

    data.forEach((d,i)=>{
      if(d.open_m<=0)return;
      ctx.beginPath();ctx.arc(X(i),Y(d.open_m),3,0,7);
      ctx.fillStyle='rgba(251,249,243,.9)';ctx.fill();
      ctx.strokeStyle='#1d6b4f';ctx.lineWidth=1.5;ctx.stroke();
    });

    if(mx!==null){
      const nearI=Math.round((mx-pad.l)/(cw||1)*(data.length-1));
      if(nearI>=0&&nearI<data.length){
        const hx=X(nearI);
        ctx.strokeStyle='rgba(27,30,25,.25)';ctx.lineWidth=1;ctx.setLineDash([4,3]);
        ctx.beginPath();ctx.moveTo(hx,pad.t);ctx.lineTo(hx,H-pad.b);ctx.stroke();
        ctx.setLineDash([]);
        const d=data[nearI];
        const delta=nearI>0?d.total_m-data[nearI-1].total_m:null;
        const dpct=delta&&data[nearI-1].total_m>0?Math.round(delta/data[nearI-1].total_m*100):null;
        tip.style.display='block';
        const tipLeft=hx>W*0.65?hx-222:hx+12;
        tip.style.left=tipLeft+'px';tip.style.top=pad.t+'px';
        const deskMap=cur===MD?MDK:WDK;
        const allDesks=deskMap[d.label]||[];
        const desks=allDesks.slice(0,5);
        const totalDesks=allDesks.length;
        const maxDm=desks.length?desks[0].total_m:1;
        const deskRows=desks.map(dk=>{
          const pct=Math.round((dk.total_m/maxDm)*100);
          const short=dk.label.length>22?dk.label.slice(0,21)+'…':dk.label;
          return '<div style="margin-top:6px">'
            +'<div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:2px">'
            +'<span style="font-size:9.5px;color:#8893A4;white-space:nowrap;overflow:hidden;max-width:130px;display:inline-block;text-overflow:ellipsis">'+short+'</span>'
            +'<span style="font-size:9.5px;color:#B0BAC8;margin-left:8px;white-space:nowrap">'+fmts(dk.total_m)+'+'+'</span>'
            +'</div>'
            +'<div style="height:3px;background:rgba(27,30,25,.10);border-radius:2px">'
            +'<div style="width:'+pct+'%;height:100%;background:#B4924E;border-radius:2px;opacity:.85"></div>'
            +'</div></div>';
        }).join('');
        tip.innerHTML='<div class="tip-label">'+d.label+'</div>'
          +'<div class="tip-row"><span class="tip-dot" style="background:#B4924E"></span>Awarded &nbsp;<b>'+fmt(d.total_m)+'+'+'</b>'+(dpct!==null?' <span style="opacity:.7;font-size:10px">'+(dpct>=0?'+':'')+dpct+'%</span>':'')+'</div>'
          +(d.open_m>0?'<div class="tip-row"><span class="tip-dot" style="background:#1d6b4f"></span>Open &nbsp;&nbsp;&nbsp;&nbsp;<b>'+fmt(d.open_m)+'+'+'</b></div>':'')
          +'<div style="margin-top:6px;padding-top:6px;border-top:1px solid rgba(255,255,255,.1);font-size:10px;color:#566273">'+d.notice_count+' notices &middot; '+d.open_count+' open</div>'
          +(desks.length?'<div style="margin-top:8px;padding-top:8px;border-top:1px solid rgba(236,230,214,.2)">'
            +'<div style="font-size:9px;letter-spacing:.1em;text-transform:uppercase;color:#86897E;margin-bottom:4px">Top 5 desks'+(totalDesks>5?' of '+totalDesks:'')+'</div>'
            +deskRows+'</div>':'');
      }
    }else{tip.style.display='none';}
  }

  cv.addEventListener('mousemove',e=>{const r=cv.getBoundingClientRect();mx=e.clientX-r.left;draw();});
  cv.addEventListener('mouseleave',()=>{mx=null;draw();});

  document.getElementById('tog-month').addEventListener('click',function(){cur=MD;this.classList.add('tog-active');document.getElementById('tog-week').classList.remove('tog-active');draw();});
  document.getElementById('tog-week').addEventListener('click',function(){cur=WD;this.classList.add('tog-active');document.getElementById('tog-month').classList.remove('tog-active');draw();});

  new ResizeObserver(draw).observe(cv.parentElement);
  draw();

  const bars=document.querySelectorAll('.sbar-fill');
  if(bars.length){
    const bo=new IntersectionObserver(es=>es.forEach(e=>{
      if(e.isIntersecting){e.target.style.width=e.target.dataset.pct+'%';bo.unobserve(e.target);}
    }),{threshold:.1});
    bars.forEach(b=>bo.observe(b));
  }

  const rv=document.querySelectorAll('.rv');
  const ro=new IntersectionObserver(es=>es.forEach(e=>{
    if(e.isIntersecting){e.target.classList.add('in');ro.unobserve(e.target);}
  }),{threshold:.04,rootMargin:'0px 0px 80px 0px'});
  rv.forEach(el=>{
    const rect=el.getBoundingClientRect();
    if(rect.top<window.innerHeight)el.classList.add('in');
    else ro.observe(el);
  });
})();
</script>
</body>
</html>`);

}));

app.get("/charts/embed", asyncRoute(async (req, res) => {
  type EmbedPoint = { label: string; total_m: number; open_m: number; notice_count: number; open_count: number };
  type EmbedDeskRow = { mlabel: string; category: string; total_m: number };
  const OCAP = 2_000_000_000;
  let mp: EmbedPoint[] = [], wp: EmbedPoint[] = [];
  let mdkMap: Record<string, { label: string; total_m: number }[]> = {};
  let wdkMap: Record<string, { label: string; total_m: number }[]> = {};
  if (pool) {
    const [mR, wR, mdR, wdR] = await Promise.all([
      pool.query<EmbedPoint>(`
        SELECT to_char(date_trunc('month', notice_date), 'Mon ''YY') AS label,
               ROUND(SUM(value_amount) FILTER (WHERE value_amount>0 AND value_amount<${OCAP})/1e6::numeric,2)::float AS total_m,
               ROUND(COALESCE(SUM(value_amount) FILTER (WHERE (LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%') AND value_amount>0 AND value_amount<${OCAP}),0)/1e6::numeric,2)::float AS open_m,
               COUNT(*)::int AS notice_count,
               COUNT(*) FILTER (WHERE LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')::int AS open_count
        FROM homepage_signals WHERE notice_date>NOW()-INTERVAL '13 months' AND notice_date<=NOW() AND notice_date IS NOT NULL
        GROUP BY date_trunc('month',notice_date) ORDER BY date_trunc('month',notice_date)`),
      pool.query<EmbedPoint>(`
        SELECT to_char(date_trunc('week', notice_date), 'DD Mon') AS label,
               ROUND(SUM(value_amount) FILTER (WHERE value_amount>0 AND value_amount<${OCAP})/1e6::numeric,2)::float AS total_m,
               ROUND(COALESCE(SUM(value_amount) FILTER (WHERE (LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%') AND value_amount>0 AND value_amount<${OCAP}),0)/1e6::numeric,2)::float AS open_m,
               COUNT(*)::int AS notice_count,
               COUNT(*) FILTER (WHERE LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')::int AS open_count
        FROM homepage_signals WHERE notice_date>NOW()-INTERVAL '14 weeks' AND notice_date<=NOW() AND notice_date IS NOT NULL
        GROUP BY date_trunc('week',notice_date) ORDER BY date_trunc('week',notice_date)`),
      pool.query<EmbedDeskRow>(`
        SELECT to_char(date_trunc('month',notice_date),'Mon ''YY') AS mlabel, category,
               ROUND(SUM(value_amount) FILTER (WHERE value_amount>0 AND value_amount<${OCAP})/1e6::numeric,1)::float AS total_m
        FROM homepage_signals WHERE notice_date>NOW()-INTERVAL '13 months' AND notice_date IS NOT NULL
        GROUP BY date_trunc('month',notice_date),category ORDER BY date_trunc('month',notice_date),SUM(value_amount) DESC NULLS LAST`),
      pool.query<EmbedDeskRow>(`
        SELECT to_char(date_trunc('week',notice_date),'DD Mon') AS mlabel, category,
               ROUND(SUM(value_amount) FILTER (WHERE value_amount>0 AND value_amount<${OCAP})/1e6::numeric,1)::float AS total_m
        FROM homepage_signals WHERE notice_date>NOW()-INTERVAL '14 weeks' AND notice_date IS NOT NULL
        GROUP BY date_trunc('week',notice_date),category ORDER BY date_trunc('week',notice_date),SUM(value_amount) DESC NULLS LAST`),
    ]);
    mp = mR.rows.map(r => ({ ...r, total_m: r.total_m||0, open_m: r.open_m||0 }));
    wp = wR.rows.map(r => ({ ...r, total_m: r.total_m||0, open_m: r.open_m||0 }));
    for (const r of mdR.rows) { if (!r.total_m||r.total_m<=0) continue; const lbl=DESK_PROFILES.find(d=>d.slug===r.category)?.label||r.category; (mdkMap[r.mlabel]=mdkMap[r.mlabel]||[]).push({label:lbl,total_m:r.total_m}); }
    for (const r of wdR.rows) { if (!r.total_m||r.total_m<=0) continue; const lbl=DESK_PROFILES.find(d=>d.slug===r.category)?.label||r.category; (wdkMap[r.mlabel]=wdkMap[r.mlabel]||[]).push({label:lbl,total_m:r.total_m}); }
  }
  res.type("html").send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
:root{--paper:#FBF9F3;--paper-2:#F6F2E8;--ink:#1B1E19;--accent:#B4924E;--slate:#86897E;--border:rgba(27,30,25,.12);--border-2:rgba(27,30,25,.18);--mono:"Spline Sans Mono","SF Mono",ui-monospace,monospace}
*{box-sizing:border-box;margin:0;padding:0}
html{background:var(--paper-2)}
body{background:var(--paper-2);overflow:hidden;padding-bottom:12px}
.wrap{padding:10px 16px 0}
.toolbar{display:flex;align-items:center;justify-content:space-between;margin-bottom:8px}
.legend{display:flex;gap:14px;align-items:center}
.leg{display:flex;align-items:center;gap:5px;font-family:var(--mono);font-size:10px;color:var(--slate)}
.leg-l{width:16px;height:2px;background:var(--accent)}
.leg-l.g{background:transparent;border-bottom:2px dashed #1d6b4f;}
.tog-group{display:flex;border:1px solid var(--border-2)}
.tog{font-family:var(--mono);font-size:10px;letter-spacing:.07em;text-transform:uppercase;padding:5px 12px;background:var(--paper-2);border:none;cursor:pointer;color:var(--slate)}
.tog.active{background:#102A1E;color:#ECE6D6}
.cv-wrap{position:relative}
canvas{display:block;width:100%;background:var(--paper-2)}
.tip{position:absolute;background:#102A1E;color:#ECE6D6;padding:10px 14px;font-family:var(--mono);font-size:11px;pointer-events:none;display:none;z-index:10;width:210px}
.tip-lbl{font-size:12px;font-weight:600;color:#ECE6D6;margin-bottom:5px;text-transform:uppercase;letter-spacing:.06em}
.tip-row{display:flex;align-items:center;gap:6px;margin-top:2px}
.tip-dot{width:7px;height:7px;border-radius:50%;flex-shrink:0}
</style>
</head>
<body>
<div class="wrap">
  <div class="toolbar">
    <div class="legend">
      <div class="leg"><div class="leg-l"></div>Awarded</div>
      <div class="leg"><div class="leg-l g"></div>Open pipeline</div>
    </div>
    <div class="tog-group">
      <button class="tog active" id="t-mo">12 months</button>
      <button class="tog" id="t-wk">12 weeks</button>
    </div>
  </div>
  <div class="cv-wrap">
    <canvas id="ec"></canvas>
    <div class="tip" id="et"></div>
  </div>
</div>
<script>
(function(){
  const MD=${JSON.stringify(mp)};
  const WD=${JSON.stringify(wp)};
  const MDK=${JSON.stringify(mdkMap)};
  const WDK=${JSON.stringify(wdkMap)};
  let cur=MD;
  const cv=document.getElementById('ec');
  const tip=document.getElementById('et');
  let mx=null;
  function fmt(v){return v>=1000?'£'+(v/1000).toFixed(2)+'bn+':'£'+v.toFixed(0)+'m+';}
  function fmts(v){return v>=1000?'£'+(v/1000).toFixed(1)+'bn+':'£'+Math.round(v)+'m+';}
  function draw(){
    const dpr=window.devicePixelRatio||1;
    const W=cv.parentElement.clientWidth;
    const H=300;
    cv.width=W*dpr;cv.height=H*dpr;
    cv.style.width=W+'px';cv.style.height=H+'px';
    const ctx=cv.getContext('2d');
    ctx.scale(dpr,dpr);ctx.clearRect(0,0,W,H);
    const data=cur;if(!data.length)return;
    const pad={t:36,r:16,b:72,l:72};
    const cw=W-pad.l-pad.r,ch=H-pad.t-pad.b;
    const aVals=data.map(d=>d.total_m).filter(v=>v>0);
    const oVals=data.map(d=>d.open_m).filter(v=>v>0);
    const allV=[...aVals,...oVals];
    const yMax=Math.max(...allV)*1.18||10;
    const X=i=>pad.l+(data.length>1?i/(data.length-1):0.5)*cw;
    const Y=v=>pad.t+ch-(v/yMax)*ch;
    // Y gridlines
    for(let i=0;i<=4;i++){
      const v=(yMax/4)*i;const y=Y(v);
      ctx.strokeStyle=i===0?'#ccc':'#e8e2d8';ctx.lineWidth=i===0?1.5:1;ctx.setLineDash(i===0?[]:[3,3]);
      ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(W-pad.r,y);ctx.stroke();ctx.setLineDash([]);
      ctx.font='9.5px "Spline Sans Mono",monospace';ctx.fillStyle='#86897E';ctx.textAlign='right';
      ctx.fillText(v>=1000?'£'+(v/1000).toFixed(1)+'bn':'£'+Math.round(v)+'m',pad.l-6,y+3);
    }
    // X labels
    ctx.font='9px "Spline Sans Mono",monospace';ctx.fillStyle='#86897E';
    data.forEach((d,i)=>{const x=X(i);ctx.save();ctx.translate(x,H-pad.b+8);ctx.rotate(-Math.PI/4);ctx.textAlign='right';ctx.fillText(d.label,0,0);ctx.restore();});
    // Area fill (drawn first so everything else sits on top)
    ctx.beginPath();data.forEach((d,i)=>{i===0?ctx.moveTo(X(i),Y(d.total_m)):ctx.lineTo(X(i),Y(d.total_m));});
    ctx.lineTo(X(data.length-1),H-pad.b);ctx.lineTo(X(0),H-pad.b);ctx.closePath();ctx.fillStyle='rgba(180,146,78,0.08)';ctx.fill();
    // Awarded line
    ctx.strokeStyle='#B4924E';ctx.lineWidth=2;ctx.beginPath();data.forEach((d,i)=>{i===0?ctx.moveTo(X(i),Y(d.total_m)):ctx.lineTo(X(i),Y(d.total_m));});ctx.stroke();
    // Open pipeline dashed (drawn on top of area fill so it's always visible)
    const opData=data.filter(d=>d.open_m>0);
    if(opData.length>=2){ctx.strokeStyle='#1d6b4f';ctx.lineWidth=1.5;ctx.setLineDash([5,4]);ctx.beginPath();let pw=false;data.forEach((d,i)=>{if(d.open_m>0){const x=X(i),y=Y(d.open_m);pw?ctx.lineTo(x,y):ctx.moveTo(x,y);pw=true;}else{pw=false;}});ctx.stroke();ctx.setLineDash([]);}
    // Dots + peak marker
    const peakI=data.reduce((pi,d,i)=>d.total_m>data[pi].total_m?i:pi,0);
    data.forEach((d,i)=>{
      const x=X(i),y=Y(d.total_m),isPeak=i===peakI;
      ctx.beginPath();ctx.arc(x,y,isPeak?5:3,0,Math.PI*2);ctx.fillStyle=isPeak?'#B4924E':'#FBF9F3';ctx.fill();ctx.strokeStyle='#B4924E';ctx.lineWidth=1.8;ctx.stroke();
      if(isPeak){ctx.font='600 9px "Spline Sans Mono",monospace';ctx.fillStyle='#B4924E';ctx.textAlign='center';ctx.fillText(fmts(d.total_m),x,y-10);ctx.font='600 8px "Spline Sans Mono",monospace';ctx.fillText('▲ PEAK',x,y-20);}
    });
    // Hover tooltip
    if(mx!==null){
      const ni=Math.round((mx-pad.l)/(cw||1)*(data.length-1));
      if(ni>=0&&ni<data.length){
        const hx=X(ni);
        ctx.strokeStyle='rgba(11,15,20,.3)';ctx.lineWidth=1;ctx.setLineDash([4,3]);
        ctx.beginPath();ctx.moveTo(hx,pad.t);ctx.lineTo(hx,H-pad.b);ctx.stroke();ctx.setLineDash([]);
        const d=data[ni];
        const delta=ni>0?d.total_m-data[ni-1].total_m:null;
        const dpct=delta&&data[ni-1].total_m>0?Math.round(delta/data[ni-1].total_m*100):null;
        tip.style.display='block';
        tip.style.left=(hx>W*0.65?hx-222:hx+10)+'px';tip.style.top=pad.t+'px';
        const deskMap=cur===MD?MDK:WDK;
        const desks=(deskMap[d.label]||[]).slice(0,5);
        const maxDm=desks.length?desks[0].total_m:1;
        const deskRows=desks.map(dk=>{
          const pct=Math.round((dk.total_m/maxDm)*100);
          const short=dk.label.length>22?dk.label.slice(0,21)+'…':dk.label;
          return '<div style="margin-top:5px"><div style="display:flex;justify-content:space-between;margin-bottom:2px"><span style="font-size:9.5px;color:#9AA093;overflow:hidden;max-width:128px;display:inline-block;text-overflow:ellipsis;white-space:nowrap">'+short+'</span><span style="font-size:9.5px;color:#ECE6D6;margin-left:6px;white-space:nowrap">'+fmts(dk.total_m)+'</span></div><div style="height:3px;background:rgba(236,230,214,.2);border-radius:2px"><div style="width:'+pct+'%;height:100%;background:#B4924E;border-radius:2px;opacity:.85"></div></div></div>';
        }).join('');
        tip.innerHTML='<div class="tip-lbl">'+d.label+'</div>'
          +'<div class="tip-row"><span class="tip-dot" style="background:#B4924E"></span>Awarded &nbsp;<b>'+fmt(d.total_m)+'</b>'+(dpct!==null?' <span style="opacity:.7;font-size:10px">'+(dpct>=0?'+':'')+dpct+'%</span>':'')+'</div>'
          +(d.open_m>0?'<div class="tip-row"><span class="tip-dot" style="background:#14532d"></span>Open &nbsp;&nbsp;&nbsp;&nbsp;<b>'+fmt(d.open_m)+'</b></div>':'')
          +'<div style="margin-top:5px;padding-top:5px;border-top:1px solid rgba(236,230,214,.2);font-size:10px;color:#9AA093">'+d.notice_count+' notices &middot; '+d.open_count+' open</div>'
          +(desks.length?'<div style="margin-top:7px;padding-top:7px;border-top:1px solid rgba(236,230,214,.2)"><div style="font-size:9px;letter-spacing:.1em;text-transform:uppercase;color:#9AA093;margin-bottom:2px">Top 5 desks'+(desks.length<(deskMap[d.label]||[]).length?' of '+(deskMap[d.label]||[]).length:'')+'</div>'+deskRows+'</div>':'');
      }
    }else{tip.style.display='none';}
  }
  cv.addEventListener('mousemove',e=>{const r=cv.getBoundingClientRect();mx=e.clientX-r.left;draw();});
  cv.addEventListener('mouseleave',()=>{mx=null;draw();});
  document.getElementById('t-mo').addEventListener('click',function(){cur=MD;this.classList.add('active');document.getElementById('t-wk').classList.remove('active');draw();});
  document.getElementById('t-wk').addEventListener('click',function(){cur=WD;this.classList.add('active');document.getElementById('t-mo').classList.remove('active');draw();});
  new ResizeObserver(draw).observe(cv.parentElement);
  draw();
})();
</script>
</body>
</html>`);
}));

app.get("/desks", asyncRoute(async (req, res) => {
  const page = Math.max(1, parseInt(String(req.query.page ?? "1"), 10) || 1);
  const entries = await Promise.all(
    DESK_PROFILES.filter(d => d.live).map(async profile => ({
      profile,
      cached: await getDeskCache(profile.slug).catch(() => null),
    }))
  );
  res.type("html").send(desksPage(entries, page, getAuthUser(req)));
}));

app.get("/desk/:slug", asyncRoute(async (req, res) => {
  const profile = DESK_PROFILES.find(d => d.slug === req.params.slug);
  if (!profile) { res.status(404).type("html").send(notFoundHtml("Desk not found", getAuthUser(req))); return; }

  const cached = await getDeskCache(profile.slug).catch(() => null);
  const isStale = !cached || (Date.now() - new Date(cached.cached_at).getTime() > DESK_CACHE_TTL_MS);

  if (isStale) {
    // fire-and-forget; if cold the page renders the compiling state
    compileDeskInBackground(profile).catch(err => captureError(err, { desk: { slug: profile.slug } }));
  }

  res.type("html").send(deskPage(profile, cached, getAuthUser(req)));
}));

app.get("/desk/:slug/sub/:sub", asyncRoute(async (req, res) => {
  const profile = DESK_PROFILES.find(d => d.slug === req.params.slug);
  if (!profile) { res.status(404).type("html").send(notFoundHtml("Desk not found", getAuthUser(req))); return; }

  let matchCat: DeskCategory | null = null;
  let matchSub: string | null = null;
  for (const cat of profile.categories) {
    for (const s of cat.subcategories) {
      if (slugify(s) === req.params.sub) { matchCat = cat; matchSub = s; break; }
    }
    if (matchCat) break;
  }
  if (!matchCat || !matchSub) { res.status(404).type("html").send(notFoundHtml("Subcategory not found", getAuthUser(req))); return; }

  const cached = await getDeskCache(profile.slug).catch(() => null);
  const isStale = !cached || (Date.now() - new Date(cached.cached_at).getTime() > DESK_CACHE_TTL_MS);
  if (isStale) {
    compileDeskInBackground(profile).catch(err => captureError(err, { desk: { slug: profile.slug } }));
  }

  res.type("html").send(subPage(profile, matchCat, matchSub, cached, getAuthUser(req)));
}));

app.get("/desk/:slug/notices", asyncRoute(async (req, res) => {
  const profile = DESK_PROFILES.find(d => d.slug === req.params.slug);
  if (!profile) { res.status(404).type("html").send(notFoundHtml("Desk not found", getAuthUser(req))); return; }
  const cached = await getDeskCache(profile.slug).catch(() => null);
  const isStale = !cached || (Date.now() - new Date(cached.cached_at).getTime() > DESK_CACHE_TTL_MS);
  if (isStale) compileDeskInBackground(profile).catch(err => captureError(err, { desk: { slug: profile.slug } }));
  const buyerFilter = typeof req.query.buyer === "string" ? req.query.buyer : null;
  res.type("html").send(noticesPage(profile, cached, buyerFilter, getAuthUser(req)));
}));

app.get("/desk/:slug/buyers", asyncRoute(async (req, res) => {
  const profile = DESK_PROFILES.find(d => d.slug === req.params.slug);
  if (!profile) { res.status(404).type("html").send(notFoundHtml("Desk not found", getAuthUser(req))); return; }
  const cached = await getDeskCache(profile.slug).catch(() => null);
  const isStale = !cached || (Date.now() - new Date(cached.cached_at).getTime() > DESK_CACHE_TTL_MS);
  if (isStale) compileDeskInBackground(profile).catch(err => captureError(err, { desk: { slug: profile.slug } }));
  res.type("html").send(buyersPage(profile, cached, getAuthUser(req)));
}));

app.get("/scan/:id/compare", asyncRoute(async (req, res) => {
  const scan = await getScan(req.params.id);
  if (!scan || scan.status !== "completed") {
    res.status(404).type("html").send(notFoundHtml("Scan not found or not completed", getAuthUser(req)));
    return;
  }
  const prior = (await getScansByCompany(scan.company_name, scan.id))[0] || null;
  res.type("html").send(comparePage(scan, prior));
}));

// ─── Desk page helpers ────────────────────────────────────────────────────────

type InferredCategory = { label: string; count: number; value: number; subcategories: string[]; latestDate: number };

function timeAgo(dateStr: string | null | undefined): string {
  if (!dateStr) return "—";
  const ms = Date.now() - new Date(dateStr).getTime();
  const h = Math.floor(ms / 3_600_000);
  if (h < 1) return "< 1h ago";
  if (h < 24) return `${h}h ago`;
  const d = Math.floor(h / 24);
  if (d > 90) return new Date(dateStr).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" });
  return `${d}d ago`;
}

function buyerOrgType(buyer: string): string {
  const b = buyer.toLowerCase();
  if (b.includes("nhs") || b.includes("hospital") || b.includes("clinical commissioning") || b.includes("integrated care")) return "HEALTH";
  if (b.includes("city council") || b.includes("borough council") || b.includes("district council") || b.includes("county council") || b.includes("metropolitan borough") || b.includes("london borough") || b.includes("unitary authority")) return "LOCAL AUTHORITY";
  if (b.includes("ministry of") || b.includes("department for") || b.includes("department of") || b.includes("cabinet office") || b.includes("home office") || b.includes("treasury") || b.includes("hmrc") || b.includes("dvla") || b.includes("highways england")) return "CENTRAL GOV";
  if (b.includes("housing association") || b.includes("homes england") || b.includes("housing trust") || b.includes("registered provider")) return "HOUSING";
  if (b.includes("university") || b.includes("college") || b.includes(" school") || b.includes("academy trust")) return "EDUCATION";
  return "";
}

function buyerInitials(buyer: string): string {
  if (/^nhs /i.test(buyer)) return "NHS";
  const stop = new Set(["the","a","of","and","for","&","in","on","at","to"]);
  const words = buyer.split(/[\s,]+/).filter(w => w.length > 1 && !stop.has(w.toLowerCase()));
  return words.slice(0, 3).map(w => w[0].toUpperCase()).join("") || buyer.slice(0, 2).toUpperCase();
}

function getCategoryIcon(label: string): string {
  const l = label.toLowerCase();
  const s = (d: string) => `<svg class="dm-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="${d}"/></svg>`;
  if (l.includes("repair") || l.includes("maintenance") || l.includes("void")) return s("M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.77-3.77a6 6 0 0 1-7.94 7.94l-6.91 6.91a2.12 2.12 0 0 1-3-3l6.91-6.91a6 6 0 0 1 7.94-7.94l-3.76 3.76z");
  if (l.includes("refurb") || l.includes("fit-out") || l.includes("fitout")) return s("M4 20h16M8 20V8m8 12V8m-4 12V4");
  if (l.includes("roof") || l.includes("window") || l.includes("fabric")) return s("M3 21V12l9-9 9 9v9H3zm6 0v-7h6v7");
  if (l.includes("m&e") || l.includes("mechanical") || l.includes("electrical") || l.includes("plumbing")) return s("M13 2L3 14h9l-1 8 10-12h-9z");
  if (l.includes("fire") || l.includes("compliance") || l.includes("remediation")) return s("M12 2l9 4.9V12c0 5.5-3.8 10.7-9 12-5.2-1.3-9-6.5-9-12V6.9zM9 12l2 2 4-4");
  if (l.includes("decarb") || l.includes("retrofit") || l.includes("energy efficiency") || l.includes("net zero")) return s("M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9z");
  if (l.includes("ground") || l.includes("civil") || l.includes("external")) return s("M12 22V12M12 12l-4-4M12 12l4-4M3 20h18M5 20V10l7-7 7 7v10");
  if (l.includes("suppl") || l.includes("material") || l.includes("hire")) return s("M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16zM3.27 6.96 12 12l8.73-5.04M12 22.08V12");
  if (l.includes("school") || l.includes("building") || l.includes("estate")) return s("M3 21V12l9-9 9 9v9H3zm6 0v-5h6v5");
  if (l.includes("technology") || l.includes("digital")) return s("M9 3H5a2 2 0 0 0-2 2v4m6-6h10a2 2 0 0 1 2 2v4M9 3v18m0 0h10a2 2 0 0 0 2-2V9M9 21H5a2 2 0 0 1-2-2V9m0 0h18");
  if (l.includes("training") || l.includes("skill")) return s("M12 2l9 4.9V12c0 5.5-3.8 10.7-9 12-5.2-1.3-9-6.5-9-12V6.9z");
  if (l.includes("send") || l.includes("alternative")) return s("M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2m22-2v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75M13 7a4 4 0 1 1-8 0 4 4 0 0 1 8 0");
  if (l.includes("further") || l.includes("higher")) return s("M22 10v6m0 0-3-3m3 3 3-3M2 9l10-5 10 5-10 5zM6 12.5v5");
  if (l.includes("hard fm") || l.includes("soft fm") || l.includes("managed")) return s("M20 7H4a2 2 0 0 0-2 2v10a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2V9a2 2 0 0 0-2-2zM16 7V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v2");
  if (l.includes("construction framework") || l.includes("framework")) return s("M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5");
  return s("M20 7H4a2 2 0 0 0-2 2v10a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2V9a2 2 0 0 0-2-2zM16 7V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v2");
}

function inferDeskCategories(notices: ProcurementNotice[], categories: DeskCategory[]): InferredCategory[] {
  const result: InferredCategory[] = categories.map(c => ({ label: c.label, count: 0, value: 0, subcategories: c.subcategories, latestDate: 0 }));
  for (const notice of notices) {
    const text = (notice.title + " " + notice.description).toLowerCase();
    for (let i = 0; i < categories.length; i++) {
      if (categories[i].keywords.some(kw => text.includes(kw))) {
        result[i].count++;
        result[i].value += notice.awardedValue ?? 0;
        const d = new Date(notice.publishedDate || notice.awardedDate || "").getTime();
        if (!isNaN(d) && d > result[i].latestDate) result[i].latestDate = d;
        break;
      }
    }
  }
  return result;
}

function deskPage(profile: DeskProfile, cached: { data: ProcurementData; cached_at: string } | null, authCtx?: { email: string; tier: UserTier } | null): string {
  const isCompiling = cached === null;
  const data = cached?.data;

  const deskKeywords = profile.categories.flatMap(c => c.keywords);
  const cutoff365 = Date.now() - 365 * 24 * 3_600_000;
  const allOpen = dedupeNoticesSoft(
    (data?.contractsFinder.open || []).concat(data?.findTender?.notices || [])
  ).sort((a, b) => {
    const da = new Date(a.publishedDate || a.awardedDate || 0).getTime();
    const db = new Date(b.publishedDate || b.awardedDate || 0).getTime();
    return db - da;
  });
  const allMatchingOpen = allOpen.filter(n => {
    const t = new Date(n.publishedDate || n.awardedDate || 0).getTime();
    if (t <= cutoff365) return false;
    if (isOverseasNotice(n.title, n.buyer || "")) return false;
    const title = n.title.toLowerCase();
    return deskKeywords.some(kw => title.includes(kw));
  });
  const openNoticeCount = allMatchingOpen.length;
  const openNotices = allMatchingOpen.slice(0, 6);
  const awardedNotices = data?.contractsFinder.awarded || [];

  const awardedCount = awardedNotices.length;
  const uniqueBuyerCount = new Set(awardedNotices.map(n => n.buyer).filter(b => b && b !== "Not stated")).size;

  const demandCategories = profile.live && !isCompiling
    ? inferDeskCategories(awardedNotices, profile.categories)
    : [];

  // Self-calibrating outlier threshold (see computeOutlierThreshold in lib/intel.ts):
  // excludes magnitude data errors (academy at £18bn) without capping real large contracts.
  const noticeOutlierThreshold = computeOutlierThreshold(awardedNotices.map(n => n.awardedValue ?? 0));

  // Exclude data-error outliers (magnitude errors like £18bn for an academy) from totals
  const validAwardedNotices = awardedNotices.filter(n => (n.awardedValue ?? 0) <= noticeOutlierThreshold);
  const totalAwarded = validAwardedNotices.reduce((s, n) => s + (n.awardedValue ?? 0), 0);

  // Buyer map: aggregate awarded value + open notice count per buyer
  const buyerMap = new Map<string, { awardedValue: number; awardedCount: number; openCount: number }>();
  for (const n of awardedNotices) {
    if (!n.buyer || n.buyer === "Not stated") continue;
    const noticeValue = n.awardedValue ?? 0;
    if (noticeValue > noticeOutlierThreshold) continue; // skip data-error outlier, not a cap
    const e = buyerMap.get(n.buyer) || { awardedValue: 0, awardedCount: 0, openCount: 0 };
    e.awardedCount++;
    e.awardedValue += noticeValue;
    buyerMap.set(n.buyer, e);
  }
  for (const n of allOpen) {
    if (!n.buyer || n.buyer === "Not stated") continue;
    const e = buyerMap.get(n.buyer) || { awardedValue: 0, awardedCount: 0, openCount: 0 };
    e.openCount++;
    buyerMap.set(n.buyer, e);
  }
  const topBuyers = [...buyerMap.entries()]
    .filter(([buyer]) => !isAggregatorBuyer(buyer))
    .sort((a, b) => b[1].awardedValue - a[1].awardedValue)
    .slice(0, 5);

  const fmtBig = (v: number) => v >= 1_000_000_000
    ? `£${(v / 1_000_000_000).toFixed(2)}bn`
    : `£${(v / 1_000_000).toFixed(2)}m`;

  const fmtShort = (v: number) => v >= 1e9 ? `£${(v / 1e9).toFixed(1)}bn`
    : v >= 1e6 ? `£${(v / 1e6).toFixed(0)}m`
    : v >= 1e3 ? `£${Math.round(v / 1e3)}k`
    : `£${Math.round(v)}`;

  const topCats = profile.live && !isCompiling
    ? [...demandCategories].sort((a, b) => b.value - a.value).slice(0, 5).filter(c => c.value > 0)
    : [];
  const maxCatVal = topCats[0]?.value || 1;

  // ── Market intelligence data ─────────────────────────────────────────────
  const nowMs = Date.now();

  const awardedCountWithValue = validAwardedNotices.filter(n => (n.awardedValue ?? 0) > 0).length;
  const avgContractVal = awardedCountWithValue > 0 ? totalAwarded / awardedCountWithValue : 0;

  const buyersThisMonth = new Set(
    allOpen.filter(n => {
      if (nowMs - new Date(n.publishedDate || 0).getTime() > 30 * 24 * 3_600_000) return false;
      return deskKeywords.some(kw => n.title.toLowerCase().includes(kw));
    }).map(n => n.buyer).filter(b => b && !isAggregatorBuyer(b))
  ).size;

  const closingSoonRawCount = allOpen.filter(n => {
    if (!n.deadlineDate) return false;
    const d = new Date(n.deadlineDate).getTime();
    return d > nowMs && d <= nowMs + 7 * 24 * 3_600_000 &&
      deskKeywords.some(kw => n.title.toLowerCase().includes(kw));
  }).length;

  // Monthly spend trend (last 12 months, from awardedNotices, no future dates)
  const monthlySpend = new Map<string, number>();
  for (const n of awardedNotices) {
    const d = n.awardedDate ? new Date(n.awardedDate) : (n.publishedDate ? new Date(n.publishedDate) : null);
    if (!d || d.getTime() < cutoff365 || d.getTime() > nowMs) continue;
    const mk = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
    monthlySpend.set(mk, (monthlySpend.get(mk) || 0) + (n.awardedValue || 0));
  }
  const trendData = [...monthlySpend.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  const maxMonthVal = Math.max(...trendData.map(([, v]) => v), 1);

  // Recent awards (keyword-matched, last 90 days, no future dates)
  const cutoff90ms = nowMs - 90 * 24 * 3_600_000;
  const recentAwards = awardedNotices.filter(n => {
    const t = new Date(n.awardedDate || n.publishedDate || 0).getTime();
    if (t < cutoff90ms || t > nowMs) return false;
    return deskKeywords.some(kw => n.title.toLowerCase().includes(kw));
  }).sort((a, b) =>
    new Date(b.awardedDate || b.publishedDate || 0).getTime() -
    new Date(a.awardedDate || a.publishedDate || 0).getTime()
  ).slice(0, 6);

  // Buyer tier classification (relative to top buyer's spend)
  const maxBuyerSpend = topBuyers[0]?.[1].awardedValue || 1;
  const buyerTierLabel = (spend: number): [string, string] => {
    const pct = spend / maxBuyerSpend;
    if (pct >= 0.6) return ["T1", "bw-tier-1"];
    if (pct >= 0.2) return ["T2", "bw-tier-2"];
    return ["T3", "bw-tier-3"];
  };

  // Demand signal panel
  const demandHtml = profile.live && !isCompiling
    ? `<div class="dp-head-row">
        <span class="dp-eyebrow">AWARDED DEMAND SIGNAL</span>
        <span class="dp-info" title="Based on awarded notices in the public record">ⓘ</span>
       </div>
       <p class="dp-caveat-sm">Based on awarded notices found in the public record<br>for this desk profile (last 12 months).</p>
       <div class="dp-stats">
         <div class="dp-stat">
           <span class="dp-val">${escapeHtml(fmtBig(totalAwarded))}+</span>
           <span class="dp-stat-label">Total awarded value</span>
         </div>
         <div class="dp-stat">
           <span class="dp-val">${escapeHtml(awardedCount.toLocaleString("en-GB"))}</span>
           <span class="dp-stat-label">Awarded notices</span>
         </div>
         <div class="dp-stat">
           <span class="dp-val">${escapeHtml(uniqueBuyerCount.toLocaleString("en-GB"))}</span>
           <span class="dp-stat-label">Public buyers</span>
         </div>
       </div>
       ${topCats.length ? `<div class="dp-bars-head">Top recurring demand areas <span class="dp-bars-sub">(by awarded value)</span></div>
       ${topCats.map(c => {
         const pct = Math.round((c.value / maxCatVal) * 100);
         return `<div class="dp-bar-row">
           <span class="dp-bar-label">${escapeHtml(c.label)}</span>
           <div class="dp-bar-track"><div class="dp-bar-fill" style="width:${pct}%"></div></div>
           <span class="dp-bar-val">${escapeHtml(fmtMoney(c.value))}</span>
         </div>`;
       }).join("")}
       <a class="dp-map-link" href="#demand-map">Open the demand map &darr;</a>` : ""}
       <p class="dp-caveat-foot">Caveat: Not a whole-market estimate. See sources below.</p>`
    : `<div class="dp-head-row">
        <span class="dp-eyebrow">AWARDED DEMAND SIGNAL</span>
       </div>
       <p style="color:var(--muted);margin-top:16px;font-size:14px;line-height:1.7">${isCompiling ? "Demand data compiles on first request.<br>Refresh after ~90 seconds." : "This desk is coming soon."}</p>
       ${isCompiling ? `<span class="chip chip-amber" style="margin-top:16px;display:inline-block">Compiling &mdash; refresh in ~90 seconds</span>` : ""}`;

  // Live opportunities panel — scored and bucketed
  const deskOppContext: DeskOpportunityContext = {
    type: "desk",
    slug: profile.slug,
    label: profile.label,
    keywords: deskKeywords,
  };
  const deskScoredOpen = profile.live && !isCompiling && openNotices.length > 0
    ? scoreAndBucketNotices(openNotices.map(normaliseFromProcurementNotice), deskOppContext)
    : [];

  // Urgency strip: scored notices closing within 7 days
  const closingSoonScored = deskScoredOpen.filter(n => {
    if (!n.deadlineDate) return false;
    const d = new Date(n.deadlineDate).getTime();
    return d > nowMs && d <= nowMs + 7 * 24 * 3_600_000;
  });
  const urgencyIds = new Set(closingSoonScored.slice(0, 3).map(n => n.id));
  const regularCards = deskScoredOpen.filter(n => !urgencyIds.has(n.id));

  const urgencyStripHtml = closingSoonScored.length > 0
    ? `<div class="urgency-strip">
      <div class="urgency-strip-head">
        <span class="live-dot" style="background:var(--brand);animation:none;margin-right:2px"></span>
        CLOSING IN 7 DAYS &mdash; ${closingSoonScored.length} ${closingSoonScored.length === 1 ? "notice" : "notices"}
      </div>
      ${closingSoonScored.slice(0, 3).map(n => {
        const d = new Date(n.deadlineDate!).getTime();
        const daysLeft = Math.max(1, Math.ceil((d - nowMs) / (24 * 3_600_000)));
        const safeHref = n.url && n.url !== "#" ? escapeHtml(n.url) : "#";
        return `<a class="urgency-item" href="${safeHref}" target="_blank" rel="noopener noreferrer">
          <span class="urgency-title">${escapeHtml(n.title.slice(0, 70))}</span>
          <span class="urgency-badge">${daysLeft}d left</span>
        </a>`;
      }).join("")}
    </div>`
    : "";

  const liveHtml = `<div class="dp-head-row" style="margin-bottom:14px">
    <div style="display:flex;align-items:center;gap:8px">
      <span class="live-dot"></span>
      <span class="dp-eyebrow">LIVE OPPORTUNITIES IN THIS DESK</span>
    </div>
    <a href="/desk/${profile.slug}/notices" class="dp-link-sm">Opportunity board &rarr;</a>
  </div>
  ${!profile.live || isCompiling
    ? `<p class="dp-caveat-sm">Opportunity feed compiles on first request.<br>Refresh after ~90 seconds.</p>`
    : deskScoredOpen.length
      ? urgencyStripHtml + regularCards.slice(0, 3).map(n => renderOpportunityCard(n, { deskSlug: profile.slug })).join("") + `<a href="/desk/${profile.slug}/notices" class="dp-link-sm" style="display:inline-block;margin-top:14px">See all opportunities &rarr;</a>`
      : `<p class="dp-caveat-sm">No open notices at last refresh.</p><a href="/desk/${profile.slug}/notices" class="dp-map-link" style="font-weight:700">Check the full board &rarr;</a>`
  }
  <p class="ls-foot">Sourced from Contracts Finder and Find a Tender &nbsp;&middot;&nbsp; Public record only</p>`;

  // Buyer watchlist panel
  const watchlistHtml = `<div class="dp-head-row" style="margin-bottom:16px">
    <div style="display:flex;align-items:center;gap:8px">
      <span class="dp-eyebrow">BUYER WATCHLIST</span>
      <span class="dp-info" title="Top buyers by estimated 12-month spend on this desk">ⓘ</span>
    </div>
    <a href="/desk/${profile.slug}/buyers" class="dp-link-sm">View full watchlist &rarr;</a>
  </div>
  ${!profile.live || isCompiling || topBuyers.length === 0
    ? `<p class="dp-caveat-sm">Buyer data compiles with the demand signal.<br>Refresh after ~90 seconds.</p>`
    : topBuyers.map(([buyer, info]) => {
        const orgType = buyerOrgType(buyer);
        const initials = buyerInitials(buyer);
        const spend = info.awardedValue > 0 ? fmtMoney(info.awardedValue) : "—";
        const activeCount = info.openCount || info.awardedCount;
        const tagClass = orgType === "HEALTH" ? "bw-tag-health"
          : orgType === "LOCAL AUTHORITY" ? "bw-tag-la"
          : orgType === "CENTRAL GOV" ? "bw-tag-gov"
          : orgType === "HOUSING" ? "bw-tag-housing"
          : orgType === "EDUCATION" ? "bw-tag-edu"
          : "bw-tag-other";
        const [tierLabel, tierClass] = buyerTierLabel(info.awardedValue);
        const spendPct = Math.round((info.awardedValue / maxBuyerSpend) * 100);
        return `<div class="bw-row">
          <div class="bw-avatar">${escapeHtml(initials)}</div>
          <div class="bw-info">
            <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:8px;margin-bottom:4px">
              <div class="bw-name" style="margin-bottom:0">${escapeHtml(buyer.slice(0, 50))}</div>
              <span class="bw-tier ${tierClass}">${tierLabel}</span>
            </div>
            ${orgType ? `<span class="bw-tag ${tagClass}">${escapeHtml(orgType)}</span>` : ""}
            <div class="bw-meta" style="margin-top:6px">
              <span class="bw-spend">${escapeHtml(spend)}</span>
              <span class="bw-meta-label"> total awarded</span>
            </div>
            <div class="bw-spend-bar-track"><div class="bw-spend-bar-fill" style="width:${spendPct}%"></div></div>
            <div class="bw-meta"><span class="bw-meta-label">Active notices: ${activeCount}</span></div>
          </div>
        </div>`;
      }).join("")
  }
  <p class="dp-caveat-foot">Watchlist rotates daily. Figures are indicative.</p>`;

  // ── Market Pulse strip ──────────────────────────────────────────────────
  const pulseHtml = profile.live && !isCompiling ? `
  <div class="dp-pulse">
    <div class="dp-pulse-inner">
      <div class="dp-pulse-stat">
        <span class="dp-pulse-val">${escapeHtml(fmtBig(totalAwarded))}+</span>
        <span class="dp-pulse-label">Market size (12m)</span>
      </div>
      <div class="dp-pulse-stat">
        <span class="dp-pulse-val">${openNoticeCount}</span>
        <span class="dp-pulse-label">Active tenders</span>
      </div>
      <div class="dp-pulse-stat">
        <span class="dp-pulse-val">${avgContractVal > 0 ? escapeHtml(fmtBig(avgContractVal)) : "&mdash;"}</span>
        <span class="dp-pulse-label">Avg contract value</span>
      </div>
      <div class="dp-pulse-stat">
        <span class="dp-pulse-val">${buyersThisMonth > 0 ? buyersThisMonth : "&mdash;"}</span>
        <span class="dp-pulse-label">Buyers publishing now</span>
      </div>
      <div class="dp-pulse-stat${closingSoonRawCount > 0 ? " dp-pulse-urgent" : ""}">
        <span class="dp-pulse-val">${closingSoonRawCount > 0 ? closingSoonRawCount : "&mdash;"}</span>
        <span class="dp-pulse-label">Closing in 7 days</span>
      </div>
    </div>
  </div>` : "";

  // ── Spend trend chart ────────────────────────────────────────────────────
  const spendChartHtml = trendData.length >= 2
    ? `<div class="trend-chart">
      <div class="trend-bars">
        ${trendData.map(([key, val]) => {
          const pct = Math.max(Math.round((val / maxMonthVal) * 100), 2);
          const dt = new Date(key + "-01");
          const mo = dt.toLocaleDateString("en-GB", { month: "short" });
          const yr = String(dt.getFullYear()).slice(2);
          return `<div class="trend-bar-col" title="${escapeHtml(mo + " '" + yr + ": " + fmtShort(val))}">
            <div class="trend-bar" style="height:${pct}%"></div>
            <div class="trend-bar-label">${escapeHtml(mo)}<br><span>${escapeHtml("'" + yr)}</span></div>
          </div>`;
        }).join("")}
      </div>
      <div class="trend-foot">
        <span class="trend-total">${escapeHtml(fmtBig(totalAwarded))}+</span>
        <span class="trend-total-label">12-month awarded total &mdash; ${trendData.length} months of data</span>
      </div>
    </div>`
    : `<p style="color:var(--muted);font-size:14px;margin-top:16px">Trend data compiles after the desk has been live for a few weeks.</p>`;

  // ── Analytics section ───────────────────────────────────────────────────
  const catBreakdownHtml = topCats.length > 0
    ? topCats.map(c => {
      const pct = Math.round((c.value / maxCatVal) * 100);
      return `<div class="cat-breakdown-item">
        <div>
          <div class="cat-breakdown-label">${escapeHtml(c.label)}</div>
          <div class="cat-breakdown-track"><div class="cat-breakdown-fill" style="width:${pct}%"></div></div>
        </div>
        <div class="cat-breakdown-val">${escapeHtml(fmtMoney(c.value))}</div>
      </div>`;
    }).join("")
    : `<p style="color:var(--muted);font-size:14px;margin-top:16px">Category breakdown compiles on first desk load.</p>`;

  const analyticsHtml = profile.live && !isCompiling ? `
  <section class="analytics-section">
    <div class="analytics-inner">
      <div>
        <div class="analytics-head">12-MONTH SPEND TREND</div>
        <p style="font-size:13px;color:var(--muted);margin-bottom:20px;letter-spacing:.01em">Monthly awarded contract value &mdash; public record</p>
        ${spendChartHtml}
      </div>
      <div>
        <div class="analytics-head">CATEGORY BREAKDOWN</div>
        <p style="font-size:13px;color:var(--muted);margin-bottom:24px;letter-spacing:.01em">Top sub-sectors by awarded spend (12 months)</p>
        ${catBreakdownHtml}
      </div>
    </div>
  </section>` : "";

  // ── Recent awards strip ──────────────────────────────────────────────────
  const recentAwardsHtml = recentAwards.length > 0
    ? `<section class="awards-section">
      <div class="awards-inner">
        <div class="awards-head-row">
          <span class="awards-title">RECENT AWARDS &mdash; LAST 90 DAYS</span>
          <span style="font-family:var(--mono);font-size:10.5px;color:var(--faint)">${recentAwards.length} award${recentAwards.length === 1 ? "" : "s"} found</span>
        </div>
        <div class="awards-grid">
          ${recentAwards.map(n => {
            const val = n.awardedValue;
            const dt = n.awardedDate || n.publishedDate;
            const dtFmt = dt ? new Date(dt).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" }) : "—";
            return `<div class="award-card">
              <div class="award-card-buyer">${escapeHtml(n.buyer.slice(0, 45))}</div>
              <div class="award-card-title">${escapeHtml(n.title.slice(0, 90))}</div>
              <div class="award-card-meta">
                <span class="award-card-val">${val && val > 0 ? escapeHtml(fmtMoney(val)) : "&mdash;"}</span>
                <span class="award-card-date">${escapeHtml(dtFmt)}</span>
              </div>
              ${n.awardedSupplier ? `<div class="award-card-winner">&#127942; ${escapeHtml(n.awardedSupplier.slice(0, 50))}</div>` : ""}
            </div>`;
          }).join("")}
        </div>
      </div>
    </section>`
    : "";

  // Demand map grid
  const sortedCategories = [...profile.categories].sort((a, b) => {
    const da = demandCategories.find(c => c.label === a.label)?.latestDate ?? 0;
    const db = demandCategories.find(c => c.label === b.label)?.latestDate ?? 0;
    return db - da;
  });

  const dmGridHtml = sortedCategories.map(cat => {
    const inferred = demandCategories.find(c => c.label === cat.label);
    const count = inferred?.count ?? 0;
    const more = cat.subcategories.length - 7;
    const subsHtml = cat.subcategories.map((s, i) =>
      `<li${i >= 7 ? ' class="dm-sub-x" style="display:none"' : ""}><a href="/desk/${profile.slug}/sub/${slugify(s)}">${escapeHtml(s)}</a></li>`
    ).join("");
    const toggleHtml = more > 0
      ? `<button class="dm-more-btn" data-open="0" data-more="${more}" onclick="var x=this.closest('.dm-card').querySelectorAll('.dm-sub-x');var o=this.dataset.open==='1';x.forEach(e=>e.style.display=o?'none':'list-item');this.textContent=o?'+ '+this.dataset.more+' more':'Show less';this.dataset.open=o?'0':'1'">+ ${more} more</button>`
      : "";
    return `<div class="dm-card">
      <div class="dm-card-head">
        <div class="dm-icon-wrap">${getCategoryIcon(cat.label)}</div>
        <div class="dm-card-title">
          <span class="dm-name">${escapeHtml(cat.label)}</span>
          ${count > 0 ? `<span class="dm-count">${count} ${count === 1 ? "notice" : "notices"}</span>` : ""}
        </div>
      </div>
      <ul class="dm-subs">${subsHtml}</ul>
      ${toggleHtml}
    </div>`;
  }).join("");

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>${escapeHtml(profile.label)} &mdash; GovRevenue Desk</title>
<style>
${pageShellCss()}
html{scroll-behavior:smooth}
/* Masthead */
.dm-mast{padding:60px 0 56px;background:radial-gradient(120% 160% at 80% 0%,#16341F 0%,#0E2417 60%,#0A1C12 100%);color:#ECE6D6}
.dm-mast-inner{padding:0 56px;display:grid;grid-template-columns:1fr 360px;gap:72px;align-items:start}
.dm-mast-eyebrow{font-family:var(--mono);font-size:11px;letter-spacing:.18em;text-transform:uppercase;color:var(--brand);margin-bottom:14px}
.dm-mast h1{font-family:var(--serif);font-size:clamp(36px,4.2vw,56px);font-weight:400;line-height:1.02;letter-spacing:-.02em;margin-bottom:20px;color:#ECE6D6}
.dm-mast-lede{font-size:17px;color:#C5C9BC;line-height:1.65;margin-bottom:20px;max-width:42em}
.dm-source-badge{font-size:13px;color:var(--muted);display:flex;align-items:center;gap:7px}
.dm-mast-cta{border:1px solid rgba(236,230,214,.16);padding:24px;background:#0C1F15}
.dm-mast-cta p{font-size:14px;line-height:1.55;margin-bottom:16px;color:#C5C9BC}
.btn-cta{display:flex;align-items:center;justify-content:center;gap:8px;background:var(--brand);color:#10110D;font-family:var(--sans);font-size:14px;font-weight:600;padding:14px;transition:.18s}
.btn-cta:hover{background:var(--brand-hot)}
/* Three panels — dark intelligence surface */
.dp-panels{background:var(--base);border-bottom:1px solid rgba(255,255,255,.04)}
.dp-panels-inner{padding:0 56px;display:grid;grid-template-columns:repeat(3,minmax(0,1fr));border-top:1px solid var(--border)}
.dp-panel{padding:52px 40px;border-right:1px solid var(--border)}
.dp-head-row{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px}
.dp-eyebrow{font-family:var(--mono);font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--muted)}
.dp-info{font-size:13px;color:var(--faint);cursor:help;margin-left:4px}
.dp-caveat-sm{font-size:13.5px;color:var(--muted);line-height:1.7;margin-bottom:24px}
.dp-caveat-foot{font-family:var(--mono);font-size:11px;color:var(--faint);margin-top:28px;padding-top:18px;border-top:1px solid var(--border)}
.dp-link-sm{font-family:var(--mono);font-size:11px;letter-spacing:.04em;color:var(--brand);text-decoration:underline;text-decoration-color:var(--brand-dim)}
.dp-link-sm:hover{text-decoration-color:var(--brand);color:var(--brand-hot)}
.dp-bars-head{font-family:var(--mono);font-size:11px;letter-spacing:.09em;text-transform:uppercase;color:var(--faint);margin:34px 0 20px}
.dp-bars-sub{text-transform:none;letter-spacing:0;font-size:11px;color:var(--faint)}
.dp-map-link{display:inline-block;font-family:var(--mono);font-size:11px;letter-spacing:.04em;color:var(--brand);margin-top:20px;text-decoration:underline;text-decoration-color:var(--brand-dim)}
.dp-map-link:hover{text-decoration-color:var(--brand)}
/* Demand stats — raised terminal card */
.dp-stats{display:grid;grid-template-columns:repeat(3,1fr);background:var(--surface-2);border:1px solid var(--border-2);margin:26px 0 14px}
.dp-stat{padding:30px 22px}
.dp-stat:not(:last-child){border-right:1px solid var(--border)}
.dp-val{display:block;font-family:var(--mono);font-size:38px;font-weight:600;letter-spacing:-.02em;line-height:1.05;color:var(--text)}
.dp-stat-label{display:block;font-family:var(--mono);font-size:9.5px;letter-spacing:.09em;text-transform:uppercase;color:var(--faint);margin-top:9px}
/* Bar rows on dark */
.dp-bar-row{display:grid;grid-template-columns:1fr 80px 68px;gap:12px;align-items:center;margin-bottom:16px}
.dp-bar-label{font-size:13px;color:var(--muted)}
.dp-bar-track{height:3px;background:rgba(27,30,25,.10);border-radius:2px}
.dp-bar-fill{height:3px;background:var(--brand);border-radius:2px}
.dp-bar-val{font-family:var(--mono);font-size:12px;color:var(--faint);text-align:right}
/* Live dot */
.live-dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--green);flex-shrink:0;margin-right:4px;animation:ldpulse 2.4s ease-in-out infinite}
@keyframes ldpulse{0%{box-shadow:0 0 0 0 rgba(34,197,94,.4)}70%{box-shadow:0 0 0 6px rgba(34,197,94,0)}100%{box-shadow:0 0 0 0 rgba(34,197,94,0)}}
/* Market Pulse strip */
.dp-pulse{background:var(--surface);border-bottom:1px solid var(--border)}
.dp-pulse-inner{padding:0 56px;display:flex}
.dp-pulse-stat{flex:1;padding:20px 24px;border-right:1px solid var(--border)}
.dp-pulse-stat:last-child{border-right:none}
.dp-pulse-val{display:block;font-family:var(--mono);font-size:26px;font-weight:600;letter-spacing:-.02em;color:var(--text);line-height:1.1}
.dp-pulse-label{display:block;font-family:var(--mono);font-size:9.5px;letter-spacing:.09em;text-transform:uppercase;color:var(--faint);margin-top:6px}
.dp-pulse-urgent .dp-pulse-val{color:var(--brand)}
.dp-pulse-urgent .dp-pulse-label{color:var(--brand-dim)}
/* Urgency strip */
.urgency-strip{background:var(--brand-dim);border:1px solid rgba(180,146,78,.25);padding:12px 16px;margin-bottom:16px}
.urgency-strip-head{font-family:var(--mono);font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:var(--brand);margin-bottom:10px;display:flex;align-items:center;gap:4px}
.urgency-item{display:flex;justify-content:space-between;align-items:center;padding:7px 0;border-bottom:1px solid rgba(180,146,78,.1);text-decoration:none;transition:background .12s;margin:0 -16px;padding-left:16px;padding-right:16px}
.urgency-item:last-child{border-bottom:none;padding-bottom:0}
.urgency-item:hover{background:rgba(180,146,78,.12)}
.urgency-item:hover .urgency-title{color:var(--text)}
.urgency-title{font-size:12.5px;color:var(--muted);line-height:1.35;flex:1;min-width:0;overflow:hidden;white-space:nowrap;text-overflow:ellipsis;padding-right:12px;transition:color .12s}
.urgency-badge{font-family:var(--mono);font-size:10px;background:rgba(180,146,78,.2);color:var(--brand);padding:3px 8px;border-radius:2px;white-space:nowrap;flex-shrink:0}
/* Analytics section — dark zone continuation */
.analytics-section{background:var(--base);padding:56px 0;border-bottom:1px solid var(--border)}
.analytics-inner{padding:0 56px;display:grid;grid-template-columns:1fr 1fr;gap:48px;align-items:start}
.analytics-head{font-family:var(--mono);font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--muted);margin-bottom:8px}
/* Spend trend chart */
.trend-chart{background:var(--surface-2);border:1px solid var(--border-2);padding:28px 24px 0}
.trend-bars{display:flex;align-items:flex-end;justify-content:space-between;gap:5px;height:140px;border-bottom:1px solid var(--border);padding-bottom:0;overflow:hidden}
.trend-bar-col{flex:1;max-width:46px;display:flex;flex-direction:column;align-items:center;height:100%;justify-content:flex-end}
.trend-bar{width:100%;background:linear-gradient(to top,rgba(180,146,78,.4),var(--brand));border-radius:2px 2px 0 0;transition:opacity .15s;cursor:default}
.trend-bar:hover{opacity:.75}
.trend-bar-label{font-family:var(--mono);font-size:8.5px;color:var(--faint);text-align:center;padding:10px 0 12px;line-height:1.4}
.trend-foot{padding:20px 0 28px;display:flex;align-items:baseline;gap:12px}
.trend-total{font-family:var(--mono);font-size:30px;font-weight:600;color:var(--text)}
.trend-total-label{font-family:var(--mono);font-size:10px;color:var(--faint);letter-spacing:.06em;text-transform:uppercase}
/* Category breakdown — dark */
.cat-breakdown-item{display:grid;grid-template-columns:1fr 72px;gap:16px;align-items:center;margin-bottom:22px}
.cat-breakdown-label{font-size:13.5px;color:var(--muted);margin-bottom:6px}
.cat-breakdown-track{height:3px;background:rgba(27,30,25,.10);border-radius:2px}
.cat-breakdown-fill{height:3px;background:var(--brand);border-radius:2px}
.cat-breakdown-val{font-family:var(--mono);font-size:12px;color:var(--faint);text-align:right;padding-top:20px}
/* Recent awards — dark */
.awards-section{background:var(--surface-2);border-bottom:1px solid var(--border)}
.awards-inner{padding:56px 56px}
.awards-head-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:28px}
.awards-title{font-family:var(--mono);font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--muted)}
.awards-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}
.award-card{background:var(--surface-3);border:1px solid var(--border-2);padding:22px;display:flex;flex-direction:column;transition:border-color .15s,box-shadow .15s}
.award-card:hover{border-color:rgba(180,146,78,.3);box-shadow:0 4px 24px rgba(0,0,0,.4)}
.award-card-buyer{font-family:var(--mono);font-size:9.5px;letter-spacing:.07em;text-transform:uppercase;color:var(--faint);margin-bottom:8px;overflow:hidden;white-space:nowrap;text-overflow:ellipsis}
.award-card-title{font-size:13.5px;font-weight:500;color:var(--text);line-height:1.45;margin-bottom:14px;overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;flex:1}
.award-card-meta{display:flex;justify-content:space-between;align-items:baseline}
.award-card-val{font-family:var(--mono);font-size:18px;font-weight:600;color:var(--text)}
.award-card-date{font-family:var(--mono);font-size:10px;color:var(--faint)}
.award-card-winner{font-family:var(--mono);font-size:10.5px;color:var(--faint);margin-top:10px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;border-top:1px solid var(--border);padding-top:10px}
/* Buyer tier & spend bar */
.bw-tier{font-family:var(--mono);font-size:8.5px;letter-spacing:.08em;padding:2px 7px;border-radius:2px;flex-shrink:0}
.bw-tier-1{background:var(--brand-dim);color:var(--brand);border:1px solid rgba(180,146,78,.25)}
.bw-tier-2{background:rgba(34,197,94,.1);color:var(--green);border:1px solid rgba(34,197,94,.2)}
.bw-tier-3{background:rgba(255,255,255,.04);color:var(--faint);border:1px solid rgba(255,255,255,.08)}
.bw-spend-bar-track{height:2px;background:rgba(27,30,25,.10);border-radius:1px;margin:8px 0 4px}
.bw-spend-bar-fill{height:2px;background:var(--brand);border-radius:1px;opacity:.7}
/* ls-table (used in subPage) */
.ls-table{width:100%;border-collapse:collapse;font-size:13.5px;margin-top:10px}
.ls-table th{font-family:var(--mono);font-size:10px;letter-spacing:.09em;text-transform:uppercase;color:var(--muted);text-align:left;padding:0 8px 12px 0;border-bottom:1px solid var(--border-2)}
.ls-table th.ls-th-r{text-align:right}
.ls-table td{padding:14px 8px 14px 0;border-bottom:1px solid var(--border);vertical-align:top}
.ls-title-cell{overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}
.ls-table a{color:var(--brand);text-decoration:underline;text-decoration-color:var(--brand-dim)}
.ls-table a:hover{text-decoration-color:var(--brand)}
.ls-buyer{color:var(--muted);font-size:12.5px}
.ls-val{font-family:var(--mono);font-size:12.5px;white-space:nowrap;text-align:right;padding-left:24px}
.ls-date{font-family:var(--mono);font-size:12.5px;color:var(--muted);white-space:nowrap;text-align:right;padding-left:16px}
.ls-foot{font-family:var(--mono);font-size:11px;color:var(--muted);margin-top:18px}
/* Buyer watchlist on dark */
.bw-row{display:flex;gap:16px;padding:20px 0;border-bottom:1px solid var(--border)}
.bw-row:last-of-type{border-bottom:none}
.bw-avatar{width:44px;height:44px;border-radius:4px;background:var(--surface-3);color:var(--muted);font-family:var(--mono);font-size:10px;font-weight:600;display:flex;align-items:center;justify-content:center;letter-spacing:.04em;flex-shrink:0;margin-top:1px;border:1px solid var(--border-2)}
.bw-info{flex:1;min-width:0}
.bw-name{font-size:13.5px;font-weight:500;line-height:1.35;margin-bottom:5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:var(--text)}
.bw-tag{font-family:var(--mono);font-size:9.5px;letter-spacing:.05em;padding:2px 7px;border-radius:2px;display:inline-block;margin-bottom:6px}
.bw-tag-health{background:#0e2a1f;color:#4dba8a;border:1px solid rgba(45,155,111,.2)}
.bw-tag-la{background:#0e1a2e;color:#6090d4;border:1px solid rgba(96,144,212,.2)}
.bw-tag-gov{background:#261c0e;color:#c4a35a;border:1px solid rgba(196,163,90,.2)}
.bw-tag-housing{background:#1c0e2e;color:#a07ad4;border:1px solid rgba(160,122,212,.2)}
.bw-tag-edu{background:#261a0e;color:#d4924a;border:1px solid rgba(212,146,74,.2)}
.bw-tag-other{background:rgba(255,255,255,.04);color:var(--muted);border:1px solid rgba(255,255,255,.08)}
.bw-meta{font-family:var(--mono);font-size:11px;color:var(--muted);line-height:1.65}
.bw-spend{font-size:16px;color:var(--text);font-family:var(--mono);font-weight:600;margin-right:2px}
.bw-meta-label{font-size:11px;color:var(--muted)}
.bw-sample{font-weight:400;opacity:.4;letter-spacing:.03em}
/* Demand map — dark */
.dm-section{padding:64px 0;background:var(--base);border-bottom:1px solid var(--border)}
.dm-section-inner{padding:0 56px}
.dm-head-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}
.dm-title{font-family:var(--mono);font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--muted)}
.dm-title-info{font-size:13px;color:var(--faint);cursor:help;margin-left:5px}
.dm-sub{font-size:13px;color:var(--muted);margin-bottom:28px}
.dm-open-all{font-family:var(--mono);font-size:10.5px;letter-spacing:.04em;color:var(--muted);border:1px solid var(--border-2);padding:6px 14px;cursor:pointer;background:var(--surface);transition:.15s}
.dm-open-all:hover{border-color:var(--brand);color:var(--brand)}
.dm-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(230px,1fr));gap:18px}
.dm-card{border:1px solid var(--border-2);padding:28px 26px 24px;background:var(--surface);transition:border-color .15s,background .15s;cursor:default}
.dm-card:hover{border-color:var(--brand);background:var(--surface-2)}
.dm-card-head{display:flex;align-items:flex-start;gap:12px;margin-bottom:14px}
.dm-icon-wrap{flex-shrink:0;width:36px;height:36px;background:var(--surface-2);border:1px solid var(--border-2);display:flex;align-items:center;justify-content:center;padding:7px;color:var(--muted)}
.dm-icon{width:100%;height:100%}
.dm-card-title{flex:1;min-width:0}
.dm-name{display:block;font-size:13.5px;font-weight:600;line-height:1.35;color:var(--text)}
.dm-count{font-family:var(--mono);font-size:10.5px;color:var(--muted);font-weight:400;margin-top:3px;display:block}
.dm-subs{list-style:none;font-size:12px;color:var(--muted);line-height:1.9;margin-top:4px}
.dm-subs li{padding-left:0}
.dm-subs a{color:var(--muted);text-decoration:none;border-bottom:1px solid var(--border);transition:color .12s,border-color .12s}
.dm-subs a:hover{color:var(--brand);border-bottom-color:var(--brand)}
.dm-more-btn{font-family:var(--mono);font-size:11px;color:var(--brand);background:none;border:none;cursor:pointer;padding:6px 0 0;text-decoration:underline;text-decoration-color:var(--brand-dim);display:block}
.dm-more-btn:hover{text-decoration-color:var(--brand)}
/* Sources bar */
.dm-sources-bar{background:var(--surface);border-top:1px solid var(--border)}
.dm-sources-inner{padding:16px 56px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px}
.dm-sources-left{font-size:12.5px;color:var(--muted)}
.dm-sources-right{display:flex;gap:4px;align-items:center;flex-wrap:wrap}
.dm-src-label{font-family:var(--mono);font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);margin-right:6px}
.dm-src-link{font-family:var(--mono);font-size:11px;color:var(--muted);text-decoration:underline;text-decoration-color:var(--border-2);padding:0 6px}
.dm-src-link:hover{color:var(--text)}
.dm-foot-copy{text-align:center;font-family:var(--mono);font-size:10.5px;letter-spacing:.06em;color:var(--muted);padding:12px 0 16px;border-top:1px solid var(--border)}
@media(max-width:1100px){
  .dp-panels-inner{grid-template-columns:1fr}
  .dm-mast-inner{grid-template-columns:1fr}
  .dm-mast-cta{display:none}
  .dm-mast h1{font-size:36px}
  .analytics-inner{grid-template-columns:1fr;gap:40px}
  .awards-grid{grid-template-columns:repeat(2,1fr)}
}
@media(max-width:760px){
  .gh-inner,.dm-mast-inner,.dp-panels-inner,.dm-section-inner,.dm-sources-inner{padding-left:16px;padding-right:16px}
  .dp-pulse-inner,.analytics-inner,.awards-inner{padding-left:16px;padding-right:16px}
  .dm-mast{padding:32px 0 28px}
  .dm-mast-inner{grid-template-columns:1fr;gap:0}
  .dm-mast h1{font-size:24px}
  .dm-mast-lede{font-size:14px}
  .dp-panels-inner{grid-template-columns:1fr;border-left:none}
  .dp-panel{padding:24px 0;border-right:none;border-bottom:1px solid var(--border)}
  .dp-stats{grid-template-columns:1fr 1fr}
  .dp-val{font-size:26px}
  .dp-bar-row{grid-template-columns:1fr 60px}
  .dp-bar-track{display:none}
  .dm-grid{grid-template-columns:1fr 1fr}
  .ls-val,.ls-date,.ls-buyer{display:none}
  .dm-sources-inner{flex-direction:column;align-items:flex-start;gap:6px;padding-top:14px;padding-bottom:14px}
  .dm-sources-right{flex-wrap:wrap}
  .ls-table{display:block;overflow-x:auto;-webkit-overflow-scrolling:touch}
  .dp-pulse-inner{flex-wrap:wrap}
  .dp-pulse-stat{flex:0 0 50%;border-right:none;border-bottom:1px solid var(--border)}
  .awards-grid{grid-template-columns:1fr}
  .analytics-inner{grid-template-columns:1fr;gap:24px}
  .awards-inner{padding:36px 16px}
  .sub-cta-row{flex-direction:column;gap:14px;align-items:flex-start}
}
@media(max-width:480px){
  .dm-grid{grid-template-columns:1fr}
  .dp-stats{grid-template-columns:1fr}
  .dp-pulse-stat{flex:0 0 100%}
  .dm-mast h1{font-size:20px}
}
${oppCardCss()}
/* Scoped overrides: opp-cards on dark panel surface */
.dp-panel .opp-card{background:var(--surface-2);border:1px solid var(--border-2);box-shadow:0 2px 16px rgba(0,0,0,.38),0 1px 4px rgba(0,0,0,.22);transition:box-shadow .18s,transform .18s}
.dp-panel .opp-card:hover{box-shadow:0 8px 32px rgba(0,0,0,.48);transform:translateY(-2px);border-color:var(--brand)}
.dp-panel .opp-card--low_confidence{opacity:.75;box-shadow:0 1px 8px rgba(0,0,0,.2)}
.dp-panel .opp-cold{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);color:var(--muted)}
.dp-panel .opp-cold strong{color:var(--text)}
.dp-panel .ls-foot{color:var(--faint)}
.dp-panel .opp-cards{gap:14px}
${deskOpportunityCss()}
</style>
</head>
<body>
${pageShellHeader(profile, authCtx)}

<section class="dm-mast">
  <div class="dm-mast-inner">
    <div>
      <div class="dm-mast-eyebrow">DESK</div>
      <h1>${escapeHtml(profile.label)}</h1>
      <p class="dm-mast-lede">${escapeHtml(profile.standfirst)}</p>
      <div class="dm-source-badge">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2l9 4.9V12c0 5.5-3.8 10.7-9 12-5.2-1.3-9-6.5-9-12V6.9z"/></svg>
        <span>Public record only. Sourced from Contracts Finder and Find a Tender.</span>
      </div>
    </div>
    <div class="dm-mast-cta">
      <p>Find out if your firm fits this desk. A scan compares your services against live ${escapeHtml(profile.label)} procurement and returns a sourced verdict in minutes.</p>
      <a class="btn-cta" href="/scan">RUN A SCAN &nbsp;&rarr;</a>
    </div>
  </div>
</section>

${pulseHtml}

<div class="dp-panels">
  <div class="dp-panels-inner">
    <div class="dp-panel">${demandHtml}</div>
    <div class="dp-panel">${liveHtml}</div>
    <div class="dp-panel">${watchlistHtml}</div>
  </div>
</div>

${analyticsHtml}

${recentAwardsHtml}

<section class="dm-section" id="demand-map">
  <div class="dm-section-inner">
    <div class="dm-head-row">
      <div>
        <span class="dm-title">DEMAND MAP &ndash; ${escapeHtml(profile.label.toUpperCase())}</span>
        <span class="dm-title-info" title="All major categories and sub-categories this desk scans for">ⓘ</span>
      </div>
      <button type="button" class="dm-open-all" data-open="0" onclick="var o=this.dataset.open==='1';this.closest('section').querySelectorAll('.dm-sub-x').forEach(e=>e.style.display=o?'none':'list-item');this.closest('section').querySelectorAll('.dm-more-btn').forEach(b=>{b.textContent=o?('+ '+b.dataset.more+' more'):'Show less';b.dataset.open=o?'0':'1'});this.textContent=o?'Open all':'Close all';this.dataset.open=o?'0':'1'">Open all</button>
    </div>
    <p class="dm-sub">All major categories and sub-categories this desk scans for.</p>
    <div class="dm-grid">${dmGridHtml}</div>
  </div>
</section>

<div class="dm-sources-bar">
  <div class="dm-sources-inner">
    <span class="dm-sources-left">Public record. No insider information. Always verify on the source.</span>
    <div class="dm-sources-right">
      <span class="dm-src-label">SOURCES</span>
      <a class="dm-src-link" href="https://www.contractsfinder.service.gov.uk" target="_blank" rel="noopener noreferrer">Contracts Finder &#8599;</a>
      <a class="dm-src-link" href="https://www.find-tender.service.gov.uk" target="_blank" rel="noopener noreferrer">Find a Tender &#8599;</a>
      <a class="dm-src-link" href="https://www.localspend.co.uk" target="_blank" rel="noopener noreferrer">Local Authority Transparency &#8599;</a>
      <a class="dm-src-link" href="https://find-and-update.company-information.service.gov.uk" target="_blank" rel="noopener noreferrer">Companies House &#8599;</a>
    </div>
    <span style="font-family:var(--mono);font-size:10.5px;color:var(--muted)">Caveat: Data is indicative, not exhaustive.</span>
  </div>
</div>
<div class="dm-foot-copy"><a href="/" style="color:inherit;text-decoration:underline;text-decoration-color:var(--border)">&larr; GovRevenue</a> &nbsp;&middot;&nbsp; &copy; 2026 GovRevenue &middot; Intelligence, not certainty. Public data only.</div>

</body>
</html>`;
}

function subPage(
  profile: DeskProfile,
  cat: DeskCategory,
  subLabel: string,
  cached: { data: ProcurementData; cached_at: string } | null,
  authCtx?: { email: string; tier: UserTier } | null
): string {
  const data = cached?.data;
  const isCompiling = cached === null;

  const stopWords = new Set(["and","&","the","a","of","for","in","to","with"]);
  const subWords = subLabel.toLowerCase().split(/[\s&,\/\-]+/).filter(w => w.length > 2 && !stopWords.has(w));
  const allKw = [...new Set([...subWords, ...cat.keywords])];

  const matchNotice = (n: ProcurementNotice): boolean => {
    const text = `${n.title} ${n.description || ""}`.toLowerCase();
    return allKw.some(kw => text.includes(kw));
  };

  const matchTitle = (n: ProcurementNotice): boolean => {
    const title = n.title.toLowerCase();
    return cat.keywords.some(kw => title.includes(kw));
  };

  const allOpen = dedupeNoticesSoft(
    (data?.contractsFinder.open || [])
      .concat(data?.findTender?.notices || [])
      .filter(matchTitle)
  ).sort((a, b) => new Date(b.publishedDate || b.awardedDate || "").getTime() - new Date(a.publishedDate || a.awardedDate || "").getTime());

  const allAwarded = (data?.contractsFinder.awarded || []).filter(matchNotice)
    .sort((a, b) => new Date(b.awardedDate || b.publishedDate || "").getTime() - new Date(a.awardedDate || a.publishedDate || "").getTime());

  const buyerMap = new Map<string, { awardedValue: number; count: number }>();
  for (const n of (allAwarded as ProcurementNotice[]).concat(allOpen)) {
    if (!n.buyer || n.buyer === "Not stated") continue;
    const e = buyerMap.get(n.buyer) || { awardedValue: 0, count: 0 };
    e.count++;
    e.awardedValue += n.awardedValue ?? 0;
    buyerMap.set(n.buyer, e);
  }
  const topBuyers = [...buyerMap.entries()]
    .filter(([buyer]) => !isAggregatorBuyer(buyer))
    .sort((a, b) => b[1].awardedValue - a[1].awardedValue)
    .slice(0, 10);

  const totalValue = allAwarded.reduce((s, n) => s + (n.awardedValue ?? 0), 0);

  const cutoff365sub = Date.now() - 365 * 24 * 3_600_000;
  const recentOpen = allOpen.filter(n => {
    const t = new Date(n.publishedDate || n.awardedDate || 0).getTime();
    return t > cutoff365sub;
  });
  const openRowsHtml = recentOpen.slice(0, 30).map(n => {
    const rawVal = n.valueHigh ?? n.valueLow ?? n.awardedValue;
    const val = rawVal != null && rawVal > 0 ? fmtMoney(rawVal) : "Not public";
    return `<tr>
      <td class="ls-title-cell"><a href="${escapeHtml(n.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(n.title.slice(0, 90))}</a></td>
      <td class="ls-buyer">${escapeHtml((n.buyer || "—").slice(0, 45))}</td>
      <td class="ls-val">${escapeHtml(val)}</td>
      <td class="ls-date">${escapeHtml(timeAgo(n.publishedDate || n.awardedDate))}</td>
    </tr>`;
  }).join("");

  const awardedRowsHtml = allAwarded.slice(0, 25).map(n => {
    const val = n.awardedValue && n.awardedValue > 0 ? fmtMoney(n.awardedValue) : "Not public";
    return `<tr>
      <td class="ls-title-cell"><a href="${escapeHtml(n.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(n.title.slice(0, 90))}</a></td>
      <td class="ls-buyer">${escapeHtml((n.buyer || "—").slice(0, 45))}</td>
      <td class="ls-val">${escapeHtml(val)}</td>
      <td class="ls-date">${escapeHtml(timeAgo(n.awardedDate || n.publishedDate))}</td>
    </tr>`;
  }).join("");

  const buyerRowsHtml = topBuyers.map(([buyer, info]) => {
    const orgType = buyerOrgType(buyer);
    const tagClass = orgType === "HEALTH" ? "bw-tag-health" : orgType === "LOCAL AUTHORITY" ? "bw-tag-la" : orgType === "CENTRAL GOV" ? "bw-tag-gov" : orgType === "HOUSING" ? "bw-tag-housing" : orgType === "EDUCATION" ? "bw-tag-edu" : "bw-tag-other";
    const spend = info.awardedValue > 0 ? fmtMoney(info.awardedValue) : "—";
    return `<div class="bw-row">
      <div class="bw-avatar">${escapeHtml(buyerInitials(buyer))}</div>
      <div class="bw-info">
        <div class="bw-name">${escapeHtml(buyer.slice(0, 55))}</div>
        ${orgType ? `<span class="bw-tag ${tagClass}">${escapeHtml(orgType)}</span>` : ""}
        <div class="bw-meta"><span class="bw-spend">${escapeHtml(spend)}</span><span class="bw-meta-label"> awarded past year</span></div>
        <div class="bw-meta"><span class="bw-meta-label">${info.count} ${info.count === 1 ? "notice" : "notices"}</span></div>
      </div>
    </div>`;
  }).join("");

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>${escapeHtml(subLabel)} &mdash; ${escapeHtml(profile.label)} &mdash; GovRevenue</title>
<style>
${pageShellCss()}
.sub-mast{padding:52px 0 44px;background:radial-gradient(120% 160% at 80% 0%,#16341F 0%,#0E2417 60%,#0A1C12 100%);color:#ECE6D6}
.sub-mast-inner{padding:0 56px}
.sub-crumb{font-family:var(--mono);font-size:11px;letter-spacing:.06em;color:var(--muted);margin-bottom:18px;display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.sub-crumb a{color:var(--muted);text-decoration:underline;text-decoration-color:var(--border-2)}
.sub-crumb a:hover{color:var(--brand)}
.sub-crumb-sep{color:var(--border-2)}
.sub-crumb-active{color:var(--text)}
.sub-mast h1{font-family:var(--serif);font-size:clamp(36px,4.2vw,52px);font-weight:400;line-height:1.02;letter-spacing:-.02em;margin-bottom:16px;color:#ECE6D6}
.sub-lede{font-size:16px;color:var(--muted);line-height:1.65;margin-bottom:32px}
.sub-lede strong{color:var(--text)}
.sub-stats{display:grid;grid-template-columns:repeat(3,1fr);max-width:540px;border:1px solid var(--border-2)}
.sub-stat{padding:20px 24px;background:var(--surface-2)}
.sub-stat:not(:last-child){border-right:1px solid var(--border)}
.sub-stat-val{display:block;font-family:var(--serif);font-size:32px;font-weight:500;letter-spacing:-.02em;line-height:1.1;color:var(--text)}
.sub-stat-label{display:block;font-family:var(--mono);font-size:10px;letter-spacing:.07em;text-transform:uppercase;color:var(--muted);margin-top:6px}
.sub-body{padding:56px 0;background:var(--base)}
.sub-body-inner{padding:0 56px}
.sub-two-col{display:grid;grid-template-columns:1fr 320px;gap:48px;margin-bottom:56px}
.sub-sec-head{display:flex;align-items:center;justify-content:space-between;margin-bottom:18px;padding-bottom:14px;border-bottom:1px solid var(--border-2)}
.sub-sec-eyebrow{font-family:var(--mono);font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--muted);display:flex;align-items:center;gap:6px}
.sub-sec-count{font-family:var(--mono);font-size:11px;color:var(--muted)}
.sub-awarded-sec{margin-bottom:56px}
.sub-empty{font-size:14px;color:var(--muted);padding:28px 0;font-family:var(--mono)}
.sub-cta-row{display:flex;align-items:center;gap:28px;padding:36px;border:1px solid var(--border-2);background:var(--surface-2)}
.sub-cta-text{font-family:var(--sans);font-size:17px;line-height:1.5;flex:1;color:var(--text)}
.live-dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--green);flex-shrink:0;animation:ldpulse 2.4s ease-in-out infinite}
@keyframes ldpulse{0%{box-shadow:0 0 0 0 rgba(34,197,94,.4)}70%{box-shadow:0 0 0 6px rgba(34,197,94,0)}100%{box-shadow:0 0 0 0 rgba(34,197,94,0)}}
.ls-table{width:100%;border-collapse:collapse;font-size:13.5px;margin-top:4px}
.ls-table th{font-family:var(--mono);font-size:10px;letter-spacing:.09em;text-transform:uppercase;color:var(--muted);text-align:left;padding:0 8px 12px 0;border-bottom:1px solid var(--border-2)}
.ls-table th.ls-th-r{text-align:right}
.ls-table td{padding:13px 8px 13px 0;border-bottom:1px solid var(--border);vertical-align:top}
.ls-title-cell{overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}
.ls-table a{color:var(--brand);text-decoration:underline;text-decoration-color:var(--brand-dim)}
.ls-table a:hover{text-decoration-color:var(--brand)}
.ls-buyer{color:var(--muted);font-size:12.5px}
.ls-val{font-family:var(--mono);font-size:12.5px;white-space:nowrap;text-align:right;padding-left:24px}
.ls-date{font-family:var(--mono);font-size:12.5px;color:var(--muted);white-space:nowrap;text-align:right;padding-left:16px}
.ls-foot{font-family:var(--mono);font-size:11px;color:var(--muted);margin-top:14px}
.bw-row{display:flex;gap:12px;padding:16px 0;border-bottom:1px solid var(--border)}
.bw-row:last-of-type{border-bottom:none}
.bw-avatar{width:40px;height:40px;border-radius:4px;background:var(--surface-3);color:var(--muted);font-family:var(--mono);font-size:10px;display:flex;align-items:center;justify-content:center;letter-spacing:.04em;flex-shrink:0;margin-top:1px}
.bw-info{flex:1;min-width:0}
.bw-name{font-size:13px;font-weight:500;line-height:1.35;margin-bottom:5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:var(--text)}
.bw-tag{font-family:var(--mono);font-size:9.5px;letter-spacing:.05em;padding:2px 7px;border-radius:2px;display:inline-block;margin-bottom:5px}
.bw-tag-health{background:#0e2a1f;color:#4dba8a;border:1px solid rgba(45,155,111,.2)}
.bw-tag-la{background:#0e1a2e;color:#6090d4;border:1px solid rgba(96,144,212,.2)}
.bw-tag-gov{background:#261c0e;color:#c4a35a;border:1px solid rgba(196,163,90,.2)}
.bw-tag-housing{background:#1c0e2e;color:#a07ad4;border:1px solid rgba(160,122,212,.2)}
.bw-tag-edu{background:#261a0e;color:#d4924a;border:1px solid rgba(212,146,74,.2)}
.bw-tag-other{background:rgba(255,255,255,.04);color:var(--muted);border:1px solid var(--border-2)}
.bw-meta{font-family:var(--mono);font-size:11px;color:var(--muted);line-height:1.65}
.bw-spend{font-size:14px;color:var(--text);font-family:var(--mono);font-weight:600;margin-right:2px}
.bw-meta-label{font-size:11px;color:var(--muted)}
.btn-cta{display:inline-flex;align-items:center;gap:8px;background:var(--brand);color:#fff;font-family:var(--mono);font-size:12px;letter-spacing:.12em;text-transform:uppercase;padding:15px 24px;transition:.18s;flex-shrink:0}
.btn-cta:hover{background:var(--brand-hot)}
.dm-sources-bar{background:var(--surface);border-top:1px solid var(--border)}
.dm-sources-inner{padding:16px 56px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px}
.dm-sources-left{font-size:12.5px;color:var(--muted)}
.dm-sources-right{display:flex;gap:4px;align-items:center;flex-wrap:wrap}
.dm-src-label{font-family:var(--mono);font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);margin-right:6px}
.dm-src-link{font-family:var(--mono);font-size:11px;color:var(--muted);text-decoration:underline;text-decoration-color:var(--border-2);padding:0 6px}
.dm-src-link:hover{color:var(--text)}
.dm-foot-copy{text-align:center;font-family:var(--mono);font-size:10.5px;letter-spacing:.06em;color:var(--muted);padding:12px 0 16px;border-top:1px solid var(--border)}
@media(max-width:1100px){.sub-two-col{grid-template-columns:1fr}}
@media(max-width:760px){
  .gh-inner,.sub-mast-inner,.sub-body-inner,.dm-sources-inner{padding-left:16px;padding-right:16px}
  .sub-mast{padding:32px 0 28px}
  .sub-mast h1{font-size:30px}
  .sub-stats{grid-template-columns:1fr 1fr}
  .sub-two-col{grid-template-columns:1fr}
  .ls-val,.ls-date,.ls-buyer{display:none}
  .dm-sources-inner{flex-direction:column;align-items:flex-start;gap:6px;padding-top:14px;padding-bottom:14px}
  .dm-sources-right{flex-wrap:wrap}
  .ls-table{display:block;overflow-x:auto;-webkit-overflow-scrolling:touch}
}
@media(max-width:480px){
  .sub-stats{grid-template-columns:1fr}
}
</style>
</head>
<body>
${pageShellHeader(profile, authCtx)}

<section class="sub-mast">
  <div class="sub-mast-inner">
    <div class="sub-crumb">
      <a href="/desk/${profile.slug}">${escapeHtml(profile.label)}</a>
      <span class="sub-crumb-sep">&rsaquo;</span>
      <span>${escapeHtml(cat.label)}</span>
      <span class="sub-crumb-sep">&rsaquo;</span>
      <span class="sub-crumb-active">${escapeHtml(subLabel)}</span>
    </div>
    <h1>${escapeHtml(subLabel.toUpperCase())}</h1>
    <p class="sub-lede">Public procurement activity for <strong>${escapeHtml(subLabel)}</strong> across the UK public sector.</p>
    <div class="sub-stats">
      <div class="sub-stat">
        <span class="sub-stat-val">${isCompiling ? "—" : totalValue > 0 ? fmtMoney(totalValue)+"+" : "—"}</span>
        <span class="sub-stat-label">Awarded value</span>
      </div>
      <div class="sub-stat">
        <span class="sub-stat-val">${isCompiling ? "—" : String(recentOpen.length)}</span>
        <span class="sub-stat-label">Open opportunities</span>
      </div>
      <div class="sub-stat">
        <span class="sub-stat-val">${isCompiling ? "—" : String(buyerMap.size)}</span>
        <span class="sub-stat-label">Unique buyers</span>
      </div>
    </div>
  </div>
</section>

<section class="sub-body">
  <div class="sub-body-inner">
    <div class="sub-two-col">
      <div class="sub-col-main">
        <div class="sub-sec-head">
          <span class="sub-sec-eyebrow"><span class="live-dot"></span>&nbsp;Live Signal</span>
          <span class="sub-sec-count">${isCompiling ? "compiling…" : `${recentOpen.length} open`}</span>
        </div>
        ${isCompiling
          ? `<p class="sub-empty">Compiling &mdash; check back in 90 seconds.</p>`
          : recentOpen.length > 0
            ? `<table class="ls-table">
                <thead><tr>
                  <th>Notice</th><th class="ls-buyer">Buyer</th>
                  <th class="ls-val ls-th-r">Value</th>
                  <th class="ls-date ls-th-r">Posted</th>
                </tr></thead>
                <tbody>${openRowsHtml}</tbody>
              </table>
              ${recentOpen.length > 30 ? `<p class="ls-foot">Showing 30 of ${recentOpen.length} matched notices.</p>` : ""}`
            : `<p class="sub-empty">No open opportunities found matching this subcategory. Data refreshes every 24 hours.</p>`
        }
      </div>
      <div class="sub-col-side">
        <div class="sub-sec-head">
          <span class="sub-sec-eyebrow">Buyer Watchlist</span>
          <span class="sub-sec-count">${isCompiling ? "&mdash;" : `${buyerMap.size} buyers`}</span>
        </div>
        ${isCompiling
          ? `<p class="sub-empty">Compiling&hellip;</p>`
          : topBuyers.length > 0
            ? buyerRowsHtml
            : `<p class="sub-empty">No buyers found yet.</p>`
        }
      </div>
    </div>

    ${!isCompiling && allAwarded.length > 0 ? `
    <div class="sub-awarded-sec">
      <div class="sub-sec-head">
        <span class="sub-sec-eyebrow">Awarded Contracts</span>
        <span class="sub-sec-count">${allAwarded.length} awarded</span>
      </div>
      <table class="ls-table">
        <thead><tr>
          <th>Notice</th><th class="ls-buyer">Buyer</th>
          <th class="ls-val ls-th-r">Value</th>
          <th class="ls-date ls-th-r">Awarded</th>
        </tr></thead>
        <tbody>${awardedRowsHtml}</tbody>
      </table>
      ${allAwarded.length > 25 ? `<p class="ls-foot">Showing 25 of ${allAwarded.length} matched awarded contracts.</p>` : ""}
    </div>` : ""}

    <div class="sub-cta-row">
      <p class="sub-cta-text">Run a scan targeting <strong>${escapeHtml(subLabel)}</strong> to get a full commercial intelligence report and bid opportunities for your firm.</p>
      <a class="btn-cta" href="/scan">RUN A SCAN &nbsp;&rarr;</a>
    </div>
  </div>
</section>

<div class="dm-sources-bar">
  <div class="dm-sources-inner">
    <span class="dm-sources-left">Public record. No insider information. Always verify on the source.</span>
    <div class="dm-sources-right">
      <span class="dm-src-label">SOURCES</span>
      <a class="dm-src-link" href="https://www.contractsfinder.service.gov.uk" target="_blank" rel="noopener noreferrer">Contracts Finder &#8599;</a>
      <a class="dm-src-link" href="https://www.find-tender.service.gov.uk" target="_blank" rel="noopener noreferrer">Find a Tender &#8599;</a>
    </div>
    <span style="font-family:var(--mono);font-size:10.5px;color:var(--muted)">Caveat: Data is indicative, not exhaustive.</span>
  </div>
</div>
<div class="dm-foot-copy"><a href="/" style="color:inherit;text-decoration:underline;text-decoration-color:var(--border)">&larr; GovRevenue</a> &nbsp;&middot;&nbsp; &copy; 2026 GovRevenue &middot; Intelligence, not certainty. Public data only.</div>

</body>
</html>`;
}

// ─── shared page shell ────────────────────────────────────────────────────────

function pageShellCss(): string {
  return `
@import url('https://fonts.googleapis.com/css2?family=Newsreader:ital,opsz,wght@0,6..72,400;0,6..72,500;0,6..72,600;1,6..72,400;1,6..72,500&family=Libre+Franklin:wght@400;500;600;700&family=Spline+Sans+Mono:wght@400;500;600&display=swap');
:root{
  --base:#ECE7DA;--surface:#FBF9F3;--surface-2:#F6F2E8;--surface-3:#EFEADD;
  --hero-1:#0A1C12;--hero-2:#0E2417;--hero-3:#16341F;--hero-cta:#102A1E;
  --brand:#B4924E;--brand-hot:#C4933F;--brand-dim:rgba(180,146,78,.12);
  --green:#2F8A52;--gold:#B4924E;--red:#9b2d20;--info:#1d4ed8;
  --text:#1B1E19;--text-mid:#3A3E36;--muted:#86897E;--faint:#9AA093;
  --border:rgba(27,30,25,.1);--border-2:rgba(27,30,25,.16);--border-3:rgba(27,30,25,.22);
  --sans:"Libre Franklin",system-ui,-apple-system,sans-serif;
  --mono:"Spline Sans Mono",ui-monospace,monospace;
  --serif:"Newsreader",Georgia,serif;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--base);color:var(--text);font-family:var(--sans);font-size:15px;line-height:1.55;-webkit-font-smoothing:antialiased;text-rendering:optimizeLegibility;overflow-x:hidden;-webkit-text-size-adjust:100%}
::selection{background:var(--hero-cta);color:#F3EFE6}
a{color:inherit;text-decoration:none}
/* ── global header ── */
.gh{background:rgba(236,231,218,0.92);backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);border-bottom:1px solid var(--border-2);position:sticky;top:0;z-index:50}
.gh-inner{padding:0 32px;max-width:1200px;margin:0 auto}
.gh-top{display:flex;align-items:center;justify-content:space-between;height:60px;gap:24px}
.gh-brand{display:flex;align-items:center;gap:9px;flex-shrink:0}
.gh-dot{width:10px;height:10px;background:var(--brand);border-radius:50%;flex-shrink:0}
.gh-logo{font-family:var(--serif);font-weight:500;font-size:20px;letter-spacing:-0.01em;color:var(--text)}
.gh-logo b{color:var(--brand);font-weight:500}
.gh-tag{font-family:var(--mono);font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);border-left:1px solid var(--border-2);padding-left:14px;margin-left:6px}
.gh-live{display:inline-flex;align-items:center;gap:6px;font-family:var(--mono);font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:var(--green)}
.gh-live-dot{width:6px;height:6px;border-radius:50%;background:var(--green);animation:gh-pulse 2.2s infinite}
@keyframes gh-pulse{0%{box-shadow:0 0 0 0 rgba(47,138,82,.4)}70%{box-shadow:0 0 0 5px rgba(47,138,82,0)}100%{box-shadow:0 0 0 0 rgba(47,138,82,0)}}
.gh-nav{display:flex;gap:0;overflow-x:auto;scrollbar-width:none;border-top:1px solid var(--border)}
.gh-nav::-webkit-scrollbar{display:none}
.gh-nav a{font-size:13.5px;font-weight:500;color:var(--text-mid,#3A3E36);padding:0 14px;height:38px;display:flex;align-items:center;border-bottom:2px solid transparent;white-space:nowrap;transition:color .15s}
.gh-nav a:hover{color:var(--text)}
.gh-nav a.dnav-active{color:var(--text);border-bottom-color:var(--brand)}
.gh-auth{display:flex;align-items:center;gap:16px;flex-shrink:0}
.gh-auth-name{font-family:var(--mono);font-size:10px;color:var(--muted);max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.gh-auth-link{font-size:14px;font-weight:500;color:var(--text-mid,#3A3E36);transition:color .15s}
.gh-auth-link:hover{color:var(--text)}
.gh-auth-cta{display:inline-flex;align-items:center;background:var(--hero-cta);color:#F3EFE6;font-size:13px;font-weight:600;padding:9px 16px;letter-spacing:.01em;transition:opacity .15s}
.gh-auth-cta:hover{opacity:.85}
.gh-main-nav{display:flex;align-items:center;flex:1;padding:0 8px;overflow-x:auto;scrollbar-width:none}
.gh-main-nav::-webkit-scrollbar{display:none}
.gh-main-nav a{font-size:13px;font-weight:500;color:var(--text-mid,#B0BAC8);padding:0 11px;height:60px;display:flex;align-items:center;white-space:nowrap;transition:color .15s}
.gh-main-nav a:hover{color:var(--text)}
/* ── page masthead ── */
.pg-mast{padding:40px 0 32px;border-bottom:1px solid var(--border-2);background:var(--base)}
.pg-mast-inner{padding:0 32px;max-width:1200px;margin:0 auto}
.pg-eyebrow{font-family:var(--mono);font-size:11px;letter-spacing:.18em;text-transform:uppercase;color:var(--brand);margin-bottom:10px}
.pg-crumb{font-family:var(--mono);font-size:10.5px;letter-spacing:.04em;color:var(--muted);margin-bottom:14px;display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.pg-crumb a{color:var(--muted)}.pg-crumb a:hover{color:var(--text)}
.pg-crumb-sep{color:var(--faint)}.pg-crumb-active{color:var(--text)}
.pg-mast h1{font-family:var(--serif);font-size:32px;font-weight:500;letter-spacing:-.02em;line-height:1.1;margin-bottom:20px;color:var(--text)}
.pg-stats{display:flex;gap:1px;background:var(--border-2);border:1px solid var(--border-2);width:fit-content;flex-wrap:wrap}
.pg-stat{padding:14px 22px;background:var(--surface)}
.pg-stat-val{display:block;font-family:var(--serif);font-size:26px;font-weight:500;letter-spacing:-.01em;line-height:1.1;color:var(--text)}
.pg-stat-label{display:block;font-family:var(--mono);font-size:9.5px;letter-spacing:.12em;text-transform:uppercase;color:var(--muted);margin-top:4px}
/* ── page body ── */
.pg-body{padding:40px 0 64px;background:var(--base)}
.pg-body-inner{padding:0 32px;max-width:1200px;margin:0 auto}
/* ── status tags ── */
.bw-tag{font-family:var(--mono);font-size:9px;letter-spacing:.06em;text-transform:uppercase;padding:2px 7px;display:inline-block;border:1px solid}
.bw-tag-health{background:rgba(29,107,79,.08);color:#1d6b4f;border-color:rgba(29,107,79,.22)}
.bw-tag-la{background:rgba(29,78,216,.08);color:#1d4ed8;border-color:rgba(29,78,216,.2)}
.bw-tag-gov{background:rgba(180,146,78,.1);color:#7a5a22;border-color:rgba(180,146,78,.28)}
.bw-tag-housing{background:rgba(109,40,217,.07);color:#6d28d9;border-color:rgba(109,40,217,.2)}
.bw-tag-edu{background:rgba(180,83,9,.07);color:#b45309;border-color:rgba(180,83,9,.2)}
.bw-tag-other{background:var(--surface-2);color:var(--muted);border-color:var(--border-2)}
.pg-empty{font-family:var(--mono);font-size:12px;color:var(--muted);padding:40px 0}
/* ── opportunity card ── */
.opp-card{background:var(--surface);border:1px solid var(--border-2);padding:16px 20px;margin-bottom:8px;transition:border-color .15s}
.opp-card:hover{border-color:var(--brand)}
/* ── footer ── */
.pg-foot{background:var(--hero-cta)}
.pg-foot-inner{padding:16px 32px;max-width:1200px;margin:0 auto;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;font-family:var(--mono);font-size:10.5px;color:rgba(236,230,214,.55)}
.pg-copy{text-align:center;font-family:var(--mono);font-size:10px;letter-spacing:.06em;color:rgba(236,230,214,.3);padding:10px 0 14px;background:#081710}
@media(max-width:760px){
  .gh-tag,.gh-live,.gh-auth-name,.gh-main-nav{display:none}
  .gh-inner,.pg-mast-inner,.pg-body-inner,.pg-foot-inner{padding-left:16px;padding-right:16px}
  .gh-auth{gap:10px}
  .gh-auth-cta{padding:8px 12px;font-size:12px}
  .gh-auth-link{font-size:13px}
  .gh-nav{padding-left:0}
  .pg-mast{padding:24px 0 20px}
  .pg-mast h1{font-size:22px}
  .pg-stats{width:100%;flex-wrap:wrap}
  .pg-stat{flex:1 1 45%}
  .pg-foot-inner{flex-direction:column;text-align:center;gap:6px;padding:14px 16px}
}
@media(max-width:480px){
  .pg-stat{flex:1 1 100%}
  .gh-auth-cta{padding:7px 10px;font-size:11px}
}`;
}

function pageShellHeader(profile: DeskProfile | null, authCtx?: { email: string; tier: UserTier } | null): string {
  const navLinks = DESK_PROFILES.map(d =>
    `<a href="/desk/${d.slug}"${profile && d.slug === profile.slug ? ' class="dnav-active"' : ""}>${escapeHtml(d.label)}</a>`
  ).join("");
  const authHtml = authCtx
    ? `<div class="gh-auth"><span class="gh-auth-name">${escapeHtml(authCtx.email)}</span><a href="/account" class="gh-auth-link">Dashboard</a><a href="/logout" class="gh-auth-link">Sign out</a></div>`
    : `<div class="gh-auth"><a href="/login" class="gh-auth-link">Sign in</a><a href="/scan" class="gh-auth-cta">Run a scan</a></div>`;
  return `<header class="gh">
  <div class="gh-inner">
    <div class="gh-top">
      <div class="gh-brand">
        <div class="gh-dot"></div>
        <a href="/" class="gh-logo">Gov<b>Revenue</b></a>
      </div>
      <nav class="gh-main-nav">
        <a href="/desks">Desks</a>
        <a href="/signals">Signals</a>
        <a href="/charts">Intelligence</a>
        <a href="/articles">Articles</a>
        <a href="/scan">The Scan</a>
        <a href="/pricing">Pricing</a>
      </nav>
      ${authHtml}
    </div>
    <nav class="gh-nav">
      ${navLinks}
    </nav>
  </div>
</header>`;
}

function pageShellFoot(): string {
  return `<footer class="pg-foot">
  <div class="pg-foot-inner">
    <a href="/" style="font-family:var(--serif);font-size:17px;color:#ECE6D6;letter-spacing:0">Gov<span style="color:var(--brand)">Revenue</span></a>
    <span>PUBLIC RECORD ONLY &middot; INTELLIGENCE, NOT CERTAINTY</span>
    <a href="/scan" style="color:var(--brand)">Run a scan &rarr;</a>
  </div>
</footer>
<div class="pg-copy">&copy; 2026 GovRevenue &mdash; Intelligence, not certainty.</div>`;
}

function notFoundHtml(message: string, authCtx?: { email: string; tier: UserTier } | null): string {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Not found &mdash; GovRevenue</title>
<style>${pageShellCss()}</style>
</head>
<body>
${pageShellHeader(null, authCtx)}
<main style="padding:80px 40px;max-width:1320px;margin:0 auto">
  <p style="font-family:var(--mono);font-size:10px;letter-spacing:.18em;text-transform:uppercase;color:var(--brand);margin-bottom:14px">404 &mdash; Not Found</p>
  <h1 style="font-family:var(--sans);font-size:28px;font-weight:700;letter-spacing:-.02em;margin-bottom:16px;color:var(--text)">${escapeHtml(message)}</h1>
  <a href="/" style="font-family:var(--mono);font-size:11px;letter-spacing:.07em;text-transform:uppercase;color:var(--muted);border-bottom:1px solid var(--border-2);padding-bottom:2px">&larr; Back to GovRevenue</a>
</main>
${pageShellFoot()}
</body>
</html>`;
}

// ─── /desks ────────────────────────────────────────────────────────────────────

function desksPage(entries: Array<{ profile: DeskProfile; cached: { data: ProcurementData; cached_at: string } | null }>, page = 1, authCtx?: { email: string; tier: UserTier } | null): string {
  type DS = {
    profile: DeskProfile;
    openCount: number;
    awardedCount: number;
    totalValue: number;
    uniqueBuyers: number;
    topCats: InferredCategory[];
    cachedAt: string | null;
  };
  const stats: DS[] = entries.map(({ profile, cached }) => {
    if (!cached) return { profile, openCount: 0, awardedCount: 0, totalValue: 0, uniqueBuyers: 0, topCats: [], cachedAt: null };
    const open = cached.data.contractsFinder.open ?? [];
    const awarded = cached.data.contractsFinder.awarded ?? [];
    const all = [...open, ...awarded];
    const totalValue = awarded.reduce((s, n) => s + (n.awardedValue ?? 0), 0);
    const uniqueBuyers = new Set(all.map(n => n.buyer).filter((b): b is string => !!b)).size;
    const topCats = inferDeskCategories(awarded, profile.categories).filter(c => c.count > 0).sort((a, b) => b.value - a.value).slice(0, 3);
    return { profile, openCount: open.length, awardedCount: awarded.length, totalValue, uniqueBuyers, topCats, cachedAt: cached.cached_at };
  });
  const sorted = [...stats].sort((a, b) => b.totalValue - a.totalValue);
  const grandTotal = stats.reduce((s, d) => s + d.totalValue, 0);
  const totalOpen = stats.reduce((s, d) => s + d.openCount, 0);
  const totalAwarded = stats.reduce((s, d) => s + d.awardedCount, 0);
  const totalBuyers = stats.reduce((s, d) => s + d.uniqueBuyers, 0);
  const liveCount = stats.filter(d => d.cachedAt !== null).length;
  const totalNotices = totalOpen + totalAwarded;
  const PAGE_SIZE = 12;
  const totalPages = Math.max(1, Math.ceil(sorted.length / PAGE_SIZE));
  const currentPage = Math.max(1, Math.min(page, totalPages));
  const paginated = sorted.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE);

  const renderCard = (d: DS, rank: number): string => {
    const sharePct = grandTotal > 0 ? Math.round((d.totalValue / grandTotal) * 100) : 0;
    const maxCatVal = d.topCats.length > 0 ? d.topCats[0].value : 0;
    const updatedStr = d.cachedAt ? timeAgo(d.cachedAt) : null;

    const pillBlock = d.cachedAt
      ? `<span class="dl-live-pill"><span class="dl-live-dot"></span>LIVE DATA</span>`
      : `<span class="dl-no-data-pill">COMPILING</span>`;

    const shareBlock = d.cachedAt ? `
      <div class="dl-share-section">
        <div class="dl-share-lbl"><span>VALUE SHARE <span style="opacity:.55;font-size:9px;text-transform:none;letter-spacing:.03em">of indexed spend</span></span><span>${sharePct}%</span></div>
        <div class="dl-share-track"><div class="dl-share-fill" style="width:${sharePct}%"></div></div>
      </div>` : "";

    const statsGrid = `
      <div class="dl-stats">
        <div class="dl-stat"><div class="dl-stat-val">${d.totalValue > 0 ? fmtMoney(d.totalValue)+"+" : "—"}</div><div class="dl-stat-lbl">Awarded Value</div></div>
        <div class="dl-stat"><div class="dl-stat-val">${d.openCount > 0 ? d.openCount : "—"}</div><div class="dl-stat-lbl">Open Now</div></div>
        <div class="dl-stat"><div class="dl-stat-val">${d.awardedCount > 0 ? d.awardedCount : "—"}</div><div class="dl-stat-lbl">Awarded</div></div>
        <div class="dl-stat"><div class="dl-stat-val">${d.uniqueBuyers > 0 ? d.uniqueBuyers : "—"}</div><div class="dl-stat-lbl">Buyers</div></div>
      </div>`;

    const catsBlock = d.topCats.length > 0 ? `
      <div class="dl-cats">
        <div class="dl-cats-title">Top Categories</div>
        ${d.topCats.map(cat => `
          <div class="dl-cat-row"><span class="dl-cat-name">${escapeHtml(cat.label)}</span><span class="dl-cat-val">${cat.value > 0 ? fmtMoney(cat.value) : cat.count + " contracts"}</span></div>
          <div class="dl-cat-bar-wrap"><div class="dl-cat-bar-fill" style="width:${maxCatVal > 0 ? Math.round((cat.value / maxCatVal) * 100) : 0}%"></div></div>
        `).join("")}
      </div>` : "";

    return `<article class="dl-card">
      <div class="dl-card-head">
        <div class="dl-card-rank">#${rank + 1} &middot; BY VALUE</div>
        <div class="dl-card-label"><a href="/desk/${escapeHtml(d.profile.slug)}">${escapeHtml(d.profile.label)}</a></div>
        ${pillBlock}
        <p class="dl-card-stand">${escapeHtml(d.profile.standfirst)}</p>
      </div>
      ${shareBlock}${statsGrid}${catsBlock}
      <div class="dl-meta">
        <span class="dl-meta-cats">${d.profile.categories.length} ${d.profile.categories.length === 1 ? "category" : "categories"}</span>
        ${updatedStr ? `<span class="dl-meta-updated">Updated ${escapeHtml(updatedStr)}</span>` : `<a class="dl-meta-link" href="/desk/${escapeHtml(d.profile.slug)}">View desk →</a>`}
      </div>
    </article>`;
  };

  const cards = paginated.map((d, i) => renderCard(d, (currentPage - 1) * PAGE_SIZE + i)).join("");

  const pagerLinks = Array.from({ length: totalPages }, (_, i) => i + 1).map(p => {
    if (totalPages > 7 && Math.abs(p - currentPage) > 2 && p !== 1 && p !== totalPages) {
      return p === currentPage - 3 || p === currentPage + 3 ? `<span class="dl-pager-ellipsis">&hellip;</span>` : "";
    }
    return `<a href="/desks?page=${p}" class="dl-pager-btn${p === currentPage ? " dl-pager-active" : ""}">${p}</a>`;
  }).filter(Boolean).join("");

  const pagerNav = totalPages > 1 ? `
    <nav class="dl-pager">
      ${currentPage > 1 ? `<a href="/desks?page=${currentPage - 1}" class="dl-pager-btn dl-pager-arrow">&larr; Prev</a>` : `<span class="dl-pager-btn dl-pager-disabled">&larr; Prev</span>`}
      ${pagerLinks}
      ${currentPage < totalPages ? `<a href="/desks?page=${currentPage + 1}" class="dl-pager-btn dl-pager-arrow">Next &rarr;</a>` : `<span class="dl-pager-btn dl-pager-disabled">Next &rarr;</span>`}
    </nav>` : "";
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>All Intelligence Desks — GovRevenue</title>
<meta name="description" content="UK public-sector procurement intelligence across ${DESK_PROFILES.length} industry desks. Live data from Contracts Finder and Find a Tender.">
<style>
${pageShellCss()}
.dl-hero{background:radial-gradient(120% 170% at 82% 0%,#16341F 0%,#0E2417 60%,#0A1C12 100%);color:#ECE6D6;padding:60px 0 56px}
.dl-hero-inner,.dl-body-inner{padding:0 56px}
.dl-eyebrow{font-family:var(--mono);font-size:10px;letter-spacing:.18em;text-transform:uppercase;color:var(--muted);margin-bottom:18px}
.dl-hero h1{font-family:var(--serif);font-size:clamp(38px,4.8vw,62px);font-weight:400;line-height:1.0;letter-spacing:-.02em;margin-bottom:14px;color:#ECE6D6}
.dl-hero-sub{font-size:16px;color:#C5C9BC;max-width:540px;line-height:1.6;margin-bottom:36px}
.dl-agg{display:grid;grid-template-columns:repeat(5,1fr);gap:1px;background:rgba(236,230,214,.16);border:1px solid rgba(236,230,214,.16);overflow:hidden;transform:translateY(28px)}
.dl-agg-stat{padding:18px 20px;background:#0C1F15}
.dl-agg-val{font-family:var(--serif);font-size:26px;font-weight:500;color:#ECE6D6;margin-bottom:4px;letter-spacing:-.01em}
.dl-agg-lbl{font-family:var(--mono);font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:var(--muted)}
.dl-body{background:var(--base);padding:60px 0 80px}
.dl-sort-bar{display:flex;align-items:baseline;justify-content:space-between;margin-bottom:36px;padding-bottom:16px;border-bottom:1px solid var(--border-2)}
.dl-sort-title{font-family:var(--sans);font-size:26px;font-weight:700;color:var(--text)}
.dl-sort-meta{font-family:var(--mono);font-size:11px;color:var(--muted);letter-spacing:.06em}
.dl-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:24px}
.dl-card{background:var(--surface);border:1px solid var(--border-2);border-radius:2px;display:flex;flex-direction:column;transition:border-color .15s,box-shadow .15s;position:relative;cursor:pointer}
.dl-card:hover{background:var(--hero-cta)!important;border-color:var(--hero-cta);box-shadow:0 4px 24px rgba(0,0,0,.12)}.dl-card:hover .dl-card-label a{color:#ECE6D6}.dl-card:hover .dl-card-rank,.dl-card:hover .dl-card-meta,.dl-card:hover .dl-card-stat-label{color:#9AA093}.dl-card:hover .dl-live-pill{background:rgba(47,138,82,.2);color:#4ade80}
.dl-card-head{padding:28px 28px 20px}
.dl-card-rank{font-family:var(--mono);font-size:10px;letter-spacing:.12em;text-transform:uppercase;color:var(--muted);margin-bottom:10px}
.dl-card-label{font-family:var(--serif);font-size:20px;font-weight:500;line-height:1.15;letter-spacing:-.01em;margin-bottom:8px}
.dl-card-label a{color:var(--text)}
.dl-card-label a:hover{color:var(--brand)}
.dl-card-label a::after{content:'';position:absolute;inset:0;z-index:0}
.dl-meta a,.dl-cats,.dl-stats{position:relative;z-index:1}
.dl-live-pill{display:inline-flex;align-items:center;gap:5px;font-family:var(--mono);font-size:9px;letter-spacing:.1em;text-transform:uppercase;color:var(--green);background:rgba(47,138,82,.1);border:1px solid rgba(47,138,82,.2);padding:3px 8px;margin-bottom:10px}
.dl-live-dot{width:5px;height:5px;border-radius:50%;background:var(--green);animation:dlPulse 2s infinite}
@keyframes dlPulse{0%,100%{opacity:1}50%{opacity:.4}}
.dl-no-data-pill{display:inline-flex;align-items:center;gap:5px;font-family:var(--mono);font-size:9px;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);background:rgba(255,255,255,.05);padding:3px 8px;border-radius:100px;margin-bottom:10px}
.dl-card-stand{font-size:13px;color:var(--muted);line-height:1.5}
.dl-share-section{padding:0 28px 20px}
.dl-share-lbl{display:flex;justify-content:space-between;font-family:var(--mono);font-size:10px;letter-spacing:.07em;text-transform:uppercase;color:var(--muted);margin-bottom:7px}
.dl-share-track{height:3px;background:rgba(255,255,255,.06);border-radius:2px;overflow:hidden}
.dl-share-fill{height:100%;background:var(--brand);border-radius:2px}
.dl-stats{display:grid;grid-template-columns:1fr 1fr;gap:1px;background:var(--border);border-top:1px solid var(--border);border-bottom:1px solid var(--border)}
.dl-stat{background:var(--surface-2);padding:16px 28px}
.dl-stat-val{font-family:var(--mono);font-size:22px;color:var(--text);margin-bottom:2px}
.dl-stat-lbl{font-family:var(--mono);font-size:9px;letter-spacing:.1em;text-transform:uppercase;color:var(--muted)}
.dl-cats{padding:20px 28px}
.dl-cats-title{font-family:var(--mono);font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);margin-bottom:12px}
.dl-cat-row{display:flex;justify-content:space-between;align-items:center;gap:10px;margin-bottom:4px}
.dl-cat-name{font-size:12px;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0}
.dl-cat-val{font-family:var(--mono);font-size:11px;color:var(--muted);flex-shrink:0}
.dl-cat-bar-wrap{height:2px;background:rgba(255,255,255,.06);border-radius:1px;margin-bottom:8px}
.dl-cat-bar-fill{height:100%;background:var(--brand);opacity:.55;border-radius:1px}
.dl-meta{padding:16px 28px;border-top:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;margin-top:auto;font-family:var(--mono);font-size:10px;letter-spacing:.06em}
.dl-meta-cats,.dl-meta-updated{color:var(--muted)}
.dl-meta-link{color:var(--brand)}
@media(max-width:1100px){.dl-agg{grid-template-columns:repeat(3,1fr)}.dl-grid{grid-template-columns:repeat(2,1fr)}}
@media(max-width:760px){.dl-hero-inner,.dl-body-inner{padding-left:16px;padding-right:16px}.dl-hero{padding:48px 0 40px}.dl-hero h1{font-size:34px}.dl-agg{grid-template-columns:1fr 1fr}.dl-agg-val{font-size:26px}.dl-sort-bar{flex-direction:column;gap:6px}.dl-grid{grid-template-columns:1fr;gap:16px}.dl-body{padding:36px 0 56px}}
.dl-pager{display:flex;align-items:center;justify-content:center;gap:6px;padding:48px 0 0;flex-wrap:wrap}
.dl-pager-btn{font-family:var(--mono);font-size:12px;letter-spacing:.07em;padding:8px 14px;border:1px solid var(--border-2);border-radius:2px;color:var(--text);transition:border-color .15s,background .15s}
.dl-pager-btn:hover{border-color:var(--brand);background:var(--brand-dim)}
.dl-pager-active{background:var(--brand);color:#fff;border-color:var(--brand)}
.dl-pager-active:hover{background:var(--brand);color:#fff}
.dl-pager-disabled{font-family:var(--mono);font-size:12px;letter-spacing:.07em;padding:8px 14px;border:1px solid var(--border);border-radius:2px;color:var(--muted);cursor:default}
.dl-pager-ellipsis{font-family:var(--mono);font-size:12px;color:var(--muted);padding:0 4px}
.dl-pager-arrow{font-size:11px}
</style>
</head>
<body>
${pageShellHeader(null, authCtx)}
<section class="dl-hero">
  <div class="dl-hero-inner">
    <div class="dl-eyebrow">All Intelligence Desks</div>
    <h1>UK Public-Sector<br>Contract Intelligence</h1>
    <p class="dl-hero-sub">Live procurement data across ${liveCount} active desks. Sourced from Contracts Finder and Find a Tender. Updated continuously.</p>
    <div class="dl-agg">
      <div class="dl-agg-stat"><div class="dl-agg-val">${grandTotal > 0 ? fmtMoney(grandTotal)+"+" : "—"}</div><div class="dl-agg-lbl">Total Contract Value</div></div>
      <div class="dl-agg-stat"><div class="dl-agg-val">${totalOpen > 0 ? totalOpen.toLocaleString() : "—"}</div><div class="dl-agg-lbl">Open Now</div></div>
      <div class="dl-agg-stat"><div class="dl-agg-val">${totalAwarded > 0 ? totalAwarded.toLocaleString() : "—"}</div><div class="dl-agg-lbl">Awarded</div></div>
      <div class="dl-agg-stat"><div class="dl-agg-val">${totalBuyers > 0 ? totalBuyers.toLocaleString() : "—"}</div><div class="dl-agg-lbl">Buyers Tracked</div></div>
      <div class="dl-agg-stat"><div class="dl-agg-val">${liveCount}<span style="font-size:18px;opacity:.5">/${DESK_PROFILES.length}</span></div><div class="dl-agg-lbl">Desks Live</div></div>
    </div>
  </div>
</section>
<section class="dl-body">
  <div class="dl-body-inner">
    <div class="dl-sort-bar">
      <div class="dl-sort-title">All Desks — ranked by contract value</div>
      <div class="dl-sort-meta">${DESK_PROFILES.length} desks &middot; ${totalNotices.toLocaleString()} notices indexed</div>
    </div>
    <div class="dl-grid">${cards}</div>
    ${pagerNav}
  </div>
</section>
${pageShellFoot()}
</body>
</html>`;
}

// ─── /desk/:slug/notices ──────────────────────────────────────────────────────

function noticesPage(
  profile: DeskProfile,
  cached: { data: ProcurementData; cached_at: string } | null,
  _buyerFilter: string | null = null,
  authCtx?: { email: string; tier: UserTier } | null
): string {
  const data = cached?.data;
  const isCompiling = cached === null;

  const boardKw = profile.categories.flatMap(c => c.keywords);
  const allOpen = dedupeNoticesSoft(
    (data?.contractsFinder.open || [])
      .concat(data?.findTender?.notices || [])
      .filter(n => !isAggregatorBuyer(n.buyer || "") && !isOverseasNotice(n.title, n.buyer || "") && boardKw.some(kw => n.title.toLowerCase().includes(kw)))
  ).sort((a, b) => new Date(b.publishedDate || b.awardedDate || "").getTime() - new Date(a.publishedDate || a.awardedDate || "").getTime());

  const allAwarded = (data?.contractsFinder.awarded || [])
    .sort((a, b) => new Date(b.awardedDate || b.publishedDate || "").getTime() - new Date(a.awardedDate || a.publishedDate || "").getTime());

  const totalValue = allAwarded.reduce((s, n) => s + (n.awardedValue ?? 0), 0);
  const uniqueBuyers = new Set([...allOpen, ...allAwarded].map(n => n.buyer).filter(Boolean)).size;

  const boardOppContext: DeskOpportunityContext = {
    type: "desk",
    slug: profile.slug,
    label: profile.label,
    keywords: profile.categories.flatMap(c => c.keywords),
  };

  const scoredOpen = allOpen.length > 0
    ? scoreAndBucketNotices(allOpen.map(normaliseFromProcurementNotice), boardOppContext)
    : [];
  const scoredAwarded = allAwarded.length > 0
    ? scoreAndBucketNotices(allAwarded.map(normaliseFromProcurementNotice), boardOppContext)
    : [];

  // Bucket counts for sidebar filter
  const bucketCounts = new Map<string, number>();
  for (const n of scoredOpen) {
    bucketCounts.set(n.bucket, (bucketCounts.get(n.bucket) || 0) + 1);
  }
  const chaseNowCount = bucketCounts.get("chase_now") || 0;
  const closingThisWeek = scoredOpen.filter(n => {
    if (!n.deadlineDate) return false;
    const days = Math.floor((new Date(n.deadlineDate).getTime() - Date.now()) / 86_400_000);
    return days >= 0 && days <= 7;
  }).length;


  const boardContent = isCompiling
    ? `<div class="nb-empty" style="padding:40px 32px;text-align:center">
        <strong>Compiling the public record.</strong> Run a scan while this desk warms up.<br><br>
        <a href="/scan" style="font-family:var(--mono);font-size:11px;color:var(--brand);text-decoration:underline">Run a fit check &rarr;</a>
       </div>`
    : renderOpportunityBoardContent(scoredOpen, profile.slug, scoredAwarded);

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>Opportunity Board &mdash; ${escapeHtml(profile.label)} &mdash; GovRevenue</title>
<style>
${pageShellCss()}
/* ── Dark theme override for opportunity board ── */
:root{
  --base:#0B1018;--surface:#111A26;--surface-2:#18222F;--surface-3:#1E2A3A;
  --brand:#B4924E;--brand-hot:#C4933F;--brand-dim:rgba(180,146,78,.14);
  --green:#22C55E;--gold:#B4924E;--red:#EF4444;--info:#60A5FA;
  --text:#E9EEF5;--text-mid:#C8C0AE;--muted:#9AA093;--faint:#6B7280;
  --border:rgba(255,255,255,.06);--border-2:rgba(255,255,255,.10);--border-3:rgba(255,255,255,.16);
  /* bridge tokens for opportunity engine */
  --paper:var(--surface);--paper-2:var(--surface-2);
  --ink:var(--text);--slate:var(--muted);
  --accent:var(--brand);--line:var(--border);--line-strong:var(--border-2);
}
body{background:var(--base);color:var(--text)}
.gh{background:rgba(11,16,24,.92)}
.gh-nav a{color:var(--text-mid)}
.gh-nav a.dnav-active{color:var(--text);border-bottom-color:var(--brand)}
${oppCardCss()}
${winBriefCss()}
${noticesBoardCss()}
/* ── Opportunity board hero (notices page only) ── */
.ob-mast{background:radial-gradient(120% 160% at 80% 0%,#16341F 0%,#0E2417 60%,#0A1C12 100%);color:#ECE6D6}
.ob-mast-inner{padding:48px 48px 0;max-width:1400px;margin:0 auto}
.ob-crumb{font-family:var(--mono);font-size:11px;letter-spacing:.05em;margin-bottom:20px;display:flex;align-items:center;gap:8px}
.ob-crumb a{color:rgba(236,230,214,.5);text-decoration:none;transition:color .15s}.ob-crumb a:hover{color:#ECE6D6}
.ob-crumb-sep{color:rgba(236,230,214,.25)}.ob-crumb-active{color:rgba(236,230,214,.7)}
.ob-mast h1{font-family:var(--serif);font-size:clamp(26px,3vw,40px);font-weight:400;line-height:1.05;letter-spacing:-.02em;color:#ECE6D6;margin-bottom:10px}
.ob-lede{font-size:14px;color:rgba(197,201,188,.75);margin-bottom:36px;max-width:48em}
.ob-stats{display:flex;border-top:1px solid rgba(236,230,214,.1)}
.ob-stat{flex:1;padding:22px 28px;border-right:1px solid rgba(236,230,214,.08)}.ob-stat:last-child{border-right:none}
.ob-stat-num{font-family:var(--mono);font-size:28px;font-weight:600;letter-spacing:-.02em;line-height:1;margin-bottom:7px;display:flex;align-items:center;gap:6px}
.ob-stat-label{font-family:var(--mono);font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:rgba(197,201,188,.5)}
.ob-live-dot{width:6px;height:6px;border-radius:50%;background:#4ade80;flex-shrink:0;animation:obPulse 2s infinite}
@keyframes obPulse{0%,100%{box-shadow:0 0 0 0 rgba(74,222,128,.45)}60%{box-shadow:0 0 0 5px rgba(74,222,128,0)}}
@media(max-width:768px){.ob-mast-inner{padding:32px 20px 0}.ob-stats{flex-wrap:wrap}.ob-stat{flex:1 1 50%;padding:16px 20px}.ob-stat-num{font-size:22px}}
</style>
</head>
<body>
${pageShellHeader(profile, authCtx)}

<section class="ob-mast">
  <div class="ob-mast-inner">
    <div class="ob-crumb">
      <a href="/desk/${escapeHtml(profile.slug)}">${escapeHtml(profile.label)}</a>
      <span class="ob-crumb-sep">&rsaquo;</span>
      <span class="ob-crumb-active">Opportunity Board</span>
    </div>
    <h1>OPPORTUNITY BOARD &mdash; ${escapeHtml(profile.label.toUpperCase())}</h1>
    <p class="ob-lede">Contracts you can chase now. Public notices scored against this desk profile.</p>
  </div>
  <div class="ob-stats">
    <div class="ob-stat">
      <div class="ob-stat-num" style="color:#4ade80">${isCompiling ? "—" : String(allOpen.length)}<span class="ob-live-dot"></span></div>
      <div class="ob-stat-label">Open Notices</div>
    </div>
    <div class="ob-stat">
      <div class="ob-stat-num" style="color:#D4A95A">${isCompiling ? "—" : String(chaseNowCount)}</div>
      <div class="ob-stat-label">Chase Now</div>
    </div>
    <div class="ob-stat">
      <div class="ob-stat-num" style="color:#FBBF24">${isCompiling ? "—" : String(closingThisWeek)}</div>
      <div class="ob-stat-label">Closing This Week</div>
    </div>
    <div class="ob-stat">
      <div class="ob-stat-num" style="color:#93C5FD">${isCompiling ? "—" : totalValue > 0 ? escapeHtml(fmtMoney(totalValue))+"+" : "—"}</div>
      <div class="ob-stat-label">Awarded Value</div>
    </div>
    <div class="ob-stat">
      <div class="ob-stat-num" style="color:#C5C9BC">${isCompiling ? "—" : String(uniqueBuyers)}</div>
      <div class="ob-stat-label">Unique Buyers</div>
    </div>
  </div>
</section>

<div class="nb-board-wrap">
  <div class="nb-filter-bar">
    <span class="nb-filter-label">Source</span>
    <button class="nb-filter-btn nb-active" data-src="all">All</button>
    <button class="nb-filter-btn" data-src="CF">Contracts Finder</button>
    <button class="nb-filter-btn" data-src="FTS">Find a Tender</button>
    <span class="nb-filter-sep"></span>
    <span class="nb-filter-label">Sort</span>
    <select class="nb-sort-select" id="nb-sort">
      <option value="default">Best match</option>
      <option value="deadline">Deadline — soonest</option>
      <option value="value">Value — highest</option>
      <option value="published">Published — newest</option>
    </select>
    <span id="nb-count-label" style="margin-left:auto"></span>
    <span class="nb-filter-sep"></span>
    <a href="/scan?desk=${escapeHtml(profile.slug)}" class="nb-action-btn">Run Fit Check</a>
    <a href="/desk/${escapeHtml(profile.slug)}/buyers" class="nb-action-btn">Buyer Intel</a>
  </div>
  <p class="nb-disclaimer">Public record only &middot; No insider information &middot; Matched against the ${escapeHtml(profile.label)} desk profile</p>
  ${boardContent}
</div>

${pageShellFoot()}
<script>
(function(){
  var PAGE_SIZE=12;
  var page=1;
  var activeSrc='all';
  var activeSort='default';
  var grid=document.getElementById('nb-grid');
  var paginationEl=document.getElementById('nb-pagination');
  var countLabel=document.getElementById('nb-count-label');
  if(!grid)return;

  var allCards=Array.from(grid.querySelectorAll('.nb-card'));
  // Store original sort order as data attribute
  allCards.forEach(function(c,i){c.setAttribute('data-orig',String(i));});

  function getVisible(){
    return allCards.filter(function(c){
      return activeSrc==='all'||c.getAttribute('data-src')===activeSrc;
    });
  }

  function sortCards(cards){
    if(activeSort==='default'){
      return cards.slice().sort(function(a,b){return parseInt(a.getAttribute('data-orig')||'0',10)-parseInt(b.getAttribute('data-orig')||'0',10);});
    }
    return cards.slice().sort(function(a,b){
      if(activeSort==='deadline'){
        var ta=parseInt(a.getAttribute('data-deadline-ts')||'0',10);
        var tb=parseInt(b.getAttribute('data-deadline-ts')||'0',10);
        if(ta===0&&tb===0)return 0;
        if(ta===0)return 1;
        if(tb===0)return -1;
        return ta-tb;
      }
      if(activeSort==='value') return parseInt(b.getAttribute('data-value')||'0',10)-parseInt(a.getAttribute('data-value')||'0',10);
      if(activeSort==='published') return parseInt(b.getAttribute('data-published-ts')||'0',10)-parseInt(a.getAttribute('data-published-ts')||'0',10);
      return 0;
    });
  }

  function render(){
    var visible=getVisible();
    var sorted=sortCards(visible);
    var totalPages=Math.max(1,Math.ceil(sorted.length/PAGE_SIZE));
    if(page>totalPages)page=1;
    var start=(page-1)*PAGE_SIZE;
    var pageCards=sorted.slice(start,start+PAGE_SIZE);

    // Show/hide
    allCards.forEach(function(c){c.style.display='none';});
    pageCards.forEach(function(c){c.style.display='';grid.appendChild(c);});

    // Count label
    if(countLabel){
      var end=Math.min(start+PAGE_SIZE,sorted.length);
      countLabel.textContent=sorted.length>0?(start+1)+'–'+end+' of '+sorted.length+' notices':'No notices match';
    }

    // Pagination
    if(!paginationEl)return;
    if(totalPages<=1){paginationEl.innerHTML='';return;}
    var nums='';
    var lo=Math.max(1,page-2);
    var hi=Math.min(totalPages,page+2);
    if(lo>1)nums+='<button class="nb-pg-num" data-p="1">1</button>'+(lo>2?'<span style="padding:6px 4px;font-family:var(--mono);font-size:11px;color:var(--muted)">&hellip;</span>':'');
    for(var i=lo;i<=hi;i++){nums+='<button class="nb-pg-num'+(i===page?' nb-pg-active':'')+'" data-p="'+i+'">'+i+'</button>';}
    if(hi<totalPages)nums+=(hi<totalPages-1?'<span style="padding:6px 4px;font-family:var(--mono);font-size:11px;color:var(--muted)">&hellip;</span>':'')+'<button class="nb-pg-num" data-p="'+totalPages+'">'+totalPages+'</button>';
    paginationEl.innerHTML='<button class="nb-pg-btn" id="nb-prev"'+(page===1?' disabled':'')+'>&#8592; Prev</button><div class="nb-pg-nums">'+nums+'</div><button class="nb-pg-btn" id="nb-next"'+(page===totalPages?' disabled':'')+'>Next &#8594;</button>';
    paginationEl.querySelectorAll('.nb-pg-num').forEach(function(btn){
      btn.addEventListener('click',function(){page=parseInt(btn.getAttribute('data-p')||'1',10);render();window.scrollTo({top:grid.offsetTop-80,behavior:'smooth'});});
    });
    var prevBtn=document.getElementById('nb-prev');
    var nextBtn=document.getElementById('nb-next');
    if(prevBtn)prevBtn.addEventListener('click',function(){if(page>1){page--;render();window.scrollTo({top:grid.offsetTop-80,behavior:'smooth'});}});
    if(nextBtn)nextBtn.addEventListener('click',function(){if(page<totalPages){page++;render();window.scrollTo({top:grid.offsetTop-80,behavior:'smooth'});}});
  }

  // Source filter buttons
  document.querySelectorAll('.nb-filter-btn').forEach(function(btn){
    btn.addEventListener('click',function(){
      document.querySelectorAll('.nb-filter-btn').forEach(function(b){b.classList.remove('nb-active');});
      btn.classList.add('nb-active');
      activeSrc=btn.getAttribute('data-src')||'all';
      page=1;render();
    });
  });

  // Sort select
  var sortSel=document.getElementById('nb-sort');
  if(sortSel)sortSel.addEventListener('change',function(){activeSort=sortSel.value;page=1;render();});

  render();
})();
</script>
<div style="text-align:center;font-family:var(--mono);font-size:10.5px;letter-spacing:.06em;color:var(--muted);padding:16px 0 20px;border-top:1px solid var(--border)"><a href="/" style="color:inherit;text-decoration:underline;text-decoration-color:var(--border-2)">&larr; GovRevenue</a> &nbsp;&middot;&nbsp; &copy; 2026 GovRevenue &middot; Public record only.</div>
</body>
</html>`;
}

// ─── /desk/:slug/buyers ───────────────────────────────────────────────────────

function buyersPage(
  profile: DeskProfile,
  cached: { data: ProcurementData; cached_at: string } | null,
  authCtx?: { email: string; tier: UserTier } | null
): string {
  const data = cached?.data;
  const isCompiling = cached === null;

  const allOpen  = dedupeNoticesSoft((data?.contractsFinder.open || []).concat(data?.findTender?.notices || []));
  const allAwarded = data?.contractsFinder.awarded || [];
  const allNotices = [...allAwarded, ...allOpen];

  type BuyerEntry = {
    name: string; initials: string; orgType: string;
    totalSpend: number; openCount: number; awardedCount: number;
    latestDate: number; categories: { label: string; count: number }[];
  };

  const buyerMap = new Map<string, BuyerEntry>();

  for (const n of allNotices) {
    if (!n.buyer || n.buyer === "Not stated") continue;
    let e = buyerMap.get(n.buyer);
    if (!e) {
      e = { name: n.buyer, initials: buyerInitials(n.buyer), orgType: buyerOrgType(n.buyer),
             totalSpend: 0, openCount: 0, awardedCount: 0, latestDate: 0, categories: [] };
      buyerMap.set(n.buyer, e);
    }
    const isAwarded = n.status === "awarded" || allAwarded.includes(n as any);
    if (isAwarded) { e.awardedCount++; e.totalSpend += n.awardedValue ?? 0; }
    else e.openCount++;
    const d = new Date(n.publishedDate || n.awardedDate || "").getTime();
    if (!isNaN(d) && d > e.latestDate) e.latestDate = d;
    // Map to desk categories
    const text = `${n.title} ${n.description || ""}`.toLowerCase();
    for (const cat of profile.categories) {
      if (cat.keywords.some(kw => text.includes(kw))) {
        const existing = e.categories.find(c => c.label === cat.label);
        if (existing) existing.count++;
        else e.categories.push({ label: cat.label, count: 1 });
        break;
      }
    }
  }

  const buyers = [...buyerMap.values()]
    .filter(b => !isAggregatorBuyer(b.name))
    .sort((a, b) => b.totalSpend - a.totalSpend || b.awardedCount - a.awardedCount);

  const totalSpend = buyers.reduce((s, b) => s + b.totalSpend, 0);
  const totalOpen  = buyers.reduce((s, b) => s + b.openCount, 0);

  const buyerCards = buyers.map((b, i) => {
    const tagClass = b.orgType === "HEALTH" ? "bw-tag-health" : b.orgType === "LOCAL AUTHORITY" ? "bw-tag-la" : b.orgType === "CENTRAL GOV" ? "bw-tag-gov" : b.orgType === "HOUSING" ? "bw-tag-housing" : b.orgType === "EDUCATION" ? "bw-tag-edu" : "bw-tag-other";
    const spend = b.totalSpend > 0 ? fmtMoney(b.totalSpend) : "—";
    const lastSeen = b.latestDate ? new Date(b.latestDate).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" }) : "—";
    const topCats = [...b.categories].sort((a, c) => c.count - a.count).slice(0, 3);
    return `<div class="bi-card" data-search="${escapeHtml(b.name.toLowerCase())} ${escapeHtml(b.orgType.toLowerCase())}">
      <div class="bi-card-left">
        <div class="bi-rank">${i + 1}</div>
        <div class="bi-avatar">${escapeHtml(b.initials)}</div>
      </div>
      <div class="bi-card-body">
        <div class="bi-name">${escapeHtml(b.name)}</div>
        <div class="bi-tags">
          ${b.orgType ? `<span class="bw-tag ${tagClass}">${escapeHtml(b.orgType)}</span>` : ""}
          ${topCats.map(c => `<span class="bi-cat-tag">${escapeHtml(c.label)}</span>`).join("")}
        </div>
        <div class="bi-meta-row">
          <span class="bi-spend">${escapeHtml(spend)}</span><span class="bi-spend-label"> awarded spend</span>
          <span class="bi-sep">&middot;</span>
          <span class="bi-notices">${b.awardedCount} awarded</span>
          ${b.openCount > 0 ? `<span class="bi-sep">&middot;</span><span class="bi-open">${b.openCount} open now</span>` : ""}
          <span class="bi-sep">&middot;</span>
          <span class="bi-last">Last seen ${escapeHtml(lastSeen)}</span>
        </div>
      </div>
      ${b.openCount > 0 ? `<div class="bi-card-right"><a href="/desk/${profile.slug}/notices?buyer=${encodeURIComponent(b.name)}" class="bi-cta">View notices &rarr;</a></div>` : ""}
    </div>`;
  }).join("");

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>Buyer Intelligence &mdash; ${escapeHtml(profile.label)} &mdash; GovRevenue</title>
<style>
${pageShellCss()}
.bi-toolbar{display:flex;align-items:center;gap:12px;margin-bottom:28px;flex-wrap:wrap}
.bi-search{flex:1;min-width:200px;max-width:360px;font-family:var(--mono);font-size:12px;padding:9px 14px;border:1px solid var(--border-2);background:var(--surface-2);color:var(--text);outline:none}
.bi-search:focus{border-color:var(--brand)}
.bi-count{font-family:var(--mono);font-size:11px;color:var(--muted);margin-left:auto}
.bi-card{display:flex;align-items:flex-start;gap:16px;padding:22px 0;border-bottom:1px solid var(--border)}
.bi-card:first-child{border-top:1px solid var(--border-2)}
.bi-card:last-child{border-bottom:1px solid var(--border-2)}
.bi-card-left{display:flex;flex-direction:column;align-items:center;gap:6px;flex-shrink:0}
.bi-rank{font-family:var(--mono);font-size:10px;color:var(--muted);letter-spacing:.04em}
.bi-avatar{width:44px;height:44px;border-radius:4px;background:var(--surface-3);color:var(--muted);font-family:var(--mono);font-size:11px;display:flex;align-items:center;justify-content:center;letter-spacing:.04em}
.bi-card-body{flex:1;min-width:0}
.bi-name{font-size:15px;font-weight:600;line-height:1.3;margin-bottom:8px;color:var(--text)}
.bi-tags{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px;align-items:center}
.bi-cat-tag{font-family:var(--mono);font-size:9.5px;letter-spacing:.04em;padding:2px 7px;border-radius:2px;background:rgba(255,255,255,.05);color:var(--muted);border:1px solid var(--border-2)}
.bi-meta-row{font-family:var(--mono);font-size:11.5px;color:var(--muted);display:flex;flex-wrap:wrap;gap:4px;align-items:center}
.bi-spend{font-family:var(--mono);font-size:16px;font-weight:600;color:var(--text)}
.bi-spend-label{font-size:11px}
.bi-sep{color:var(--border-2);margin:0 2px}
.bi-open{color:var(--green);font-weight:600}
.bi-card-right{flex-shrink:0;align-self:center}
.bi-cta{font-family:var(--mono);font-size:11px;letter-spacing:.07em;text-transform:uppercase;color:var(--brand);text-decoration:underline;text-decoration-color:var(--brand-dim);white-space:nowrap}
.bi-cta:hover{text-decoration-color:var(--brand)}
@media(max-width:760px){.bi-card-right{display:none}}
</style>
</head>
<body>
${pageShellHeader(profile, authCtx)}

<section class="pg-mast">
  <div class="pg-mast-inner">
    <div class="pg-crumb">
      <a href="/desk/${profile.slug}">${escapeHtml(profile.label)}</a>
      <span class="pg-crumb-sep">&rsaquo;</span>
      <span class="pg-crumb-active">Buyer Intelligence</span>
    </div>
    <h1>BUYER INTELLIGENCE</h1>
    <div class="pg-stats">
      <div class="pg-stat">
        <span class="pg-stat-val">${isCompiling ? "—" : String(buyers.length)}</span>
        <span class="pg-stat-label">Unique buyers</span>
      </div>
      <div class="pg-stat">
        <span class="pg-stat-val">${isCompiling ? "—" : totalSpend > 0 ? fmtMoney(totalSpend)+"+" : "—"}</span>
        <span class="pg-stat-label">Total awarded spend</span>
      </div>
      <div class="pg-stat">
        <span class="pg-stat-val">${isCompiling ? "—" : String(totalOpen)}</span>
        <span class="pg-stat-label">Open opportunities</span>
      </div>
    </div>
  </div>
</section>

<section class="pg-body">
  <div class="pg-body-inner">
    ${isCompiling
      ? `<p class="pg-empty">Compiling &mdash; data ready within 90 seconds. Refresh to check.</p>`
      : buyers.length === 0
        ? `<p class="pg-empty">No buyer data found. Check back after next compile.</p>`
        : `<div class="bi-toolbar">
            <input class="bi-search" type="search" placeholder="Search buyers or org type…" id="bi-search">
            <span class="bi-count" id="bi-count">${buyers.length} buyers</span>
          </div>
          <div id="bi-list">${buyerCards}</div>
          <script>
          (function(){
            var t;
            document.getElementById('bi-search').addEventListener('input',function(e){
              clearTimeout(t);
              t=setTimeout(function(){
                var q=e.target.value.toLowerCase().trim();
                var cards=document.querySelectorAll('#bi-list .bi-card');
                var vis=0;
                cards.forEach(function(c){
                  var show=!q||c.dataset.search.includes(q);
                  c.style.display=show?'':'none';
                  if(show)vis++;
                });
                document.getElementById('bi-count').textContent=vis+' buyers';
              },120);
            });
          })();
          </script>`
    }
  </div>
</section>

${pageShellFoot()}
</body>
</html>`;
}

function comparePage(current: ScanRecord, prior: ScanRecord | null): string {
  const curEdp = current.report_markdown ? parseEdpFromMarkdown(current.report_markdown) : null;
  const priEdp = prior?.report_markdown ? parseEdpFromMarkdown(prior.report_markdown) : null;
  const curData = current.procurement_json as ProcurementData | null;
  const priData = prior?.procurement_json as ProcurementData | null;
  const curOpen = (curData?.contractsFinder?.open?.length || 0) + (curData?.findTender?.notices?.length || 0);
  const priOpen = (priData?.contractsFinder?.open?.length || 0) + (priData?.findTender?.notices?.length || 0);
  const curAwarded = curData?.contractsFinder?.awarded?.length || 0;
  const priAwarded = priData?.contractsFinder?.awarded?.length || 0;
  const curBuyers = new Set<string>((curData?.contractsFinder?.open || []).map((n: any) => n.buyer).filter(Boolean));
  const priBuyers = prior ? new Set<string>((priData?.contractsFinder?.open || []).map((n: any) => n.buyer).filter(Boolean)) : new Set<string>();
  const newBuyers = [...curBuyers].filter(b => !priBuyers.has(b)).slice(0, 8);
  const droppedBuyers = [...priBuyers].filter(b => !curBuyers.has(b)).slice(0, 8);

  function gradeColour(g: string) {
    const map: Record<string, string> = { A: "#1d6b4f", B: "#1d6b4f", C: "#a97932", D: "#a97932", E: "#9b2d20" };
    return map[g?.charAt(0).toUpperCase()] || "#0B0F14";
  }
  function row(label: string, cur: string, pri: string, changed: boolean) {
    return `<tr>
      <td style="padding:10px 14px;font-size:13px;color:var(--muted);border-bottom:1px solid var(--border);font-family:var(--mono);text-transform:uppercase;letter-spacing:.06em">${escapeHtml(label)}</td>
      <td style="padding:10px 14px;font-size:15px;font-weight:600;border-bottom:1px solid var(--border);color:${escapeHtml(gradeColour(cur))}">${escapeHtml(cur)}</td>
      <td style="padding:10px 14px;font-size:15px;border-bottom:1px solid var(--border);color:var(--muted)">${escapeHtml(pri)}</td>
      <td style="padding:10px 14px;border-bottom:1px solid var(--border)">${changed ? '<span style="background:#fdf0ee;color:var(--red);font-size:11px;font-family:var(--mono);padding:2px 7px">CHANGED</span>' : '<span style="font-size:11px;font-family:var(--mono);color:var(--muted)">same</span>'}</td>
    </tr>`;
  }
  const ce = curEdp, pe = priEdp;
  const tableRows = [
    row("Verdict", ce?.verdict||"—", pe?.verdict||"—", ce?.verdict !== pe?.verdict),
    row("Evidence grade", ce?.evidenceGrade||"—", pe?.evidenceGrade||"—", ce?.evidenceGrade !== pe?.evidenceGrade),
    row("Can win now?", ce?.canTheyWinNow||"—", pe?.canTheyWinNow||"—", ce?.canTheyWinNow !== pe?.canTheyWinNow),
    row("Route", ce?.recommendedRoute||"—", pe?.recommendedRoute||"—", ce?.recommendedRoute !== pe?.recommendedRoute),
    row("Open notices", String(curOpen), String(priOpen), curOpen !== priOpen),
    row("Awarded notices", String(curAwarded), String(priAwarded), curAwarded !== priAwarded),
  ].join("");
  const formatDate = (s: string) => s ? new Date(s).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" }) : "—";
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>Compare &mdash; ${escapeHtml(current.company_name)} &mdash; GovRevenue</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Newsreader:ital,opsz,wght@0,6..72,400;0,6..72,500;1,6..72,400&family=Libre+Franklin:wght@400;500;600;700&family=Spline+Sans+Mono:wght@400;500;600&display=swap');
:root{
  --base:#ECE7DA;--surface:#FBF9F3;--surface-2:#F6F2E8;
  --brand:#B4924E;--text:#1B1E19;--text-mid:#3A3E36;--muted:#86897E;--faint:#9AA093;
  --border:rgba(27,30,25,.12);--border-2:rgba(27,30,25,.18);
  --green:#1d6b4f;--red:#9b2d20;
  --sans:"Libre Franklin",system-ui,sans-serif;
  --serif:"Newsreader",Georgia,serif;
  --mono:"Spline Sans Mono",ui-monospace,monospace;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--base);color:var(--text);font-family:var(--sans);-webkit-font-smoothing:antialiased;padding:44px 24px}
.page{max-width:900px;margin:0 auto}
.top-brand{display:flex;align-items:center;gap:8px;margin-bottom:24px}
.top-brand-dot{width:9px;height:9px;background:var(--brand);border-radius:50%}
.top-brand-name{font-family:var(--serif);font-size:18px;font-weight:500;color:var(--text);text-decoration:none}
.brand-label{font-family:var(--mono);font-size:11px;letter-spacing:0.18em;text-transform:uppercase;color:var(--brand);margin-bottom:16px}
h1{font-family:var(--serif);font-size:30px;font-weight:400;letter-spacing:-0.01em;margin-bottom:6px}
.sub{font-size:12.5px;color:var(--muted);font-family:var(--mono);margin-bottom:28px;letter-spacing:0.04em}
.back{font-size:12.5px;color:var(--muted);text-decoration:none;font-family:var(--mono);display:inline-block;margin-bottom:28px;letter-spacing:0.04em}
.back:hover{color:var(--brand)}
table{width:100%;border-collapse:collapse;background:var(--surface);border:1px solid var(--border-2)}
thead tr{background:var(--surface-2)}
th{padding:10px 14px;text-align:left;font-family:var(--mono);font-size:11px;letter-spacing:0.12em;text-transform:uppercase;color:var(--muted);font-weight:500}
.section-head{font-family:var(--serif);font-size:20px;font-weight:400;margin:36px 0 14px;padding-bottom:10px;border-bottom:1px solid var(--border-2);color:var(--text)}
.buyer-list{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px}
.buyer{font-size:12px;font-family:var(--mono);padding:5px 11px}
.buyer.new{background:rgba(29,107,79,.07);color:var(--green);border:1px solid rgba(29,107,79,.22)}
.buyer.gone{background:rgba(155,45,32,.06);color:var(--red);border:1px solid rgba(155,45,32,.20)}
.none{font-size:13px;color:var(--muted);font-family:var(--mono)}
${prior ? "" : ".no-prior{background:var(--surface);border:1px solid var(--border-2);padding:26px 30px;font-size:14px;color:var(--muted);line-height:1.6}"}
</style>
</head>
<body>
<div class="page">
  <div class="top-brand"><div class="top-brand-dot"></div><a href="/" class="top-brand-name">Gov<b>Revenue</b></a></div>
  <div class="brand-label">Scan comparison</div>
  <h1>${escapeHtml(current.company_name)}</h1>
  <div class="sub">Latest: ${escapeHtml(formatDate(current.created_at))}${prior ? ` &nbsp;&middot;&nbsp; Previous: ${escapeHtml(formatDate(prior.created_at))}` : ""}</div>
  <a class="back" href="/scan/${escapeHtml(current.id)}">&larr; Back to latest scan</a>
  ${prior ? `
  <table>
    <thead><tr><th>Metric</th><th>Latest scan</th><th>Previous scan</th><th>Change</th></tr></thead>
    <tbody>${tableRows}</tbody>
  </table>
  <div class="section-head">New buyers in latest scan</div>
  ${newBuyers.length > 0 ? `<div class="buyer-list">${newBuyers.map(b => `<span class="buyer new">+ ${escapeHtml(b)}</span>`).join("")}</div>` : '<p class="none">None detected</p>'}
  <div class="section-head">Buyers no longer appearing</div>
  ${droppedBuyers.length > 0 ? `<div class="buyer-list">${droppedBuyers.map(b => `<span class="buyer gone">&minus; ${escapeHtml(b)}</span>`).join("")}</div>` : '<p class="none">None detected</p>'}
  ` : `<div class="no-prior">No previous completed scan found for <b>${escapeHtml(current.company_name)}</b>. Run a second scan after a few weeks to generate a comparison.</div>`}
</div>
</body>
</html>`;
}

app.get("/api/scans/:id/report.pdf", asyncRoute(async (req, res) => {
  const scan = await getScan(req.params.id);

  if (!scan || !scan.report_markdown) {
    res.status(404).json({ error: "Report not found or not complete yet." });
    return;
  }

  const pdfEdp = parseEdpFromMarkdown(scan.report_markdown);
  const consistency = validateReportConsistency(pdfEdp, scan.report_markdown);

  if (!consistency.valid) {
    const detail = [...consistency.errors, ...consistency.conflicts].join(" | ");
    console.error("[pdf] blocked — consistency validation failed:", detail, { scanId: scan.id });
    res.status(422).json({
      error: "PDF export blocked: report consistency validation failed.",
      detail,
      errors: consistency.errors,
      conflicts: consistency.conflicts
    });
    return;
  }

  const html = reportPage(scan).replace(
    "</head>",
    `<style>
      .actions,
      .toolbar,
      .admin-actions,
      .download-actions,
      .no-print,
      a[href*="/report.pdf"],
      a[href*="/markdown"],
      a[href*="/relevance.json"],
      button {
        display: none !important;
      }

      body {
        background: var(--cream) !important;
      }

      main {
        margin-top: 0 !important;
      }
    </style></head>`
  );

  let browser: any = null;

  try {
    browser = await puppeteer.launch({
      headless: true,
      args: ["--no-sandbox", "--disable-setuid-sandbox"]
    });

    const page = await browser.newPage();
    await page.setContent(html, { waitUntil: "networkidle0" });
    await page.emulateMediaType("print");

    const pdf = await page.pdf({
      format: "A4",
      printBackground: true,
      displayHeaderFooter: false,
      margin: {
        top: "12mm",
        right: "10mm",
        bottom: "12mm",
        left: "10mm"
      }
    });

    const filename = `${scan.company_name.replace(/[^a-z0-9]+/gi, "_").toLowerCase()}_govrevenue_scan.pdf`;
    const pdfBuffer = Buffer.from(pdf);

    if (isPdfStorageConfigured()) {
      const key = buildPdfStorageKey(scan.id, filename);
      const storedPdf = await storePdfObject({
        key,
        filename,
        body: pdfBuffer
      });

      if (storedPdf) {
        await updateScanPdfStorage(scan.id, {
          pdf_storage_key: storedPdf.key,
          pdf_storage_url: storedPdf.publicUrl,
          pdf_storage_etag: storedPdf.etag
        });
        res.setHeader("X-PDF-Storage-Key", storedPdf.key);
      }
    }

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    res.send(pdfBuffer);
  } catch (error: any) {
    console.error("[pdf] failed", error);
    captureError(error, { pdf: { scanId: req.params.id, storageConfigured: isPdfStorageConfigured() } });
    res.status(500).send(error?.message || "PDF generation failed.");
  } finally {
    if (browser) {
      await browser.close();
    }
  }
}));

app.get("/api/scans/:id/report.md", asyncRoute(async (req, res) => {
  const scan = await getScan(req.params.id);

  if (!scan || !scan.report_markdown) {
    res.status(404).json({ error: "Report not found or not complete yet." });
    return;
  }

  const filename = `${scan.company_name.replace(/[^a-z0-9]+/gi, "_").toLowerCase()}_govrevenue_scan.md`;
  res.setHeader("Content-Type", "text/markdown; charset=utf-8");
  res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
  res.send(scan.report_markdown);
}));

// Admin timestamp cell: absolute date + time on line 1, relative ("3h ago") on line 2.
function adminTime(ts: any): string {
  if (!ts) return `<span style="color:var(--muted)">—</span>`;
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return `<span style="color:var(--muted)">—</span>`;
  const diff = Date.now() - d.getTime();
  const a = Math.abs(diff);
  const m = Math.round(a / 60_000), h = Math.round(a / 3_600_000), dd = Math.round(a / 86_400_000);
  const rel = a < 60_000 ? "just now" : m < 60 ? `${m}m` : h < 24 ? `${h}h` : dd < 30 ? `${dd}d` : `${Math.round(dd / 30)}mo`;
  const ago = a < 60_000 ? rel : diff >= 0 ? `${rel} ago` : `in ${rel}`;
  const date = d.toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "2-digit" });
  const time = d.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" });
  return `<div style="font-family:var(--mono);font-size:11px;color:var(--text);white-space:nowrap;line-height:1.35">${date} <span style="color:var(--muted)">${time}</span></div><div style="font-family:var(--mono);font-size:9px;color:var(--brand);letter-spacing:.04em">${ago}</div>`;
}

app.get("/admin/scans", requireAdmin, asyncRoute(async (req, res) => {
  const token = String(req.query.token || "");
  const reranMsg = req.query.reran ? Number(req.query.reran) : 0;

  const safePool = (sql: string, params?: any[]) =>
    pool ? pool.query(sql, params).catch(() => ({ rows: [] as any[] })) : Promise.resolve({ rows: [] as any[] });

  const [
    scansRes, scanStatsRes,
    usersRes, userStatsRes,
    signalsStatsRes,
    subsRes, briefRes,
    vDaysRes, vPathsRes, vIpsRes, vTodayRes,
  ] = await Promise.all([
    safePool(`SELECT id, created_at, status, company_name, progress_stage, error_message, user_id, pdf_storage_url,
      input_json->>'clientEmail' AS email,
      input_json->>'email'       AS email2,
      input_json->>'website'     AS website,
      input_json->>'location'    AS location,
      input_json->>'areasServed' AS areas_served,
      input_json->>'mainServices' AS main_services,
      input_json->>'secondaryServices' AS secondary_services,
      input_json->>'teamSize'    AS team_size,
      input_json->>'certifications' AS certifications,
      input_json->>'publicSectorExperience' AS ps_experience,
      input_json->>'idealContractSize'     AS ideal_contract,
      input_json->>'maximumContractSize'   AS max_contract,
      input_json->>'frameworkStatus'       AS framework_status,
      input_json->>'mainGoal'              AS main_goal,
      input_json->>'biggestConcern'        AS biggest_concern,
      input_json->>'lastPublicContract'    AS last_public_contract,
      CASE WHEN status='completed' THEN SUBSTRING(report_markdown,1,5000) ELSE NULL END AS report_snippet
    FROM scans ORDER BY created_at DESC LIMIT 200`),
    safePool(`SELECT
      COUNT(*)::int AS total,
      COUNT(*) FILTER (WHERE status='completed')::int AS completed,
      COUNT(*) FILTER (WHERE status='failed')::int AS failed,
      COUNT(*) FILTER (WHERE status IN ('running','pending'))::int AS in_progress,
      COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours')::int AS today,
      COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '7 days')::int  AS this_week,
      ROUND(100.0*COUNT(*) FILTER (WHERE status='completed')/NULLIF(COUNT(*) FILTER (WHERE status IN ('completed','failed')),0),1) AS success_rate
    FROM scans`),
    safePool(`SELECT u.id, u.email, u.created_at, u.tier, u.stripe_customer_id, u.stripe_subscription_status,
      (SELECT COUNT(*)::int FROM scans WHERE user_id=u.id) AS scan_count
    FROM users u ORDER BY u.created_at DESC LIMIT 500`),
    safePool(`SELECT
      COUNT(*)::int AS total,
      COUNT(*) FILTER (WHERE tier='free')::int    AS free_count,
      COUNT(*) FILTER (WHERE tier='pro')::int     AS pro_count,
      COUNT(*) FILTER (WHERE tier='agency')::int  AS agency_count,
      COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '7 days')::int AS new_this_week
    FROM users`),
    safePool(`SELECT
      COUNT(*)::int AS total,
      COUNT(*) FILTER (WHERE LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')::int AS open_count,
      COALESCE(SUM(value_amount) FILTER (WHERE LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%'),0) AS open_value,
      COUNT(DISTINCT category)::int AS categories,
      COUNT(*) FILTER (WHERE notice_date > NOW() - INTERVAL '24 hours')::int AS new_24h,
      COUNT(*) FILTER (WHERE deadline_date BETWEEN NOW() AND NOW()+INTERVAL '7 days')::int AS closing_7d
    FROM homepage_signals`),
    safePool(`SELECT id, scan_id, company_name, email, active, created_at, last_alerted_at,
      cardinality(alerted_notice_ids) AS alerted_count
    FROM subscriptions ORDER BY created_at DESC LIMIT 200`),
    safePool(`SELECT * FROM briefing_subscribers ORDER BY created_at DESC`),
    safePool(`SELECT DATE(visited_at) AS day, COUNT(*)::int AS visits, COUNT(DISTINCT ip)::int AS unique_ips
    FROM visitor_logs WHERE visited_at > NOW() - INTERVAL '14 days' GROUP BY day ORDER BY day DESC`),
    safePool(`SELECT path, COUNT(*)::int AS visits FROM visitor_logs
    WHERE visited_at > NOW() - INTERVAL '7 days' GROUP BY path ORDER BY visits DESC LIMIT 15`),
    safePool(`SELECT ip, COUNT(*)::int AS visits, MAX(visited_at) AS last_seen
    FROM visitor_logs WHERE visited_at > NOW() - INTERVAL '7 days' AND ip IS NOT NULL AND ip != ''
    GROUP BY ip ORDER BY visits DESC LIMIT 30`),
    safePool(`SELECT COUNT(*)::int AS total, COUNT(DISTINCT ip)::int AS unique_ips
    FROM visitor_logs WHERE visited_at > NOW() - INTERVAL '24 hours'`),
  ]);

  const ss   = (scanStatsRes.rows[0]  as any) || {};
  const us   = (userStatsRes.rows[0]  as any) || {};
  const sigs = (signalsStatsRes.rows[0] as any) || {};
  const vToday = (vTodayRes.rows[0]   as any) || { total: 0, unique_ips: 0 };
  const users         = usersRes.rows  as any[];
  const subscriptions = subsRes.rows   as any[];
  const briefing      = briefRes.rows  as any[];
  const vDays  = vDaysRes.rows  as any[];
  const vPaths = vPathsRes.rows as any[];
  const vIps   = vIpsRes.rows   as any[];

  const scans = (scansRes.rows as any[]).map(s => ({
    ...s,
    edp: s.report_snippet ? parseEdpFromMarkdown(String(s.report_snippet)) : null,
    displayEmail: (String(s.email || s.email2 || "")).trim(),
  }));

  const vColor = (v: string) => {
    const l = v.toLowerCase();
    if (l.includes("strong") || l.startsWith("yes")) return "#3ddc84";
    if (l.includes("possible") || l.includes("conditional")) return "#f59e0b";
    return "#e87979";
  };
  const gColor = (g: string) =>
    g === "A" ? "#3ddc84" : g === "B" ? "#60a5fa" : g === "C" ? "#f59e0b" : "#e87979";

  // Pre-compute scan-per-day chart (14 days)
  const days14: string[] = [];
  for (let i = 13; i >= 0; i--) {
    const d = new Date(); d.setDate(d.getDate() - i);
    days14.push(d.toISOString().slice(0, 10));
  }
  const scansByDay = new Map<string, number>();
  scans.forEach((s: any) => {
    const d = String(s.created_at || "").slice(0, 10);
    if (d) scansByDay.set(d, (scansByDay.get(d) || 0) + 1);
  });
  const maxSPerDay = Math.max(1, ...days14.map(d => scansByDay.get(d) || 0));
  const scanActivityHtml = `<div style="display:flex;align-items:flex-end;gap:3px;height:64px;margin-bottom:6px">
    ${days14.map(d => {
      const n = scansByDay.get(d) || 0;
      const h = n === 0 ? 2 : Math.max(4, Math.round((n / maxSPerDay) * 64));
      const isToday = d === new Date().toISOString().slice(0, 10);
      return `<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:2px" title="${d}: ${n} scans">
        <div style="width:100%;height:${h}px;background:${isToday ? "#B4924E" : "rgba(155,44,44,.38)"};border-radius:1px 1px 0 0;min-height:2px"></div>
        <div style="font-family:var(--mono);font-size:8px;color:var(--muted)">${d.slice(8)}</div>
      </div>`;
    }).join("")}
  </div>
  <div style="font-family:var(--mono);font-size:10px;color:var(--muted)">${ss.this_week||0} this week &middot; ${ss.today||0} today &middot; ${ss.in_progress||0} in progress</div>`;

  // Visitor daily chart
  const dayMap = new Map(vDays.map((d: any) => [String(d.day).slice(0, 10), d]));
  const maxVPerDay = Math.max(1, ...days14.map(d => Number((dayMap.get(d) as any)?.visits || 0)));
  const visitorChartHtml = vDays.length === 0
    ? `<div style="padding:20px 0;font-family:var(--mono);font-size:11px;color:var(--muted)">No visitor data yet — tracking starts on first page load after deploy.</div>`
    : `<div style="display:flex;align-items:flex-end;gap:3px;height:64px;margin-bottom:6px">
      ${days14.map(d => {
        const row = dayMap.get(d) as any;
        const visits = Number(row?.visits || 0);
        const uips = Number(row?.unique_ips || 0);
        const h = visits === 0 ? 2 : Math.max(4, Math.round((visits / maxVPerDay) * 64));
        const isToday = d === new Date().toISOString().slice(0, 10);
        return `<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:2px" title="${d}: ${visits} views, ${uips} IPs">
          <div style="width:100%;height:${h}px;background:${isToday ? "#2563ab" : "rgba(37,99,171,.4)"};border-radius:1px 1px 0 0;min-height:2px"></div>
          <div style="font-family:var(--mono);font-size:8px;color:var(--muted)">${d.slice(8)}</div>
        </div>`;
      }).join("")}
    </div>`;

  // Top paths
  const maxPathV = Math.max(1, ...vPaths.map((p: any) => Number(p.visits)));
  const topPathsHtml = vPaths.length === 0
    ? `<div style="padding:16px 0;font-family:var(--mono);font-size:11px;color:var(--muted)">No path data yet</div>`
    : vPaths.map((p: any) => `<div style="margin-bottom:10px">
        <div style="display:flex;justify-content:space-between;margin-bottom:3px">
          <span style="font-family:var(--mono);font-size:11px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:220px">${escapeHtml(p.path)}</span>
          <span style="font-family:var(--mono);font-size:11px;color:var(--muted);margin-left:8px;flex-shrink:0">${Number(p.visits).toLocaleString()}</span>
        </div>
        <div style="height:3px;background:rgba(255,255,255,.08);border-radius:2px">
          <div style="width:${Math.round((Number(p.visits)/maxPathV)*100)}%;height:100%;background:#2563ab;opacity:.7;border-radius:2px"></div>
        </div>
      </div>`).join("");

  // Top IPs
  const topIpsHtml = vIps.length === 0
    ? `<div style="padding:16px 0;font-family:var(--mono);font-size:11px;color:var(--muted)">No IP data yet</div>`
    : `<table class="a-tbl" style="min-width:unset">
        <thead><tr><th>#</th><th>IP Address</th><th>Visits</th><th>Last Seen</th></tr></thead>
        <tbody>${vIps.map((ip: any, i: number) => `<tr>
          <td style="font-family:var(--mono);font-size:11px;color:var(--muted)">${i + 1}</td>
          <td style="font-family:var(--mono);font-size:11px">${escapeHtml(ip.ip || "")}</td>
          <td style="font-family:var(--mono);font-size:12px;text-align:center;font-weight:500">${Number(ip.visits).toLocaleString()}</td>
          <td style="white-space:nowrap">${adminTime(ip.last_seen)}</td>
        </tr>`).join("")}</tbody>
      </table>`;

  // Scans rows
  const scanRowsHtml = scans.length === 0
    ? `<tr><td colspan="17" style="text-align:center;padding:40px;font-family:var(--mono);font-size:12px;color:var(--muted)">No scans yet</td></tr>`
    : scans.map((s: any) => {
        const verdict = s.edp?.verdict || "";
        const grade   = s.edp?.evidenceGrade || "";
        const vC = verdict ? vColor(verdict) : "var(--muted)";
        const gC = grade   ? gColor(grade)   : "var(--muted)";
        const emailHtml = s.displayEmail
          ? `<span style="font-family:var(--mono);font-size:11px">${escapeHtml(s.displayEmail.slice(0, 40))}</span>`
          : `<span style="color:var(--muted)">—</span>`;
        const websiteHtml = s.website
          ? `<a href="${escapeHtml(s.website)}" target="_blank" rel="noopener" style="font-family:var(--mono);font-size:11px;color:#60a5fa">↗</a>`
          : `<span style="color:var(--muted)">—</span>`;
        const errHtml = s.error_message
          ? `<div style="font-family:var(--mono);font-size:9px;color:#e87979;margin-top:2px;max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escapeHtml(s.error_message)}">${escapeHtml(s.error_message.slice(0, 70))}</div>`
          : "";
        const contractRange = [s.ideal_contract, s.max_contract].filter(Boolean).join("–") || "—";
        return `<tr>
          <td style="padding:8px 10px"><input type="checkbox" class="row-chk" value="${escapeHtml(s.id)}"></td>
          <td style="white-space:nowrap">${adminTime(s.created_at)}</td>
          <td style="font-weight:600;max-width:160px"><a href="/scan/${escapeHtml(s.id)}" target="_blank" style="color:var(--text);text-decoration:none" title="${escapeHtml(s.company_name)}">${escapeHtml(s.company_name.slice(0, 28))}</a></td>
          <td>${emailHtml}</td>
          <td style="text-align:center">${websiteHtml}</td>
          <td style="font-family:var(--mono);font-size:11px">${escapeHtml((s.location || "").slice(0, 28)) || `<span style="color:var(--muted)">—</span>`}</td>
          <td style="font-size:11px;max-width:180px;white-space:normal;line-height:1.4">${escapeHtml((s.main_services || "").slice(0, 100)) || `<span style="color:var(--muted)">—</span>`}</td>
          <td style="font-family:var(--mono);font-size:11px;text-align:center">${escapeHtml(s.team_size || "—")}</td>
          <td style="font-family:var(--mono);font-size:11px;max-width:100px">${escapeHtml((s.certifications || "").slice(0, 40)) || `<span style="color:var(--muted)">—</span>`}</td>
          <td style="font-size:11px;max-width:120px;white-space:normal;line-height:1.4">${escapeHtml((s.ps_experience || "").slice(0, 70)) || `<span style="color:var(--muted)">—</span>`}</td>
          <td style="font-family:var(--mono);font-size:11px">${escapeHtml(contractRange.slice(0, 35))}</td>
          <td style="font-family:var(--mono);font-size:11px;max-width:100px">${escapeHtml((s.framework_status || "").slice(0, 30)) || `<span style="color:var(--muted)">—</span>`}</td>
          <td style="font-size:11px;max-width:140px;white-space:normal;line-height:1.4">${escapeHtml((s.main_goal || "").slice(0, 70)) || `<span style="color:var(--muted)">—</span>`}</td>
          <td><span class="pill pill-${escapeHtml(s.status)}">${escapeHtml(s.status)}</span>${errHtml}</td>
          <td style="font-family:var(--mono);font-size:10px;color:${vC};max-width:130px;white-space:normal;line-height:1.35">${escapeHtml(verdict.slice(0, 45)) || `<span style="color:var(--muted)">—</span>`}</td>
          <td style="font-family:var(--serif);font-size:20px;font-weight:700;color:${gC};text-align:center">${escapeHtml(grade) || `<span style="color:var(--muted);font-size:14px;font-family:var(--sans)">—</span>`}</td>
          <td style="white-space:nowrap">
            <div style="display:flex;gap:4px;align-items:center">
              <a href="/scan/${escapeHtml(s.id)}" target="_blank" class="a-btn">Open</a>
              ${s.pdf_storage_url ? `<a href="${escapeHtml(s.pdf_storage_url)}" target="_blank" class="a-btn">PDF</a>` : ""}
              <form method="POST" action="/admin/scans/${escapeHtml(s.id)}/delete?token=${encodeURIComponent(token)}" onsubmit="return confirm('Delete ${escapeHtml(s.company_name.replace(/'/g, "\\'"))}?')" style="display:inline">
                <button type="submit" class="a-btn a-btn-danger">✕</button>
              </form>
            </div>
          </td>
        </tr>`;
      }).join("");

  // Users rows
  const userRowsHtml = users.length === 0
    ? `<tr><td colspan="6" style="text-align:center;padding:40px;font-family:var(--mono);font-size:12px;color:var(--muted)">No users yet</td></tr>`
    : users.map((u: any) => `<tr>
        <td style="font-family:var(--mono);font-size:11px">${escapeHtml(u.email)}</td>
        <td><span class="pill pill-${escapeHtml(u.tier)}">${escapeHtml(u.tier)}</span></td>
        <td style="white-space:nowrap">${adminTime(u.created_at)}</td>
        <td style="font-family:var(--mono);font-size:12px;font-weight:600;text-align:center">${u.scan_count || 0}</td>
        <td>${u.stripe_subscription_status ? `<span class="pill ${u.stripe_subscription_status === "active" ? "pill-active" : "pill-inactive"}">${escapeHtml(u.stripe_subscription_status)}</span>` : `<span style="color:var(--muted)">—</span>`}</td>
        <td style="font-family:var(--mono);font-size:10px;color:var(--muted)">${escapeHtml((u.stripe_customer_id || "").slice(0, 30)) || `<span style="opacity:.4">—</span>`}</td>
      </tr>`).join("");

  // Subscription rows
  const subRowsHtml = subscriptions.length === 0
    ? `<tr><td colspan="7" style="text-align:center;padding:32px;font-family:var(--mono);font-size:12px;color:var(--muted)">No subscriptions yet</td></tr>`
    : subscriptions.map((s: any) => `<tr>
        <td style="font-weight:500">${escapeHtml(s.company_name || "")}</td>
        <td style="font-family:var(--mono);font-size:11px">${escapeHtml(s.email || "")}</td>
        <td><span class="pill ${s.active ? "pill-active" : "pill-inactive"}">${s.active ? "active" : "paused"}</span></td>
        <td style="font-family:var(--mono);font-size:12px;text-align:center;font-weight:600">${s.alerted_count || 0}</td>
        <td style="white-space:nowrap">${s.last_alerted_at ? adminTime(s.last_alerted_at) : `<span style="color:var(--muted)">—</span>`}</td>
        <td style="white-space:nowrap">${adminTime(s.created_at)}</td>
        <td><a href="/admin/subscriptions?token=${encodeURIComponent(token)}" class="a-btn" style="font-size:9px">Manage</a></td>
      </tr>`).join("");

  // Briefing rows
  const briefRowsHtml = briefing.length === 0
    ? `<tr><td colspan="4" style="text-align:center;padding:32px;font-family:var(--mono);font-size:12px;color:var(--muted)">No briefing subscribers yet</td></tr>`
    : briefing.map((b: any) => `<tr>
        <td style="font-family:var(--mono);font-size:11px">${escapeHtml(b.email || "")}</td>
        <td style="font-family:var(--mono);font-size:11px">${escapeHtml(b.source || "—")}</td>
        <td style="font-family:var(--mono);font-size:11px">${escapeHtml(b.category || "All")}</td>
        <td style="white-space:nowrap">${adminTime(b.created_at)}</td>
      </tr>`).join("");

  const totalUserCount = Number(us.total || 0) || 1;
  const totalFreeCount = Number(us.free_count || 0);
  const totalProCount  = Number(us.pro_count || 0);
  const totalAgencyCount = Number(us.agency_count || 0);
  const activeSubCount = subscriptions.filter((s: any) => s.active).length;

  const envRows: [string, boolean][] = [
    ["DATABASE_URL",            !!process.env.DATABASE_URL],
    ["REDIS_URL",               !!process.env.REDIS_URL],
    ["ANTHROPIC_API_KEY",       !!process.env.ANTHROPIC_API_KEY],
    ["OPENAI_API_KEY",          !!process.env.OPENAI_API_KEY],
    ["STRIPE_SECRET_KEY",       !!process.env.STRIPE_SECRET_KEY],
    ["RESEND_API_KEY",          !!process.env.RESEND_API_KEY],
    ["COMPANIES_HOUSE_API_KEY", !!process.env.COMPANIES_HOUSE_API_KEY],
    ["SLACK_WEBHOOK_URL",       !!process.env.SLACK_WEBHOOK_URL],
    ["SENTRY_DSN",              !!process.env.SENTRY_DSN],
    ["PDF_STORAGE_ENDPOINT",    !!process.env.PDF_STORAGE_ENDPOINT],
    ["SAMPLE_PDF_URL",          !!process.env.SAMPLE_PDF_URL],
    ["BASE_URL",                !!process.env.BASE_URL],
  ];

  const configRows: [string, string][] = [
    ["Report provider",  process.env.ANTHROPIC_API_KEY ? "Claude (primary) + OpenAI fallback" : "OpenAI only"],
    ["Claude model",     process.env.ANTHROPIC_MODEL   || "claude-opus-4-8 (default)"],
    ["OpenAI model",     process.env.OPENAI_MODEL      || "gpt-4.1-mini (default)"],
    ["DB backend",       pool ? "PostgreSQL (connected)" : "In-memory (no DATABASE_URL)"],
    ["Queue backend",    process.env.REDIS_URL ? "BullMQ + Redis" : "In-process queue"],
    ["Node version",     process.version],
    ["Server time",      new Date().toISOString().slice(0, 19).replace("T", " ") + " UTC"],
    ["Uptime",           `${Math.floor(process.uptime() / 60)}m ${Math.floor(process.uptime() % 60)}s`],
  ];

  res.type("html").send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>GovRevenue — Admin</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=Inter:wght@400;450;500;600;700;800;900&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --accent:#A0522D;--accent-hot:#B8673A;--slate:#566273;--green:#22C55E;--gold:#F59E0B;--blue:#3B82F6;
  --serif:"Inter",-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif;
  --sans:"Inter",-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
  --mono:"IBM Plex Mono","SF Mono",ui-monospace,Menlo,monospace;
  --bg:#06090F;--surface:#0B1018;--surface-2:#111A26;
  --border:rgba(255,255,255,.06);--border-2:rgba(255,255,255,.1);
  --muted:#8893A4;--text:#E9EEF5;
}
html,body{height:100%;background:var(--bg);color:var(--text);font-family:var(--sans);font-size:13px;line-height:1.5;-webkit-font-smoothing:antialiased}
a{color:var(--accent);text-decoration:none}
a:hover{text-decoration:underline}
.shell{display:flex;min-height:100vh}
.sidebar{width:210px;flex-shrink:0;background:#060A0D;border-right:1px solid var(--border);display:flex;flex-direction:column;position:sticky;top:0;height:100vh;overflow-y:auto;z-index:200}
.main{flex:1;min-width:0;display:flex;flex-direction:column}
.sb-brand{padding:20px 16px 14px;border-bottom:1px solid var(--border)}
.sb-logo{font-family:var(--sans);font-size:19px;font-weight:800;letter-spacing:-.03em;color:var(--text)}
.sb-logo b{color:var(--accent)}
.sb-tag{font-family:var(--mono);font-size:8px;letter-spacing:.22em;text-transform:uppercase;color:var(--muted);margin-top:3px}
.sb-nav{padding:10px 0;flex:1}
.sb-link{display:flex;align-items:center;gap:8px;padding:8px 16px;font-family:var(--mono);font-size:10px;letter-spacing:.07em;text-transform:uppercase;color:var(--muted);text-decoration:none!important;transition:all .12s;border-left:2px solid transparent}
.sb-link:hover{color:var(--text);background:rgba(255,255,255,.04);text-decoration:none}
.sb-link.active{color:var(--text);border-left-color:var(--accent);background:rgba(160,82,45,.14)}
.sb-count{margin-left:auto;background:rgba(255,255,255,.08);font-size:9px;padding:1px 5px;border-radius:8px}
.sb-div{height:1px;background:var(--border);margin:5px 16px}
.sb-foot{padding:10px 12px;border-top:1px solid var(--border);margin-top:auto}
.sb-token{font-family:var(--mono);font-size:8px;color:var(--muted);margin-bottom:7px;letter-spacing:.04em}
.sb-action{display:block;width:100%;padding:6px 10px;background:rgba(255,255,255,.05);border:1px solid var(--border);color:var(--muted);font-family:var(--mono);font-size:8.5px;letter-spacing:.07em;text-transform:uppercase;cursor:pointer;text-align:center;margin-bottom:4px;transition:.12s;text-decoration:none}
.sb-action:hover{background:rgba(255,255,255,.1);color:var(--text);text-decoration:none}
.topbar{display:flex;align-items:center;justify-content:space-between;padding:12px 24px;border-bottom:1px solid var(--border);background:#060A0D;position:sticky;top:0;z-index:150}
.topbar-title{font-family:var(--serif);font-size:15px;font-weight:600}
.topbar-meta{display:flex;align-items:center;gap:16px;font-family:var(--mono);font-size:9.5px;color:var(--muted)}
.live-dot{width:5px;height:5px;border-radius:50%;background:var(--green);display:inline-block;margin-right:4px;animation:lp 2s infinite}
@keyframes lp{0%,100%{opacity:1}50%{opacity:.25}}
.section{padding:24px 24px 0}
.s-head{display:flex;align-items:flex-end;justify-content:space-between;margin-bottom:16px;padding-bottom:10px;border-bottom:1px solid var(--border)}
.s-eyebrow{font-family:var(--mono);font-size:8.5px;letter-spacing:.2em;text-transform:uppercase;color:var(--accent);margin-bottom:3px}
.s-title{font-family:var(--sans);font-size:21px;font-weight:800;letter-spacing:-.03em;line-height:1.05;color:#fff}
.s-sub{font-family:var(--mono);font-size:9.5px;color:var(--muted)}
.kpi-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:1px;background:var(--border);border:1px solid var(--border);margin-bottom:16px}
.kpi{background:var(--surface);padding:16px 14px 12px}
.kpi-lbl{font-family:var(--mono);font-size:8px;letter-spacing:.16em;text-transform:uppercase;color:var(--muted);margin-bottom:7px}
.kpi-val{font-family:var(--sans);font-size:30px;font-weight:800;letter-spacing:-.03em;line-height:1;color:#fff}
.kpi-sub{font-family:var(--mono);font-size:9px;color:var(--muted);margin-top:4px}
.kpi-green{color:#3ddc84}.kpi-red{color:#e87979}.kpi-gold{color:#f59e0b}.kpi-blue{color:#60a5fa}
.stat-row{display:grid;gap:1px;background:var(--border);border:1px solid var(--border);margin-bottom:12px}
.stat-cell{background:var(--surface-2);padding:12px 14px}
.stat-val{font-family:var(--serif);font-size:19px;color:var(--text);margin-bottom:2px}
.stat-lbl{font-family:var(--mono);font-size:8px;letter-spacing:.1em;text-transform:uppercase;color:var(--muted)}
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px}
.card{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);padding:16px;border-radius:10px;backdrop-filter:blur(12px) saturate(150%);-webkit-backdrop-filter:blur(12px) saturate(150%);position:relative;overflow:hidden;transition:transform .22s ease,border-color .22s,box-shadow .22s}
.card::before{content:'';position:absolute;inset:0;background:linear-gradient(135deg,rgba(255,255,255,.04) 0%,transparent 55%);pointer-events:none}
.card:hover{transform:perspective(700px) translateY(-4px) rotateX(2deg);border-color:rgba(255,255,255,.15);box-shadow:0 20px 40px -16px rgba(0,0,0,.6)}
.card-head{font-family:var(--mono);font-size:9px;letter-spacing:.14em;text-transform:uppercase;color:var(--muted);margin-bottom:10px;padding-bottom:8px;border-bottom:1px solid rgba(255,255,255,.07);display:flex;align-items:center;justify-content:space-between}
.card-head-val{color:var(--text);font-size:10.5px;text-transform:none;letter-spacing:0}
.tier-row{display:flex;align-items:center;gap:8px;margin-bottom:8px}
.tier-lbl{font-family:var(--mono);font-size:9.5px;width:52px;flex-shrink:0}
.tier-track{flex:1;height:5px;background:rgba(255,255,255,.07);border-radius:3px}
.tier-fill{height:100%;border-radius:3px}
.tier-n{font-family:var(--mono);font-size:9.5px;color:var(--text);width:20px;text-align:right;flex-shrink:0}
.tbl-wrap{overflow-x:auto;border:1px solid var(--border)}
.scroll-tbl{max-height:480px;overflow-y:auto}
table.a-tbl{border-collapse:collapse;width:100%;font-size:12px}
table.a-tbl th{font-family:var(--mono);font-size:8.5px;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);padding:8px 11px;border-bottom:1px solid var(--border);white-space:nowrap;text-align:left;background:var(--surface);position:sticky;top:0;z-index:10}
table.a-tbl td{padding:7px 11px;border-bottom:1px solid var(--border);color:var(--text);vertical-align:middle}
table.a-tbl tr:hover td{background:rgba(255,255,255,.02)}
.pill{display:inline-block;font-family:var(--mono);font-size:8px;letter-spacing:.06em;text-transform:uppercase;padding:2px 6px;border-radius:2px;font-weight:500;border:1px solid}
.pill-completed{background:#0a2018;color:#3ddc84;border-color:#1d6b4f44}
.pill-failed{background:#200a0a;color:#e87979;border-color:#9b2d2044}
.pill-running,.pill-pending{background:#20160a;color:#f59e0b;border-color:#b4530944}
.pill-free{background:#141e28;color:var(--muted);border-color:rgba(255,255,255,.1)}
.pill-pro{background:#200a0a;color:#e87979;border-color:#9b2d2044}
.pill-agency{background:#20160a;color:#f59e0b;border-color:#a9793244}
.pill-active{background:#0a2018;color:#3ddc84;border-color:#1d6b4f44}
.pill-inactive{background:#141e28;color:var(--muted);border-color:rgba(255,255,255,.08)}
.a-btn{display:inline-block;font-family:var(--mono);font-size:8.5px;letter-spacing:.07em;text-transform:uppercase;padding:4px 9px;border:1px solid var(--border);color:var(--muted);cursor:pointer;transition:.12s;background:none;white-space:nowrap}
.a-btn:hover{background:rgba(255,255,255,.07);color:var(--text);text-decoration:none}
.a-btn-danger{border-color:rgba(155,45,45,.4);color:#e87979}
.a-btn-danger:hover{background:#200a0a}
.a-btn-ok{border-color:rgba(29,107,79,.4);color:#3ddc84}
.a-btn-ok:hover{background:#0a2018}
#sel-bar{display:none;align-items:center;gap:10px;padding:8px 24px;background:rgba(155,44,44,.12);border-bottom:1px solid rgba(155,44,44,.3);position:sticky;top:46px;z-index:140}
#sel-bar.visible{display:flex}
.a-alert-ok{padding:9px 16px;background:#0a2018;border:1px solid #1d6b4f44;color:#3ddc84;font-family:var(--mono);font-size:10.5px;margin-bottom:12px;border-radius:2px}
.env-row{display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid var(--border)}
.env-key{font-family:var(--mono);font-size:10px;color:var(--muted);width:210px;flex-shrink:0}
.env-set{color:#3ddc84;font-family:var(--mono);font-size:10px}
.env-unset{color:var(--muted);opacity:.4;font-family:var(--mono);font-size:10px}
.env-val{font-family:var(--mono);font-size:10px;color:var(--text)}
.gap{height:24px}
input[type=checkbox]{accent-color:var(--accent);width:12px;height:12px;cursor:pointer}
</style>
</head>
<body>
<div class="shell">
<aside class="sidebar">
  <div class="sb-brand">
    <div class="sb-logo">Gov<b>Revenue</b></div>
    <div class="sb-tag">Command Centre</div>
  </div>
  <nav class="sb-nav">
    <a href="#overview" class="sb-link active">Overview</a>
    <a href="#scans" class="sb-link">Scans <span class="sb-count">${ss.total || 0}</span></a>
    <a href="#users" class="sb-link">Users <span class="sb-count">${us.total || 0}</span></a>
    <div class="sb-div"></div>
    <a href="#visitors" class="sb-link">Visitors</a>
    <a href="#signals" class="sb-link">Signals <span class="sb-count">${sigs.total || 0}</span></a>
    <div class="sb-div"></div>
    <a href="#alerts" class="sb-link">Alerts <span class="sb-count">${activeSubCount}</span></a>
    <a href="#briefing" class="sb-link">Briefing <span class="sb-count">${briefing.length}</span></a>
    <div class="sb-div"></div>
    <a href="#system" class="sb-link">System</a>
  </nav>
  <div class="sb-foot">
    <div class="sb-token">Token: ****${escapeHtml(token.slice(-6))}</div>
    <form method="POST" action="/admin/signals/rebuild?token=${encodeURIComponent(token)}" onsubmit="return confirm('Rebuild all signals?')">
      <button class="sb-action" type="submit">↻ Rebuild signals</button>
    </form>
    <form method="POST" action="/admin/desks/rebuild?token=${encodeURIComponent(token)}" onsubmit="return confirm('Rebuild all desk caches?')">
      <button class="sb-action" type="submit">↻ Rebuild desks</button>
    </form>
    <a href="/admin/subscriptions?token=${encodeURIComponent(token)}" class="sb-action">→ Subscriptions</a>
  </div>
</aside>
<div class="main">
<div class="topbar">
  <div class="topbar-title">Admin Command Centre</div>
  <div class="topbar-meta">
    <span><span class="live-dot"></span>Live</span>
    <span id="aclock">${new Date().toISOString().slice(0, 19).replace("T", " ")} UTC</span>
    <a href="/admin/scans?token=${encodeURIComponent(token)}" style="color:var(--muted)">↻ Refresh</a>
    <a href="/" target="_blank" style="color:var(--muted)">↗ Site</a>
  </div>
</div>
${reranMsg ? `<div class="a-alert-ok" style="margin:10px 24px 0">${reranMsg} scan(s) re-queued.</div>` : ""}
<div id="sel-bar">
  <span id="sel-count" style="font-family:var(--mono);font-size:11px">0 selected</span>
  <button class="a-btn a-btn-ok" onclick="bulkAction('rerun')">↻ Re-run</button>
  <button class="a-btn a-btn-danger" onclick="bulkAction('delete')">✕ Delete</button>
</div>
<form id="bulk-form" method="POST" style="display:none">
  <input type="hidden" name="token" value="${escapeHtml(token)}">
  <div id="bulk-ids"></div>
</form>

<!-- §1 OVERVIEW -->
<section class="section" id="overview">
  <div class="s-head">
    <div><div class="s-eyebrow">Dashboard</div><div class="s-title">Overview</div></div>
    <div class="s-sub">${new Date().toLocaleDateString("en-GB",{weekday:"long",day:"numeric",month:"long",year:"numeric"})}</div>
  </div>
  <div class="kpi-grid">
    <div class="kpi"><div class="kpi-lbl">Total Scans</div><div class="kpi-val">${ss.total || 0}</div><div class="kpi-sub"><span class="kpi-green">+${ss.today || 0}</span> today &middot; ${ss.this_week || 0} this week</div></div>
    <div class="kpi"><div class="kpi-lbl">Success Rate</div><div class="kpi-val ${Number(ss.success_rate || 0) >= 80 ? "kpi-green" : "kpi-red"}">${ss.success_rate || 0}%</div><div class="kpi-sub">${ss.completed || 0} ok &middot; ${ss.failed || 0} failed</div></div>
    <div class="kpi"><div class="kpi-lbl">Registered Users</div><div class="kpi-val">${us.total || 0}</div><div class="kpi-sub"><span class="kpi-green">+${us.new_this_week || 0}</span> this week</div></div>
    <div class="kpi"><div class="kpi-lbl">Open Signals</div><div class="kpi-val kpi-red">${Number(sigs.open_count || 0).toLocaleString("en-GB")}</div><div class="kpi-sub">${sigs.new_24h || 0} new &middot; ${sigs.closing_7d || 0} closing</div></div>
    <div class="kpi"><div class="kpi-lbl">Open Value</div><div class="kpi-val" style="font-size:20px">${fmtMoney(Number(sigs.open_value || 0))}</div><div class="kpi-sub">${sigs.categories || 0} active desks</div></div>
    <div class="kpi"><div class="kpi-lbl">Visitors Today</div><div class="kpi-val kpi-blue">${Number(vToday.total || 0).toLocaleString()}</div><div class="kpi-sub">${Number(vToday.unique_ips || 0)} unique IPs</div></div>
  </div>
  <div class="two-col">
    <div class="card">
      <div class="card-head">User Breakdown <span class="card-head-val">${us.total || 0} total</span></div>
      <div class="tier-row"><span class="tier-lbl" style="color:var(--muted)">Free</span><div class="tier-track"><div class="tier-fill" style="width:${Math.round(100*totalFreeCount/totalUserCount)}%;background:var(--slate)"></div></div><span class="tier-n">${us.free_count || 0}</span></div>
      <div class="tier-row"><span class="tier-lbl" style="color:#e87979">Pro</span><div class="tier-track"><div class="tier-fill" style="width:${Math.round(100*totalProCount/totalUserCount)}%;background:var(--accent)"></div></div><span class="tier-n">${us.pro_count || 0}</span></div>
      <div class="tier-row"><span class="tier-lbl" style="color:#f59e0b">Agency</span><div class="tier-track"><div class="tier-fill" style="width:${Math.round(100*totalAgencyCount/totalUserCount)}%;background:var(--gold)"></div></div><span class="tier-n">${us.agency_count || 0}</span></div>
      <div style="margin-top:12px;padding-top:10px;border-top:1px solid var(--border)">
        <div class="tier-row"><span class="tier-lbl" style="color:#3ddc84;font-size:9px">Alerts</span><div class="tier-track"><div class="tier-fill" style="width:${Math.min(100,Math.round(100*activeSubCount/totalUserCount))}%;background:var(--green)"></div></div><span class="tier-n">${activeSubCount}</span></div>
        <div class="tier-row" style="margin-bottom:0"><span class="tier-lbl" style="color:#60a5fa;font-size:9px">Briefing</span><div class="tier-track"><div class="tier-fill" style="width:${Math.min(100,Math.round(100*briefing.length/totalUserCount))}%;background:var(--blue)"></div></div><span class="tier-n">${briefing.length}</span></div>
      </div>
    </div>
    <div class="card">
      <div class="card-head">Scan Activity <span class="card-head-val">Last 14 days</span></div>
      ${scanActivityHtml}
    </div>
  </div>
</section>
<div class="gap"></div>

<!-- §2 SCANS -->
<section class="section" id="scans">
  <div class="s-head">
    <div><div class="s-eyebrow">Database</div><div class="s-title">Scan Intelligence</div></div>
    <div class="s-sub">${scans.length} records &middot; most recent first &middot; all form fields</div>
  </div>
  <div class="stat-row" style="grid-template-columns:repeat(6,1fr);margin-bottom:12px">
    <div class="stat-cell"><div class="stat-val" style="color:#3ddc84">${ss.completed || 0}</div><div class="stat-lbl">Completed</div></div>
    <div class="stat-cell"><div class="stat-val" style="color:#e87979">${ss.failed || 0}</div><div class="stat-lbl">Failed</div></div>
    <div class="stat-cell"><div class="stat-val" style="color:#f59e0b">${ss.in_progress || 0}</div><div class="stat-lbl">In Progress</div></div>
    <div class="stat-cell"><div class="stat-val">${ss.today || 0}</div><div class="stat-lbl">Today</div></div>
    <div class="stat-cell"><div class="stat-val">${ss.this_week || 0}</div><div class="stat-lbl">This Week</div></div>
    <div class="stat-cell"><div class="stat-val">${ss.success_rate || 0}%</div><div class="stat-lbl">Success Rate</div></div>
  </div>
  <div class="tbl-wrap scroll-tbl">
    <table class="a-tbl">
      <thead><tr>
        <th><input type="checkbox" id="chk-all"></th>
        <th>Date</th><th>Company</th><th>Email</th><th>Web</th><th>Location</th>
        <th>Main Services</th><th>Team</th><th>Certs</th><th>PS Exp</th>
        <th>Contract</th><th>Framework</th><th>Goal</th>
        <th>Status</th><th>Verdict</th><th>Grade</th><th>Actions</th>
      </tr></thead>
      <tbody>${scanRowsHtml}</tbody>
    </table>
  </div>
</section>
<div class="gap"></div>

<!-- §3 USERS -->
<section class="section" id="users">
  <div class="s-head">
    <div><div class="s-eyebrow">Database</div><div class="s-title">Registered Users</div></div>
    <div class="s-sub">${users.length} accounts</div>
  </div>
  <div class="tbl-wrap scroll-tbl">
    <table class="a-tbl">
      <thead><tr><th>Email</th><th>Tier</th><th>Joined</th><th>Scans</th><th>Stripe Status</th><th>Stripe Customer</th></tr></thead>
      <tbody>${userRowsHtml}</tbody>
    </table>
  </div>
</section>
<div class="gap"></div>

<!-- §4 VISITORS -->
<section class="section" id="visitors">
  <div class="s-head">
    <div><div class="s-eyebrow">Analytics</div><div class="s-title">Visitor Intelligence</div></div>
    <div class="s-sub">HTML page views only &middot; x-forwarded-for IPs &middot; API/admin excluded</div>
  </div>
  <div class="stat-row" style="grid-template-columns:repeat(5,1fr);margin-bottom:12px">
    <div class="stat-cell"><div class="stat-val" style="color:#60a5fa">${Number(vToday.total || 0).toLocaleString()}</div><div class="stat-lbl">Views today</div></div>
    <div class="stat-cell"><div class="stat-val">${Number(vToday.unique_ips || 0)}</div><div class="stat-lbl">Unique IPs today</div></div>
    <div class="stat-cell"><div class="stat-val">${vDays.reduce((s: number, d: any) => s + Number(d.visits || 0), 0).toLocaleString()}</div><div class="stat-lbl">Views 14 days</div></div>
    <div class="stat-cell"><div class="stat-val">${vIps.length}</div><div class="stat-lbl">Distinct IPs 7d</div></div>
    <div class="stat-cell"><div class="stat-val">${vPaths.length}</div><div class="stat-lbl">Distinct paths 7d</div></div>
  </div>
  <div class="card" style="margin-bottom:12px">
    <div class="card-head">Daily Traffic <span class="card-head-val">last 14 days</span></div>
    ${visitorChartHtml}
  </div>
  <div class="two-col">
    <div class="card"><div class="card-head">Top Pages <span class="card-head-val">7 days</span></div>${topPathsHtml}</div>
    <div class="card"><div class="card-head">Top IPs <span class="card-head-val">7 days</span></div><div class="tbl-wrap scroll-tbl" style="max-height:260px;border:none">${topIpsHtml}</div></div>
  </div>
</section>
<div class="gap"></div>

<!-- §5 SIGNALS -->
<section class="section" id="signals">
  <div class="s-head">
    <div><div class="s-eyebrow">Intelligence</div><div class="s-title">Live Signal Database</div></div>
    <div class="s-sub">${Number(sigs.total || 0).toLocaleString()} notices indexed</div>
  </div>
  <div class="stat-row" style="grid-template-columns:repeat(6,1fr)">
    <div class="stat-cell"><div class="stat-val" style="color:#3ddc84">${Number(sigs.open_count || 0).toLocaleString()}</div><div class="stat-lbl">Open now</div></div>
    <div class="stat-cell"><div class="stat-val" style="font-size:16px">${fmtMoney(Number(sigs.open_value || 0))}</div><div class="stat-lbl">Open value</div></div>
    <div class="stat-cell"><div class="stat-val" style="color:#e87979">${sigs.new_24h || 0}</div><div class="stat-lbl">New 24h</div></div>
    <div class="stat-cell"><div class="stat-val" style="color:#f59e0b">${sigs.closing_7d || 0}</div><div class="stat-lbl">Closing &lt;7d</div></div>
    <div class="stat-cell"><div class="stat-val">${sigs.categories || 0}</div><div class="stat-lbl">Active desks</div></div>
    <div class="stat-cell"><div class="stat-val">${Number(sigs.total || 0).toLocaleString()}</div><div class="stat-lbl">Total indexed</div></div>
  </div>
</section>
<div class="gap"></div>

<!-- §6 ALERTS -->
<section class="section" id="alerts">
  <div class="s-head">
    <div><div class="s-eyebrow">Notifications</div><div class="s-title">Weekly Alerts</div></div>
    <div class="s-sub">${subscriptions.length} total &middot; ${activeSubCount} active</div>
  </div>
  <div class="tbl-wrap scroll-tbl" style="margin-bottom:0">
    <table class="a-tbl">
      <thead><tr><th>Company</th><th>Email</th><th>Status</th><th>Sent</th><th>Last Alert</th><th>Subscribed</th><th></th></tr></thead>
      <tbody>${subRowsHtml}</tbody>
    </table>
  </div>
</section>
<div class="gap"></div>

<!-- §7 BRIEFING -->
<section class="section" id="briefing">
  <div class="s-head">
    <div><div class="s-eyebrow">Notifications</div><div class="s-title">Briefing Subscribers</div></div>
    <div class="s-sub">${briefing.length} subscribers</div>
  </div>
  <div class="tbl-wrap">
    <table class="a-tbl">
      <thead><tr><th>Email</th><th>Source</th><th>Sector</th><th>Joined</th></tr></thead>
      <tbody>${briefRowsHtml}</tbody>
    </table>
  </div>
</section>
<div class="gap"></div>

<!-- §8 SYSTEM -->
<section class="section" id="system">
  <div class="s-head">
    <div><div class="s-eyebrow">Infrastructure</div><div class="s-title">System Status</div></div>
    <div class="s-sub">Uptime ${Math.floor(process.uptime() / 60)}m ${Math.floor(process.uptime() % 60)}s &middot; Node ${process.version}</div>
  </div>
  <div class="two-col" style="margin-bottom:12px">
    <div class="card">
      <div class="card-head">Environment Variables</div>
      ${envRows.map(([k, set]) => `<div class="env-row"><span class="env-key">${k}</span><span class="${set ? "env-set" : "env-unset"}">${set ? "✓  Set" : "✗  Not set"}</span></div>`).join("")}
    </div>
    <div class="card">
      <div class="card-head">Runtime Config</div>
      ${configRows.map(([k, v]) => `<div class="env-row"><span class="env-key">${escapeHtml(k)}</span><span class="env-val">${escapeHtml(v)}</span></div>`).join("")}
      <div style="margin-top:14px;padding-top:10px;border-top:1px solid var(--border)">
        <div style="font-family:var(--mono);font-size:8.5px;letter-spacing:.12em;text-transform:uppercase;color:var(--muted);margin-bottom:8px">Quick Actions</div>
        <div style="display:flex;gap:7px;flex-wrap:wrap">
          <form method="POST" action="/admin/signals/rebuild?token=${encodeURIComponent(token)}" onsubmit="return confirm('Rebuild all signals?')"><button class="a-btn a-btn-ok" type="submit">↻ Rebuild signals</button></form>
          <form method="POST" action="/admin/desks/rebuild?token=${encodeURIComponent(token)}" onsubmit="return confirm('Rebuild all 24 desk caches?')"><button class="a-btn a-btn-ok" type="submit">↻ Rebuild desks</button></form>
          <button class="a-btn a-btn-danger" onclick="bulkDeleteFailed()">✕ Purge failed scans</button>
        </div>
      </div>
    </div>
  </div>
</section>
<div style="height:40px"></div>
</div><!-- .main -->
</div><!-- .shell -->

<script>
setInterval(function(){var el=document.getElementById('aclock');if(el)el.textContent=new Date().toISOString().slice(0,19).replace('T',' ')+' UTC';},1000);
var sections=document.querySelectorAll('section[id]');
var navLinks=document.querySelectorAll('.sb-link[href^="#"]');
function updateNav(){var cur='';sections.forEach(function(s){if(window.scrollY>=s.offsetTop-70)cur=s.id;});navLinks.forEach(function(l){l.classList.toggle('active',l.getAttribute('href')==='#'+cur);});}
window.addEventListener('scroll',updateNav);updateNav();
var chkAll=document.getElementById('chk-all');
var selBar=document.getElementById('sel-bar');
var selCount=document.getElementById('sel-count');
function getChecked(){return[...document.querySelectorAll('.row-chk:checked')].map(function(c){return c.value;});}
function updateBar(){var n=getChecked().length;selCount.textContent=n+' scan'+(n!==1?'s':'')+' selected';selBar.classList.toggle('visible',n>0);}
if(chkAll)chkAll.addEventListener('change',function(){document.querySelectorAll('.row-chk').forEach(function(c){c.checked=chkAll.checked;});updateBar();});
document.querySelectorAll('.row-chk').forEach(function(c){c.addEventListener('change',function(){if(chkAll)chkAll.checked=document.querySelectorAll('.row-chk:not(:checked)').length===0;updateBar();});});
function bulkAction(action){
  var ids=getChecked();if(!ids.length)return;
  if(action==='delete'&&!confirm('Permanently delete '+ids.length+' scan(s)?'))return;
  if(action==='rerun'&&!confirm('Re-run '+ids.length+' scan(s)?'))return;
  var form=document.getElementById('bulk-form');
  form.action='/admin/scans/bulk-'+action+'?token=${escapeHtml(token)}';
  document.getElementById('bulk-ids').innerHTML=ids.map(function(id){return'<input type="hidden" name="ids[]" value="'+id+'">';}).join('');
  form.submit();
}
function bulkDeleteFailed(){
  var rows=[...document.querySelectorAll('.row-chk')].filter(function(c){var r=c.closest('tr');return r&&r.querySelector('.pill-failed');});
  if(!rows.length){alert('No failed scans.');return;}
  if(!confirm('Delete '+rows.length+' failed scan(s)?'))return;
  var form=document.getElementById('bulk-form');
  form.action='/admin/scans/bulk-delete?token=${escapeHtml(token)}';
  document.getElementById('bulk-ids').innerHTML=rows.map(function(c){return'<input type="hidden" name="ids[]" value="'+c.value+'">';}).join('');
  form.submit();
}
</script>
</body>
</html>`);
}));

app.post("/api/scans/:id/subscribe", asyncRoute(async (req, res) => {
  const scan = await getScan(req.params.id);
  if (!scan) {
    res.status(404).json({ error: "Scan not found" });
    return;
  }
  if (scan.status !== "completed") {
    res.status(400).json({ error: "Scan must be completed before subscribing" });
    return;
  }

  const emailParsed = z.string().email().safeParse(req.body?.email);
  if (!emailParsed.success) {
    res.status(400).json({ error: "Valid email required" });
    return;
  }

  const sub = await createSubscription(scan.id, emailParsed.data, scan.input_json, scan.company_name);
  await enqueueWeeklyAlert(sub.id);

  res.status(201).json({ id: sub.id, message: "Subscribed to weekly alerts." });
}));

app.get("/unsubscribe/:id", asyncRoute(async (req, res) => {
  const sub = await getSubscription(req.params.id);
  if (!sub) {
    res.status(404).type("html").send(`<body style="font-family:'Inter','Helvetica Neue',Arial,sans-serif;padding:40px"><p>Subscription not found.</p></body>`);
    return;
  }
  await deactivateSubscription(sub.id);
  res.type("html").send(`<!doctype html>
<html lang="en">
<body style="font-family:'Inter','Helvetica Neue',Arial,sans-serif;background:#F3EFE6;color:#0B0F14;padding:40px">
<div style="max-width:600px;margin:auto;background:#FAF8F3;border:1px solid #0F141926;padding:32px">
  <h1 style="font-family:'Spectral','Iowan Old Style',Georgia,serif;margin-top:0">Unsubscribed</h1>
  <p>Weekly alerts for <strong>${escapeHtml(sub.company_name)}</strong> have been cancelled.</p>
  <p><a href="/" style="color:#1d6b4f">Back to GovRevenue</a></p>
</div>
</body></html>`);
}));

app.get("/unsubscribe-briefing/:id", asyncRoute(async (req, res) => {
  const id = String(req.params.id || "").trim();
  if (!id) { res.status(400).type("html").send(`<body style="font-family:'Inter','Helvetica Neue',Arial,sans-serif;padding:40px"><p>Invalid link.</p></body>`); return; }
  if (pool) {
    await pool.query(`DELETE FROM briefing_subscribers WHERE id = $1`, [id]);
  } else {
    for (const [k, v] of briefMemStore.entries()) { if (v.id === id) { briefMemStore.delete(k); break; } }
  }
  res.type("html").send(`<!doctype html>
<html lang="en">
<body style="font-family:'Inter','Helvetica Neue',Arial,sans-serif;background:#F3EFE6;color:#0B0F14;padding:40px">
<div style="max-width:600px;margin:auto;background:#FAF8F3;border:1px solid #0F141926;padding:32px">
  <h1 style="font-family:'Spectral','Iowan Old Style',Georgia,serif;margin-top:0">Unsubscribed</h1>
  <p>You have been removed from the weekly procurement briefing.</p>
  <p><a href="/" style="color:#1d6b4f">Back to GovRevenue</a></p>
</div>
</body></html>`);
}));

app.get("/admin/subscriptions", requireAdmin, asyncRoute(async (req, res) => {
  const subs = await listAllSubscriptions();
  const token = String(req.query.token || "");
  res.type("html").send(`<!doctype html>
<html lang="en">
<body style="font-family:'Inter','Helvetica Neue',Arial,sans-serif;background:#F3EFE6;color:#0B0F14;padding:32px">
<h1 style="font-family:'Spectral','Iowan Old Style',Georgia,serif">Weekly Alert Subscriptions</h1>
<p><a href="/admin/scans?token=${encodeURIComponent(token)}">← Back to scans</a></p>
<table border="1" cellpadding="10" cellspacing="0" style="background:#fff;width:100%;max-width:1200px">
<tr><th>Created</th><th>Company</th><th>Email</th><th>Active</th><th>Last Alerted</th><th>Tracked</th><th>Fire</th></tr>
${subs.map(s => `
  <tr>
    <td>${escapeHtml(formatDate(s.created_at))}</td>
    <td>${escapeHtml(s.company_name)}</td>
    <td>${escapeHtml(s.email)}</td>
    <td>${s.active ? "Yes" : "No"}</td>
    <td>${s.last_alerted_at ? escapeHtml(formatDate(s.last_alerted_at)) : "Never"}</td>
    <td>${(s.alerted_notice_ids || []).length}</td>
    <td>
      <form method="POST" action="/admin/subscriptions/${s.id}/fire?token=${encodeURIComponent(token)}">
        <button style="background:#1d6b4f;color:#fff;border:0;padding:6px 10px;cursor:pointer">Fire Now</button>
      </form>
    </td>
  </tr>`).join("")}
</table>
</body></html>`);
}));

app.post("/admin/subscriptions/:id/fire", requireAdmin, asyncRoute(async (req, res) => {
  const sub = await getSubscription(req.params.id);
  if (!sub) {
    res.status(404).json({ error: "Subscription not found" });
    return;
  }
  runWeeklyAlert(sub.id).catch(err => {
    console.error(`[alerts] manual fire failed for ${sub.id}`, err);
    captureError(err, { alertFire: { subscriptionId: sub.id } });
  });
  res.json({ message: "Alert fired. Check logs for result." });
}));

app.use((err: any, req: express.Request, res: express.Response, _next: express.NextFunction) => {
  console.error("[route] failed", err);
  captureError(err, { route: { method: req.method, path: req.path } });

  if (res.headersSent) return;

  res.status(500).json({ error: "Internal server error" });
});

initDb()
  .then(() => {
    // Railway split-runtime setup later:
    // web service: RUN_WEB=true, RUN_WORKER=false
    // worker service: RUN_WEB=false, RUN_WORKER=true
    // Missing flags keep the current beta single-service behavior and run both.
    if (RUN_WORKER) {
      startScanWorker();
      startAlertWorker();
      startBriefingWorker();
      startSignalsWorker();
    } else {
      console.log("[queue] worker disabled by RUN_WORKER=false");
    }

    if (RUN_WEB) {
      app.listen(PORT, () => {
        console.log(`[server] GovRevenue Agent running on port ${PORT}`);
        // Warm up live desk caches on startup — staggered to avoid hammering Contracts Finder.
        // Compiles one desk every 8s; 24 desks ≈ 3min total, well within rate limits.
        const liveDesks = DESK_PROFILES.filter(d => d.live);
        let delay = 0;
        for (const desk of liveDesks) {
          setTimeout(() => {
            getDeskCache(desk.slug)
              .then(cached => {
                const isStale = !cached || (Date.now() - new Date(cached.cached_at).getTime() > DESK_CACHE_TTL_MS);
                if (isStale) {
                  console.log(`[desk] warm-up: compiling ${desk.slug}`);
                  compileDeskInBackground(desk).catch(err => captureError(err, { desk: { slug: desk.slug } }));
                } else {
                  console.log(`[desk] warm-up: ${desk.slug} is fresh, skipping`);
                }
              })
              .catch(() => {
                compileDeskInBackground(desk).catch(err => captureError(err, { desk: { slug: desk.slug } }));
              });
          }, delay);
          delay += 8_000;
        }
      });
    } else {
      console.log("[server] web disabled by RUN_WEB=false");
    }
  })
  .catch(err => {
    console.error("[startup] failed", err);
    process.exit(1);
  });
