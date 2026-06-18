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
import * as Sentry from "@sentry/node";
import { Pool } from "pg";
import OpenAI from "openai";
import { z } from "zod";
import { Queue, Worker } from "bullmq";
import { Redis } from "ioredis";
import puppeteer from "puppeteer";
import { renderWorldClassDashboard } from "./designEngine.js";
import { buildPdfStorageKey, isPdfStorageConfigured, storePdfObject } from "./lib/pdfStorage.js";
import { buildScanLinks, isEmailConfigured, notifyScanCompleted, notifyScanFailed, sendWeeklyAlert } from "./lib/emailNotifications.js";
type ScanStatus = "pending" | "running" | "completed" | "failed";
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
  value_amount: number | null;
  status: string;
  fetched_at: string;
};

const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));
app.use(express.urlencoded({ extended: true, limit: "2mb" }));

const PORT = Number(process.env.PORT || 3000);
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4.1-mini";
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || "change-me-now";
const REDIS_URL = process.env.REDIS_URL || null;
const RUN_WEB = process.env.RUN_WEB !== "false";
const RUN_WORKER = process.env.RUN_WORKER !== "false";
const SENTRY_DSN = process.env.SENTRY_DSN || "";
const SENTRY_ENABLED = Boolean(SENTRY_DSN);

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
const briefMemStore = new Map<string, { id: string; email: string; category: string | null; created_at: string }>();
const deskCacheMemStore = new Map<string, { data: ProcurementData; cached_at: string }>();
const compilingDesks = new Set<string>();
const scanEvents = new EventEmitter();
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

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

function escapeHtml(value: string) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
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

function formatMoney(value: number | null | undefined) {
  if (typeof value !== "number" || Number.isNaN(value)) return "-";
  return new Intl.NumberFormat("en-GB", {
    style: "currency",
    currency: "GBP",
    maximumFractionDigits: 0
  }).format(value);
}

function formatDate(value: string | null | undefined) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleDateString("en-GB", { day: "2-digit", month: "short", year: "numeric" });
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
    valueLow: typeof item.valueLow === "number" ? item.valueLow : null,
    valueHigh: typeof item.valueHigh === "number" ? item.valueHigh : null,
    awardedValue: typeof item.awardedValue === "number" ? item.awardedValue : null,
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
  const amount = typeof tender?.value?.amount === "number" ? tender.value.amount : null;
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

async function findTenderSearch(keywords: string[]): Promise<{ notices: ProcurementNotice[]; errors: string[] }> {
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

    const response = await fetch(url, { headers: { Accept: "application/json" } });

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
  size: number
): Promise<{ notices: ProcurementNotice[]; total: number }> {
  let lastError = "";
  for (const url of CF_ENDPOINTS) {
    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({ searchCriteria, size, from })
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
  keyword: string
): Promise<ProcurementNotice[]> {
  const PAGE_SIZE = 100;
  const all: ProcurementNotice[] = [];
  let from = 0;

  while (true) {
    const { notices, total } = await contractsFinderPage(searchCriteria, keyword, from, PAGE_SIZE);
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
      id           TEXT PRIMARY KEY,
      category     TEXT NOT NULL,
      title        TEXT NOT NULL,
      buyer        TEXT,
      source       TEXT NOT NULL,
      source_url   TEXT NOT NULL,
      notice_date  TIMESTAMPTZ,
      value_amount BIGINT,
      status       TEXT NOT NULL,
      fetched_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_homepage_signals_cat_fetched
      ON homepage_signals (category, fetched_at DESC);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS briefing_subscribers (
      id          TEXT PRIMARY KEY,
      email       TEXT NOT NULL,
      category    TEXT,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE (email)
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS desk_cache (
      slug        TEXT PRIMARY KEY,
      data        JSONB NOT NULL,
      cached_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  console.log("[db] ready");
}

async function upsertSignals(signals: HomepageSignal[]): Promise<void> {
  if (signals.length === 0) return;
  if (pool) {
    for (const s of signals) {
      await pool.query(
        `INSERT INTO homepage_signals (id, category, title, buyer, source, source_url, notice_date, value_amount, status, fetched_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
         ON CONFLICT (id) DO UPDATE SET fetched_at = EXCLUDED.fetched_at`,
        [s.id, s.category, s.title, s.buyer, s.source, s.source_url,
         s.notice_date, s.value_amount, s.status, s.fetched_at]
      );
    }
  } else {
    for (const s of signals) sigMemStore.set(s.id, s);
  }
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
      `SELECT COUNT(*) AS n FROM homepage_signals WHERE fetched_at > NOW() - INTERVAL '24 hours'`
    );
    return parseInt(r.rows[0]?.n || "0", 10);
  }
  const cutoff = new Date(Date.now() - 86_400_000).toISOString();
  return [...sigMemStore.values()].filter(s => s.fetched_at > cutoff).length;
}

async function findSamplePdf(): Promise<string | null> {
  if (pool) {
    const r = await pool.query<{ pdf_storage_url: string }>(
      `SELECT pdf_storage_url FROM scans WHERE pdf_storage_url IS NOT NULL ORDER BY created_at DESC LIMIT 1`
    );
    return r.rows[0]?.pdf_storage_url || null;
  }
  for (const s of memoryStore.values()) {
    if (s.pdf_storage_url) return s.pdf_storage_url;
  }
  return null;
}

async function queryDeskSignals(categories: string[]): Promise<Map<string, HomepageSignal>> {
  const out = new Map<string, HomepageSignal>();
  if (pool) {
    const r = await pool.query<HomepageSignal>(
      `SELECT DISTINCT ON (category) id, category, title, buyer, source, source_url, notice_date, value_amount, status, fetched_at
       FROM homepage_signals WHERE category = ANY($1) ORDER BY category, fetched_at DESC`,
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

type ChartDataPoint = { month: string; total_m: number };
async function queryChartData(): Promise<{ points: ChartDataPoint[]; illustrative: boolean }> {
  if (pool) {
    const r = await pool.query<ChartDataPoint>(
      `SELECT to_char(date_trunc('month', fetched_at), 'Mon') AS month,
              ROUND(SUM(COALESCE(value_amount, 0)) / 1e6::numeric, 2)::float AS total_m
       FROM homepage_signals
       WHERE fetched_at > NOW() - INTERVAL '12 months'
         AND value_amount IS NOT NULL
       GROUP BY date_trunc('month', fetched_at)
       ORDER BY date_trunc('month', fetched_at)`
    );
    return { points: r.rows, illustrative: r.rows.length < 3 };
  }
  return { points: [], illustrative: true };
}

const DESK_CACHE_TTL_MS = 6 * 60 * 60 * 1000;

async function getDeskCache(slug: string): Promise<{ data: ProcurementData; cached_at: string } | null> {
  if (pool) {
    const r = await pool.query<{ data: ProcurementData; cached_at: string }>(
      `SELECT data, cached_at::text FROM desk_cache WHERE slug = $1`,
      [slug]
    );
    return r.rows[0] || null;
  }
  return deskCacheMemStore.get(slug) || null;
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
    res.status(401).json({ error: "Unauthorized. Add ?token=YOUR_ADMIN_TOKEN" });
    return;
  }
  next();
}

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

  const kwController = new AbortController();
  const kwTimer = setTimeout(() => kwController.abort(), 90_000);
  let response;
  try {
    response = await openai.chat.completions.create({
      model: "gpt-4.1-mini",
      messages: [{ role: "user", content: prompt }],
      response_format: { type: "json_object" },
      temperature: 0.1,
      max_tokens: 300
    }, { signal: kwController.signal });
  } finally {
    clearTimeout(kwTimer);
  }

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

async function pullProcurementData(input: z.infer<typeof intakeSchema>): Promise<ProcurementData> {
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
        keyword
      )));
    } catch (error: any) {
      captureError(error, { dataPull: { source: "contracts_finder", status: "open", keyword } });
      errors.push(`Open search failed for "${keyword}": ${error?.message || error}`);
    }

    try {
      awarded.push(...(await contractsFinderSearchAll(
        { ...base, types: ["Contract"], statuses: ["Awarded"], awardedFrom: awardedDateFrom },
        keyword
      )));
    } catch (error: any) {
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
        "cpv"
      )));
    } catch (error: any) {
      errors.push(`CPV open search failed: ${error?.message || error}`);
    }
    try {
      awarded.push(...(await contractsFinderSearchAll(
        { ...cpvBase, types: ["Contract"], statuses: ["Awarded"], awardedFrom: awardedDateFrom },
        "cpv"
      )));
    } catch (error: any) {
      errors.push(`CPV awarded search failed: ${error?.message || error}`);
    }
  }

  const companiesHouse = await companiesHouseSearch(input.companyName);
  if (companiesHouse.errors.length) {
    for (const error of companiesHouse.errors) {
      errors.push(`Companies House: ${error}`);
    }
  }

  const findTender = await findTenderSearch(keywords);
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
  const open = data.contractsFinder.open.slice(0, 14);
  const awarded = data.contractsFinder.awarded.slice(0, 14);
  const findTender = (data.findTender?.notices || []).slice(0, 14);
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
  const text = [
    input?.companyName, input?.mainServices, input?.secondaryServices,
    input?.mainGoal, input?.preferredOutput, input?.idealBuyers,
    input?.frameworkStatus, input?.lastPublicContract
  ].filter(Boolean).join(" ");
  return resolveSector(text);
}

function resolveSectorFromScan(scan: any): SectorResult {
  const input: any = scan.input_json || {};
  const text = [
    input.companyName, input.mainServices, input.secondaryServices,
    input.idealBuyers, input.mainGoal, input.preferredOutput,
    input.frameworkStatus, input.lastPublicContract,
    scan.company_name, scan.sector, scan.industry,
    scan.services, scan.main_services,
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
      <td style="padding:9px 14px;font-size:14px;color:#24140f;border-bottom:1px solid #e8d9c4">${escapeHtml(e.name)}</td>
      <td style="padding:9px 14px;text-align:center;font-size:13px;font-family:monospace;border-bottom:1px solid #e8d9c4">${e.count}</td>
      <td style="padding:9px 14px;text-align:right;font-size:13px;font-family:monospace;border-bottom:1px solid #e8d9c4">${escapeHtml(val)}</td>
      <td style="padding:9px 14px;border-bottom:1px solid #e8d9c4">
        <div style="display:flex;align-items:center;gap:8px">
          <div style="height:6px;width:${Math.max(pct, 2)}%;background:#a97932;border-radius:3px"></div>
          <span style="font-size:11px;font-family:monospace;color:#6f5b50">${pct}%</span>
        </div>
      </td>
      <td style="padding:9px 14px;font-size:12px;font-family:monospace;color:#6f5b50;border-bottom:1px solid #e8d9c4">${escapeHtml(latest)}</td>
    </tr>`;
  }).join("");
  return `<section style="margin:40px 0;background:#fffaf3;border:1px solid #d2b88f;padding:28px 32px" class="no-print">
  <h2 style="font-family:Georgia,serif;font-size:22px;font-weight:600;margin-bottom:6px;color:#24140f">Incumbent map</h2>
  <p style="font-size:13px;color:#6f5b50;margin-bottom:18px;font-family:monospace">Derived from awarded contract records in this dataset. Not exhaustive — covers notices returned by keyword search only.</p>
  <table style="width:100%;border-collapse:collapse">
    <thead>
      <tr style="background:#f3eadc">
        <th style="padding:8px 14px;text-align:left;font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#8a6f5a;font-weight:600">Supplier</th>
        <th style="padding:8px 14px;text-align:center;font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#8a6f5a;font-weight:600">Awards</th>
        <th style="padding:8px 14px;text-align:right;font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#8a6f5a;font-weight:600">Total value</th>
        <th style="padding:8px 14px;font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#8a6f5a;font-weight:600">Share</th>
        <th style="padding:8px 14px;font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#8a6f5a;font-weight:600">Latest award</th>
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

Return clean Markdown only.

Use this exact structure:

# GovRevenue Scan: [Company Name]

## 1. Executive Decision Panel
Give a clear commercial decision panel with these exact fields:

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
- The verdict must be commercially honest.
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

## 3. Intelligence Dashboard Summary
Explain commercially what the dashboard means:
- pulled records vs relevant records
- verified vs inferred evidence
- addressable value signal
- why gross pulled value is not forecast revenue
- compact value view, using £k / £m / £bn where useful

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
Include:
- Week 1: evidence and access verification
- Week 2: capability statement and bid pack
- Week 3: buyer outreach and route qualification
- Week 4: selective bid / partner activation

Then include:
- Documents needed before bidding
- Capability statement bullets
- Buyer outreach email
- Partner outreach email
- LinkedIn message
- Bid/no-bid checklist

Make every item sector-specific. Avoid generic filler.

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


async function callLlmReport(prompt: string): Promise<string> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 90_000);
  try {
    const response = await openai.responses.create({
      model: OPENAI_MODEL,
      tools: [{ type: "web_search" } as any],
      input: prompt
    }, { signal: controller.signal });
    clearTimeout(timer);
    return enforceDataQualityLanguage(response.output_text || "No report returned.");
  } catch (firstError: any) {
    try {
      const response = await openai.responses.create({
        model: OPENAI_MODEL,
        tools: [{ type: "web_search_preview" } as any],
        input: prompt
      }, { signal: controller.signal });
      clearTimeout(timer);
      return enforceDataQualityLanguage(response.output_text || "No report returned.");
    } catch (secondError: any) {
      clearTimeout(timer);
      captureError(secondError, {
        openai: { model: OPENAI_MODEL, fallbackAfterPrimaryFailure: true, primaryError: firstError?.message || String(firstError) }
      });
      throw secondError;
    }
  }
}

async function runScan(id: string, input: z.infer<typeof intakeSchema>) {
  await updateScan(id, { status: "running", error_message: null });
  await emitScanStage(id, "fetching");

  try {
    const data = await pullProcurementData(input);
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
    console.error(`[scan] failed ${id}`, err);
    captureError(err, { scan: { id, companyName: input.companyName, status: "failed" } });

    await updateScan(id, {
      status: "failed",
      error_message: err?.message || String(err)
    });
    await emitScanStage(id, "failed");

    await notifyScanFailed({
      scanId: id,
      companyName: input.companyName,
      status: "failed",
      errorSummary: err?.message || String(err)
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
      { label: "Fire Safety, Compliance & Remediation", keywords: ["fire safety", "asbestos", "legionella", "compliance", "remediation", "access control", "cctv"],                 subcategories: ["Fire safety works","Fire doors","Asbestos removal","Legionella & water hygiene","Electrical testing","Emergency lighting","Access control & CCTV","Fire risk assessment","ACM cladding removal","EWS1 surveys","Waking watch","Compartmentalisation","Cavity barriers","Fire stopping","Smoke detectors","AOV systems","Fire suppression"] },
      { label: "Decarbonisation & Retrofit",            keywords: ["retrofit", "decarb", "energy efficiency", "solar", "heat pump", "led upgrade", "net zero", "insulation"],       subcategories: ["Energy efficiency","Retrofit works","Heat pumps","Solar PV","LED upgrades","Insulation","Net zero works","Air source heat pumps","Ground source heat pumps","Cavity wall insulation","Solid wall insulation","Loft insulation","PAS 2035 retrofit","EPC improvements","Triple glazing","Demand response"] },
      { label: "Grounds, Civils & External Works",      keywords: ["grounds", "civil", "drainage", "surfacing", "landscaping", "fencing", "car park", "playground", "footpath"],    subcategories: ["Drainage","Surfacing","Landscaping","Fencing","Car parks","Playgrounds","Footpaths","Tarmac resurfacing","Block paving","Boundary walls","Security fencing","Cycle shelters","Bin stores","Street furniture","Retaining walls","Kerbing","SUDS drainage","Attenuation tanks"] },
      { label: "Supplies, Materials & Hire",            keywords: ["materials", "supplies", "plant hire", "tool hire", "scaffolding", "welfare cabin", "building materials"],        subcategories: ["Building materials","Plumbing supplies","Electrical supplies","Plant hire","Tool hire","Scaffolding","Welfare cabins","Timber & joinery","Ironmongery","Fixings & fasteners","Paint & coatings","Insulation materials","Roof materials","Floor coverings","Aggregates","Ready-mixed concrete","Skip hire","Temporary electrics"] },
      { label: "Professional Services & Consultancy",   keywords: ["consultancy", "surveying", "project management", "architectural", "quantity", "structural", "clerk of works"],  subcategories: ["Quantity surveying","Project management","Building surveying","Architectural services","Structural engineering","Clerk of works","Estate strategy","CDM coordination","Principal designer","Planning consultancy","Fire engineering","Mechanical design","Electrical design","Party wall surveying","Condition surveys","Asset management","Energy consultancy","Due diligence"] },
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
      { label: "Managed Services",    keywords: ["managed service", "total fm", "integrated", "outsourced", "facilities management"],            subcategories: ["Total FM","Integrated FM","Outsourced FM","TUPE transfers","KPI management"] },
      { label: "Energy Management",   keywords: ["energy", "utilities", "metering", "sustainability", "carbon"],                                 subcategories: ["Energy procurement","Utilities management","Smart metering","Carbon reporting","Sustainability"] },
      { label: "Compliance & Safety", keywords: ["compliance", "fire safety", "asbestos", "legionella", "water treatment", "pat testing"],      subcategories: ["Fire safety","Asbestos management","Legionella control","Water treatment","PAT testing","Statutory compliance"] },
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
      { label: "SEND & Alternative Provision", keywords: ["send", "special educational needs", "alternative provision", "pupil referral", "exclusion"],        subcategories: ["SEND support","Alternative provision","Pupil referral units","Educational psychology","Specialist tutoring","EHCP provision","Short breaks","Post-16 SEND"] },
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
      { label: "Software & Licensing", keywords: ["software", "licence", "crm", "erp", "mis", "application", "platform"], subcategories: ["ERP systems","CRM platforms","MIS systems","HR software","Finance systems","Document management","Case management","GIS systems","Asset management software","Email & productivity","Planning software","Workforce management"] },
      { label: "Cyber Security", keywords: ["cyber", "security operations", "soc", "penetration", "vulnerability", "endpoint", "siem"], subcategories: ["Penetration testing","SOC services","Endpoint protection","Vulnerability assessment","SIEM platforms","GDPR compliance","Identity & access","Phishing simulation","Data loss prevention","Incident response","Threat intelligence","Security awareness training"] },
      { label: "Networks & Infrastructure", keywords: ["network", "infrastructure", "broadband", "wifi", "connectivity", "fibre", "telecoms"], subcategories: ["Network infrastructure","Wi-Fi deployment","Connectivity services","Fibre installation","Telecoms","WAN/LAN","SD-WAN","Unified communications","Telephony systems","Public Wi-Fi","Smart city connectivity","MPLS"] },
      { label: "IT Support & Managed Services", keywords: ["it support", "managed service", "service desk", "helpdesk", "desktop support", "device"], subcategories: ["Service desk","Desktop support","Device procurement","Print management","ITSM platforms","IT outsourcing","Field support","Asset lifecycle","Patch management","IT training","Mobile device management","Field engineering"] },
      { label: "Digital Transformation", keywords: ["digital transformation", "agile", "user research", "ux", "discovery", "data strategy"], subcategories: ["Digital strategy","User research","UX/UI design","Service design","Data strategy","Analytics platforms","Business intelligence","AI & automation","RPA","Open data","API development","Accessibility compliance"] },
    ]
  },
  { slug: "social-care", label: "Adult Social Care", standfirst: "Domiciliary care, residential placements, learning disability, reablement, and carer support commissioned by councils and ICBs.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "adult social care domiciliary care residential care learning disability reablement supported living carer support", idealBuyers: "local authorities councils integrated care boards NHS England", mainGoal: "find adult social care commissioning contracts" }),
    categories: [
      { label: "Domiciliary & Home Care", keywords: ["domiciliary", "home care", "personal care", "home help", "care at home"], subcategories: ["Personal care","Domestic assistance","Medication support","Companionship","Night sits","Live-in care","Reablement support","Emergency home care","Direct payments support","Carers' assessments","Overnight care","Sensory impairment support"] },
      { label: "Residential & Nursing Care", keywords: ["residential care", "nursing home", "care home", "residential placement", "elderly care"], subcategories: ["Residential placements","Nursing placements","Dementia specialist","End-of-life care","Respite residential","EMI beds","Enhanced nursing","Intermediate residential","Frailty services","Specialist residential"] },
      { label: "Learning Disability Services", keywords: ["learning disability", "autism", "challenging behaviour", "complex needs"], subcategories: ["Supported living (LD)","Residential (LD)","Day services","Community support","Behaviour support","Autism services","Transition support","Short breaks (LD)","Advocacy","Hospital discharge (LD)","Forensic LD","Positive behaviour support"] },
      { label: "Mental Health Support", keywords: ["mental health support", "community mental health", "peer support", "recovery", "crisis support"], subcategories: ["Community mental health","Crisis support","Peer support","Wellbeing services","Recovery support","Employment support (MH)","Floating support (MH)","Dual diagnosis","IAPT delivery","Advocacy (MH)","Housing-related support","Personalised care"] },
      { label: "Reablement & Intermediate Care", keywords: ["reablement", "intermediate care", "step-down", "hospital discharge", "rehabilitation"], subcategories: ["Reablement services","Intermediate care","Hospital discharge","Step-down care","Falls prevention","Telecare","Care technology","Community rehabilitation","Virtual ward support","Extra care housing","Re-enablement","Assistive technology"] },
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
      { label: "Waste Infrastructure & Equipment", keywords: ["waste equipment", "vehicle", "compactor", "container", "recycling centre", "hwrc"], subcategories: ["Refuse vehicles","Recycling vehicles","Compactors","Wheeled bins","Containers","HWRCs","Civic amenity sites","Skip lorries","Bin sensors","Smart waste technology","Solar compactors","Underground bins"] },
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
      { label: "Council Communications", keywords: ["council communications", "resident communications", "public engagement comms", "consultation"], subcategories: ["Resident newsletters","Council website","Digital communications","Annual report","Budget consultation","Resident surveys","Neighbourhood comms","Ward briefings","Corporate publications","Intranet","Social media (council)","Corporate branding"] },
      { label: "PR & Media Relations", keywords: ["media relations", "press office", "public relations", "crisis comms", "reputation management"], subcategories: ["Press office","Spokesperson training","Crisis communications","Media monitoring","Stakeholder relations","Reputation management","Parliamentary affairs","Lobbying support","Social media PR","Broadcast PR","Press release writing","Media training"] },
      { label: "Print & Design", keywords: ["print management", "graphic design", "signage", "wayfinding", "publication", "brand identity"], subcategories: ["Graphic design","Brand identity","Print management","Signage and wayfinding","Exhibition materials","Annual reports","Leaflets and posters","Accessibility design","Large-format print","Corporate stationery","Environmental graphics","Translation and print"] },
      { label: "Digital Marketing & Media", keywords: ["digital marketing", "social media management", "seo", "paid media", "email marketing", "content strategy"], subcategories: ["Social media management","Paid search (PPC)","SEO","Email marketing","Content strategy","Video production","Animation","Podcasting","Influencer outreach","Digital analytics","Website development","App development"] },
      { label: "Consultation & Engagement", keywords: ["consultation", "engagement", "stakeholder", "co-production", "citizen engagement"], subcategories: ["Public consultation","Co-production","Citizen assemblies","Online engagement","Face-to-face events","Accessibility engagement","Community engagement","Hard-to-reach groups","Equalities consultation","Feedback analysis","Deliberative research","Participatory budgeting"] },
    ]
  },
  { slug: "leisure", label: "Leisure & Culture", standfirst: "Leisure centre management, libraries, arts, parks, sports development, and heritage for councils and public bodies.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "leisure management swimming pool sports centre library arts culture parks museums heritage sports development", idealBuyers: "local authorities district councils county councils leisure trusts", mainGoal: "find leisure and cultural services contracts" }),
    categories: [
      { label: "Leisure Management", keywords: ["leisure management", "leisure centre", "swimming pool", "sports centre", "gym", "leisure trust"], subcategories: ["Leisure centre management","Swimming pools","Sports halls","Fitness suites","Outdoor athletics","Dual-use facilities","Community sports","Leisure trust transfer","Pricing and tariff","Health referral","Disability sport","Outdoor education"] },
      { label: "Library Services", keywords: ["library", "lending", "library service", "book purchasing", "reading", "information service"], subcategories: ["Library management","Mobile libraries","Book purchasing","Self-service kiosks","Library IT systems","Reading groups","Rhyme time","Digital inclusion","Home delivery","Archive services","Library buildings","Community library"] },
      { label: "Arts & Culture", keywords: ["arts", "culture", "theatre", "museum", "gallery", "heritage", "creative arts"], subcategories: ["Theatre management","Art gallery","Museum management","Heritage interpretation","Public art commissioning","Artist residencies","Arts development","Cultural programme","Community arts","Festival support","Arts fundraising","Cultural strategy"] },
      { label: "Parks & Open Spaces", keywords: ["parks", "open space", "playground", "outdoor recreation", "allotments", "nature reserve"], subcategories: ["Parks management","Play area maintenance","Allotments","Nature reserves","Sports pitches","Outdoor gym","Footpath maintenance","Countryside access","Urban green space","Biodiversity management","Ecology surveys","Green infrastructure"] },
      { label: "Sports Development", keywords: ["sports development", "active travel", "cycling", "walking", "healthy active"], subcategories: ["Sports development","Active travel","Cycling programmes","Walking networks","Active communities","Disability sport","School sport","Club development","Workforce development (sport)","National governing bodies","Swim England","Move More"] },
      { label: "Museums & Heritage", keywords: ["museum", "heritage", "conservation", "archive", "collection", "artefact"], subcategories: ["Museum collections","Conservation and restoration","Archive digitisation","Heritage consultancy","Listed building work","Archaeological surveys","Heritage at risk","Interpretation design","Loan services","Oral history","War memorial restoration","Historic environment"] },
    ]
  },
  { slug: "planning", label: "Planning & Regeneration", standfirst: "Planning consultancy, urban regeneration, economic development, heritage, transport planning, and land strategy for councils and combined authorities.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "planning consultancy urban regeneration economic development masterplanning heritage transport planning land development", idealBuyers: "local authorities combined authorities planning authorities Homes England", mainGoal: "find planning and regeneration contracts" }),
    categories: [
      { label: "Planning Consultancy", keywords: ["planning", "planning consultancy", "planning application", "local plan", "development management"], subcategories: ["Local plan support","Development management","Planning applications","Pre-application advice","Appeals","Planning policy","Infrastructure delivery","Community infrastructure levy","Neighbourhood planning","Planning enforcement","Viability assessments","EIA coordination"] },
      { label: "Urban Regeneration", keywords: ["regeneration", "urban", "town centre", "masterplan", "place making", "renewal", "levelling up"], subcategories: ["Town centre regeneration","Masterplanning","Place making","High street recovery","Levelling Up programmes","UKSPF delivery","Heritage-led regeneration","Housing-led regeneration","Industrial site reclamation","Compulsory purchase","Vacant buildings","Business improvement districts"] },
      { label: "Economic Development", keywords: ["economic development", "inward investment", "business support", "enterprise", "growth hub"], subcategories: ["Inward investment","Business support","Enterprise zones","Growth hubs","Employment land","Skills and employment","Business rates incentives","Trade missions","Economic impact","Supply chain development","Innovation hubs","Start-up support"] },
      { label: "Heritage & Conservation", keywords: ["heritage conservation", "listed building", "historic", "building survey conservation", "conservation area"], subcategories: ["Heritage surveys","Conservation area appraisals","Listed building advice","Historic environment","Archaeology","Building recording","Heritage impact","SMR support","Historic landscape","War memorial restoration","Heritage at risk","Grant-aided works"] },
      { label: "Transport Planning", keywords: ["transport planning", "transport assessment", "traffic", "active travel", "movement strategy"], subcategories: ["Transport assessment","Traffic modelling","Active travel plans","Parking strategy","Travel plans","LTP development","Bus strategy","Cycling and walking","Road safety audits","Transport impact assessment","Vision Zero","Freight strategy"] },
      { label: "Property & Land", keywords: ["property disposal", "land", "asset disposal", "valuation", "compulsory purchase", "estate management"], subcategories: ["Asset valuation","Compulsory purchase","Land disposal","Development appraisal","Estate strategy","Property acquisition","Lease management","Rating appeals","Asset register","Commercial property","Surplus land","Community asset transfer"] },
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
      { label: "Consultation & Participation", keywords: ["consultation", "engagement research", "participation", "co-production", "deliberative"], subcategories: ["Deliberative panels","Citizens' assemblies","Online consultation platforms","Stakeholder mapping","Engagement strategy","Community research","Young people's participation","Hard-to-reach research","Equalities analysis","Accessibility research","JSNA consultation","Public panel management"] },
      { label: "Market & Economic Research", keywords: ["market research", "economic analysis", "feasibility study", "cost benefit", "option appraisal"], subcategories: ["Feasibility studies","Cost-benefit analysis","Options appraisal","Market analysis","Demand forecasting","Competition analysis","Socioeconomic impact","ROI modelling","Wellbeing economics","Green Book appraisal","Economic modelling","Sector intelligence"] },
    ]
  },
  { slug: "consulting", label: "Central Gov Consulting", standfirst: "Management consulting, digital transformation, programme delivery, policy development, and commercial advisory for central government departments.", live: true,
    pinnedProfile: intakeSchema.parse({ companyName: "GovRevenue Desk", mainServices: "management consulting digital transformation programme delivery policy development operating model commercial advisory cabinet office", idealBuyers: "central government departments Cabinet Office HMRC DVLA DWP Home Office NHS England", mainGoal: "find central government consulting contracts" }),
    categories: [
      { label: "Digital Transformation", keywords: ["digital transformation", "digitisation", "digital strategy", "digital government", "service redesign"], subcategories: ["Digital strategy","Technology assessment","Digital service redesign","Legacy modernisation","API-first design","Cloud migration strategy","Data architecture","AI readiness","Digital leadership","GDS standards","GOV.UK Notify","Service standard assessment"] },
      { label: "Programme & Project Delivery", keywords: ["programme delivery", "project management", "pmo", "agile delivery", "prince2", "gateway review"], subcategories: ["Programme management","PMO setup","Agile delivery","Portfolio management","Benefits tracking","Schedule management","Risk register","Governance frameworks","Gateway reviews","Delivery assurance","IPA reviews","Major projects authority"] },
      { label: "Organisational Transformation", keywords: ["organisational transformation", "operating model", "restructuring", "shared services", "merger"], subcategories: ["Operating model design","Shared services","Merger and acquisition","Workforce redesign","Culture change","Behavioural change","Leadership development","Succession planning","Target operating model","Benchmarking","OD consulting","Arm's-length body reform"] },
      { label: "Policy Development", keywords: ["policy development", "policy design", "regulation", "strategy", "white paper", "green paper"], subcategories: ["Policy design","Regulatory impact","Strategy development","White paper support","Consultation design","Ministerial briefings","Evidence synthesis","Parliamentary work","Public inquiry support","Arms-length bodies","Spending review","Policy simulation"] },
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


function renderDeskCard(sig: HomepageSignal, tag: string): string {
  const dateStr = sig.notice_date
    ? new Date(sig.notice_date).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" })
    : "—";
  const buyer = sig.buyer ? escapeHtml(sig.buyer.slice(0, 60)) : "Buyer not stated";
  const title = escapeHtml(sig.title.slice(0, 90));
  return `<article class="desk reveal"><div class="tag"><em>${escapeHtml(tag)}</em><time>${escapeHtml(dateStr)}</time></div><h3>${title}</h3><p>${buyer}</p><div class="src">Source: ${escapeHtml(sig.source)} &middot; public record</div></article>`;
}

async function refreshHomepageSignals(): Promise<void> {
  console.log("[signals] refresh started");
  for (const cat of SIGNAL_CATEGORIES) {
    try {
      const data = await pullProcurementData(cat.input);
      const allNotices: ProcurementNotice[] = [
        ...data.contractsFinder.open,
        ...data.contractsFinder.awarded,
        ...(data.findTender?.notices || [])
      ];
      const deduped = dedupeNotices(allNotices);
      const now = nowIso();
      const signals: HomepageSignal[] = deduped.map(n => ({
        id: n.url || `${n.source}-${n.id}`,
        category: cat.key,
        title: n.title.slice(0, 200),
        buyer: n.buyer && n.buyer !== "Not stated" ? n.buyer.slice(0, 120) : null,
        source: n.source === "Find a Tender" ? "FTS" : "CF",
        source_url: n.url,
        notice_date: n.publishedDate || n.awardedDate || null,
        value_amount: (() => { const v = n.valueHigh ?? n.valueLow ?? n.awardedValue; return v != null ? Math.round(v) : null; })(),
        status: n.status || "unknown",
        fetched_at: now
      })).filter(s => s.id && s.title);
      await upsertSignals(signals);
      console.log(`[signals] ${cat.key}: upserted ${signals.length}`);
    } catch (err: any) {
      console.error(`[signals] ${cat.key} refresh failed: ${err?.message}`);
      captureError(err, { signalRefresh: { category: cat.key } });
    }
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
      { connection: redisConnection as any, concurrency: 1, lockDuration: 300_000, stalledInterval: 60_000 }
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

function scoreCard(title: string, score: number, note: string) {
  return `
    <div class="score-card">
      <div class="score-top">
        <span>${escapeHtml(title)}</span>
        <strong>${score}</strong>
      </div>
      <div class="score-track">
        <div class="score-fill" style="width:${clampScore(score)}%"></div>
      </div>
      <p>${escapeHtml(scoreLabel(score))}. ${escapeHtml(note)}</p>
    </div>
  `;
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
      <div class="close-kicker">GovRevenue commercial close</div>
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

interface ParsedEdp {
  verdict: string;
  canTheyWinNow: string;
  bestFirstMoneyRoute: string;
  fastestActionThisWeek: string;
  mainBlocker: string;
  evidenceGrade: string;
  recommendedRoute: string;
}

function parseEdpFromMarkdown(markdown: string): ParsedEdp | null {
  const sectionMatch = markdown.match(
    /##\s*1\.\s*Executive Decision Panel([\s\S]*?)(?=\n##\s+\d+\.|\n##\s+[A-Z]|$)/i
  );
  if (!sectionMatch) return null;

  const section = sectionMatch[1];

  function extractRow(fieldPattern: string): string {
    const re = new RegExp(
      `\\|\\s*${fieldPattern}\\s*\\|\\s*([^|\\n]+?)\\s*\\|`,
      "i"
    );
    const m = section.match(re);
    return m ? m[1].trim() : "";
  }

  const verdict = extractRow("Verdict");
  const evidenceGrade = extractRow("Evidence Grade");

  if (!verdict && !evidenceGrade) return null;

  return {
    verdict: verdict || "",
    canTheyWinNow: extractRow("Can they win now\\?"),
    bestFirstMoneyRoute: extractRow("Best first money route"),
    fastestActionThisWeek: extractRow("Fastest action this week"),
    mainBlocker: extractRow("Main blocker"),
    evidenceGrade: evidenceGrade || "",
    recommendedRoute: extractRow("Recommended route"),
  };
}

function stripEdpFromMarkdown(markdown: string): string {
  return markdown.replace(
    /##\s*1\.\s*Executive Decision Panel[\s\S]*?(?=\n##\s+\d+\.|\n##\s+[A-Z])/i,
    ""
  );
}

function stripReportTitleFromMarkdown(markdown: string): string {
  return markdown.replace(/^#\s+GovRevenue\s+Scan:[^\n]*\n?/im, "");
}

interface ConsistencyReport {
  valid: boolean;
  errors: string[];
  conflicts: string[];
}

function validateReportConsistency(
  edp: ParsedEdp | null,
  markdown: string
): ConsistencyReport {
  const errors: string[] = [];
  const conflicts: string[] = [];

  if (!edp) {
    errors.push("Executive Decision Panel could not be parsed from the report markdown.");
    return { valid: false, errors, conflicts };
  }

  if (!edp.verdict) errors.push("EDP verdict is missing.");
  if (!edp.evidenceGrade) errors.push("EDP evidence grade is missing.");
  if (!edp.canTheyWinNow) errors.push("EDP can-they-win-now is missing.");
  if (!edp.recommendedRoute) errors.push("EDP recommended route is missing.");

  const gradePattern = /\|\s*Evidence Grade\s*\|\s*([A-E])\s*\|/gi;
  const foundGrades = new Set<string>();
  for (const m of markdown.matchAll(gradePattern)) {
    foundGrades.add(m[1].toUpperCase());
  }
  if (foundGrades.size > 1) {
    conflicts.push(
      `Conflicting evidence grades in report: ${[...foundGrades].join(", ")}`
    );
  }

  const verdictPattern = /\|\s*Verdict\s*\|\s*([^|\n]+?)\s*\|/gi;
  const foundVerdicts = new Set<string>();
  for (const m of markdown.matchAll(verdictPattern)) {
    foundVerdicts.add(m[1].trim().toLowerCase());
  }
  if (foundVerdicts.size > 1) {
    conflicts.push(
      `Conflicting verdicts in report: ${[...foundVerdicts].join("; ")}`
    );
  }

  return {
    valid: errors.length === 0 && conflicts.length === 0,
    errors,
    conflicts,
  };
}

function waitingPage(scan: ScanRecord): string {
  const scanId = escapeHtml(scan.id);
  const isFailed = scan.status === "failed";
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${escapeHtml(scan.company_name)} &mdash; Scanning &mdash; GovRevenue</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#f3eadc;color:#24140f;font-family:Arial,sans-serif;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:32px}
.card{max-width:560px;width:100%;background:#fffaf3;border:1px solid #d2b88f;padding:40px 44px}
.brand{font-size:11px;letter-spacing:.2em;text-transform:uppercase;color:#8a6f5a;margin-bottom:28px}
h1{font-family:Georgia,serif;font-size:24px;font-weight:600;line-height:1.25;margin-bottom:28px}
h1 b{color:#24140f}
.stage-list{list-style:none;margin-bottom:28px}
.stage{display:flex;align-items:center;gap:12px;padding:10px 0;border-bottom:1px dashed #e8d9c4;font-size:14px;color:#6f5b50;transition:color .3s}
.stage:last-child{border-bottom:0}
.dot{width:10px;height:10px;border-radius:50%;background:#d2b88f;flex-shrink:0;transition:background .3s}
.stage.active{color:#24140f}
.stage.active .dot{background:#a97932;box-shadow:0 0 0 4px #a9793230}
.stage.done{color:#1d6b4f}
.stage.done .dot{background:#1d6b4f}
.stage.fail{color:#9b2d20}
.stage.fail .dot{background:#9b2d20}
.eta{font-size:13px;color:#8a6f5a;font-family:monospace}
.err{margin-top:20px;padding:14px;background:#fdf0ee;border:1px solid #e0a090;font-size:13px;color:#9b2d20}
</style>
</head>
<body>
<div class="card">
  <div class="brand">GovRevenue &mdash; Scan</div>
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
<html>
<head>
  <title>${escapeHtml(scan.company_name)} - GovRevenue Scan</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {
      --ink:#24140f;
      --muted:#6f5b50;
      --paper:#fffaf3;
      --cream:#f3eadc;
      --line:#d2b88f;
      --gold:#a97932;
      --gold-soft:#f0e2c6;
      --green:#1d6b4f;
      --red:#9b2d20;
    }

    * { box-sizing:border-box; }

    body {
      margin:0;
      background:var(--cream);
      color:var(--ink);
      font-family:Arial, sans-serif;
      -webkit-font-smoothing:antialiased;
    }

    .page {
      max-width:1120px;
      margin:0 auto;
      padding:42px 24px 80px;
    }

    .topbar {
      display:flex;
      justify-content:space-between;
      align-items:center;
      gap:16px;
      margin-bottom:18px;
    }

    .brand {
      font-size:13px;
      letter-spacing:.14em;
      text-transform:uppercase;
      color:var(--muted);
      font-weight:800;
    }

    .actions {
      display:flex;
      gap:10px;
      flex-wrap:wrap;
    }

    .btn {
      border:1px solid var(--ink);
      background:var(--ink);
      color:#fff;
      padding:11px 14px;
      font-weight:800;
      cursor:pointer;
      text-decoration:none;
      font-size:13px;
    }

    .btn.secondary {
      background:transparent;
      color:var(--ink);
    }

    .cover {
      background:
        radial-gradient(circle at top right, rgba(169,121,50,.13), transparent 34%),
        linear-gradient(135deg, #fffaf3 0%, #fbf2e4 100%);
      border:1px solid var(--line);
      padding:38px;
      box-shadow:0 22px 70px rgba(36,20,15,.12);
      break-after:auto;
    }

    .cover h1 {
      font-family:Georgia, serif;
      font-size:52px;
      line-height:1;
      margin:0 0 14px;
      letter-spacing:-.03em;
    }

    .subtitle {
      color:var(--muted);
      font-size:17px;
      max-width:820px;
      line-height:1.55;
      text-align:justify;
      text-align-last:left;
    }

    .meta {
      display:grid;
      grid-template-columns:repeat(4, 1fr);
      gap:12px;
      margin:30px 0;
    }

    .metric {
      border:1px solid var(--line);
      background:#fff;
      padding:16px;
      min-height:94px;
    }

    .metric b {
      display:block;
      font-size:12px;
      color:var(--muted);
      text-transform:uppercase;
      letter-spacing:.08em;
      margin-bottom:8px;
    }

    .metric span {
      font-family:Georgia, serif;
      font-size:25px;
      font-weight:800;
    }

    .metric small {
      display:block;
      margin-top:5px;
      color:var(--muted);
      line-height:1.35;
    }

    .data-strip {
      border-left:5px solid var(--gold);
      background:#fff;
      padding:18px;
      margin:20px 0 0;
    }

    .data-strip p {
      margin:6px 0;
      color:var(--muted);
      font-size:14px;
      text-align:justify;
      text-align-last:left;
    }

    .premium-dashboard {
      margin-top:26px;
      background:#fff;
      border:1px solid var(--line);
      padding:30px;
      break-inside:avoid;
      page-break-inside:avoid;
    }

    .section-kicker,
    .close-kicker {
      font-size:12px;
      text-transform:uppercase;
      letter-spacing:.16em;
      color:var(--gold);
      font-weight:900;
      margin-bottom:8px;
    }

    .dash-head {
      display:grid;
      grid-template-columns:1fr auto;
      gap:18px;
      align-items:start;
      margin-bottom:20px;
    }

    .dash-head h2,
    .marketing-close h2 {
      font-family:Georgia, serif;
      margin:0 0 8px;
      font-size:30px;
      letter-spacing:-.02em;
    }

    .dash-head p,
    .marketing-close p {
      margin:0;
      color:var(--muted);
      line-height:1.65;
      text-align:justify;
      text-align-last:left;
    }

    .route-pill {
      background:var(--ink);
      color:#fff;
      padding:12px 14px;
      font-weight:900;
      font-size:13px;
      max-width:230px;
      text-align:center;
    }

    .score-grid {
      display:grid;
      grid-template-columns:repeat(5, 1fr);
      gap:12px;
      margin:18px 0 22px;
    }

    .score-card {
      border:1px solid var(--line);
      background:#fffaf3;
      padding:14px;
      min-height:132px;
    }

    .score-top {
      display:flex;
      justify-content:space-between;
      align-items:baseline;
      gap:10px;
      margin-bottom:10px;
    }

    .score-top span {
      font-size:12px;
      color:var(--muted);
      font-weight:900;
      text-transform:uppercase;
      letter-spacing:.06em;
    }

    .score-top strong {
      font-family:Georgia, serif;
      font-size:30px;
    }

    .score-track,
    .mini-bar-track {
      height:8px;
      background:#eadcc6;
      overflow:hidden;
    }

    .score-fill,
    .mini-bar-fill {
      height:100%;
      background:linear-gradient(90deg, var(--gold), var(--ink));
    }

    .score-card p {
      margin:10px 0 0;
      color:var(--muted);
      font-size:12.5px;
      line-height:1.45;
      text-align:left;
    }

    .visual-grid {
      display:grid;
      grid-template-columns:1fr 1fr;
      gap:14px;
    }

    .chart-card {
      border:1px solid var(--line);
      padding:18px;
      background:#fff;
      break-inside:avoid;
    }

    .chart-card h3 {
      margin:0 0 14px;
      font-size:18px;
      font-family:Georgia, serif;
    }

    .mini-bar-row {
      display:grid;
      grid-template-columns:190px 1fr 42px;
      gap:10px;
      align-items:center;
      padding:10px 0;
      border-top:1px solid #eadcc6;
    }

    .mini-bar-row:first-of-type {
      border-top:0;
    }

    .mini-bar-label strong {
      display:block;
      font-size:13px;
      color:var(--ink);
    }

    .mini-bar-label span {
      display:block;
      font-size:11.5px;
      color:var(--muted);
      line-height:1.35;
    }

    .mini-bar-row b {
      font-family:Georgia, serif;
      font-size:18px;
      text-align:right;
    }

    .report {
      margin-top:26px;
      background:#fff;
      border:1px solid var(--line);
      padding:34px;
    }

    .report h1 {
      font-family:Georgia, serif;
      font-size:34px;
      margin:0 0 14px;
      padding-bottom:16px;
      border-bottom:2px solid var(--line);
      text-align:left;
    }

    .report h2 {
      font-family:Georgia, serif;
      font-size:25px;
      margin:36px 0 14px;
      color:var(--ink);
      text-align:left;
      break-after:avoid;
    }

    .report h3 {
      font-size:18px;
      margin:24px 0 10px;
      color:var(--gold);
      text-align:left;
      break-after:avoid;
    }

    .report p,
    .report li {
      font-size:15px;
      line-height:1.72;
      text-align:justify;
      text-align-last:left;
      text-justify:inter-word;
      hyphens:auto;
      overflow-wrap:break-word;
    }

    .report ul,
    .report ol {
      padding-left:24px;
    }

    .report li {
      margin:8px 0;
      padding-right:2px;
    }

    .report li::marker {
      color:var(--gold);
    }

    .report-table {
      width:100%;
      border-collapse:collapse;
      margin:18px 0 30px;
      font-size:13px;
      table-layout:fixed;
      page-break-inside:auto;
    }

    .report-table td {
      border:1px solid #e1cfb3;
      padding:10px;
      vertical-align:top;
      line-height:1.48;
      word-break:normal;
      overflow-wrap:break-word;
      text-align:left;
    }

    .report-table tr:first-child td {
      background:var(--ink);
      color:#fff;
      font-weight:800;
      text-align:left;
    }

    .report-table tr {
      break-inside:avoid;
      page-break-inside:avoid;
    }

    .report a {
      color:var(--gold);
      font-weight:800;
      word-break:break-word;
    }

    .marketing-close {
      margin-top:26px;
      background:
        linear-gradient(135deg, rgba(36,20,15,.96), rgba(92,61,46,.94)),
        radial-gradient(circle at top right, rgba(169,121,50,.55), transparent 40%);
      color:#fff;
      padding:34px;
      border:1px solid var(--ink);
      break-inside:avoid;
    }

    .marketing-close p {
      color:#f3eadc;
      font-size:15px;
    }

    .close-grid {
      display:grid;
      grid-template-columns:repeat(3, 1fr);
      gap:12px;
      margin:22px 0;
    }

    .close-grid div {
      border:1px solid rgba(255,255,255,.24);
      padding:16px;
      background:rgba(255,255,255,.05);
    }

    .close-grid b {
      display:block;
      color:#d2b88f;
      font-size:12px;
      text-transform:uppercase;
      letter-spacing:.08em;
      margin-bottom:8px;
    }

    .close-grid span {
      display:block;
      line-height:1.45;
      font-weight:800;
    }

    .close-note {
      border-top:1px solid rgba(255,255,255,.24);
      padding-top:18px;
    }

    .footer {
      color:var(--muted);
      font-size:12px;
      margin-top:18px;
      text-align:justify;
      text-align-last:left;
    }

    @media (max-width:900px) {
      .cover h1 { font-size:40px; }
      .meta, .score-grid { grid-template-columns:1fr 1fr; }
      .visual-grid, .dash-head, .close-grid { grid-template-columns:1fr; }
      .mini-bar-row { grid-template-columns:1fr; }
      .mini-bar-row b { text-align:left; }
      .report { padding:22px; }
    }

    @page { size:A4; margin:12mm; }

    @media print {
      * {
        -webkit-print-color-adjust:exact !important;
        print-color-adjust:exact !important;
      }

      body {
        background:#fff;
      }

      .page {
        max-width:none;
        padding:0;
      }

      .topbar,
      .actions {
        display:none !important;
      }

      .cover,
      .premium-dashboard,
      .report,
      .marketing-close {
        box-shadow:none;
      }

      .cover {
        padding:10mm;
        break-after:page;
      }

      .premium-dashboard,
      .report,
      .marketing-close {
        padding:8mm;
      }

      .score-grid {
        grid-template-columns:repeat(5, 1fr);
      }

      .visual-grid {
        grid-template-columns:1fr 1fr;
      }

      .report-table {
        font-size:11.6px;
      }

      .report p,
      .report li {
        font-size:13.2px;
        line-height:1.62;
      }

      a {
        color:var(--ink);
        text-decoration:none;
      }
    }
  </style>
</head>
<body>
  <main class="page">
    <div class="topbar">
      <div class="brand">GovRevenue Agent</div>
      <div class="actions">
        <a class="btn" href="/api/scans/${scan.id}/report.pdf">Download PDF</a>
        <button class="btn secondary" onclick="window.print()">Browser Print</button>
        <a class="btn secondary" href="/api/scans/${scan.id}/report.md">Download Markdown</a>
        <a class="btn secondary" href="/api/scans/${scan.id}/data.json">View Data</a>
        <a class="btn secondary" href="/scan/${scan.id}/compare" title="Compare with a previous scan">Compare &uarr;</a>
      </div>
    </div>

    <section class="cover">
      <h1>Executive Decision Panel</h1>
      <p class="subtitle">Commercial public-sector revenue intelligence for <strong>${escapeHtml(scan.company_name)}</strong>. Built from intake data, Contracts Finder records, verified web research and analyst scoring.</p>

      <div class="meta">
        <div class="metric"><b>Verdict</b><span style="font-size:20px">${escapeHtml(edpVerdict || "Pending")}</span><small>Commercial recommendation</small></div>
        <div class="metric"><b>Evidence Grade</b><span>${escapeHtml(edpGrade || "Pending")}</span><small>Source-backed evidence basis</small></div>
        <div class="metric"><b>Can they win now?</b><span style="font-size:20px">${escapeHtml(edpCanWin || "Pending")}</span><small>Based on verified evidence</small></div>
        <div class="metric"><b>Recommended route</b><span style="font-size:20px">${escapeHtml(edpRoute)}</span><small>Best first money route</small></div>
      </div>

      <div class="data-strip">
        <p><strong>Best first money route:</strong> ${escapeHtml(edpBestRoute)}</p>
        ${edpFastestAction ? `<p><strong>Fastest action this week:</strong> ${escapeHtml(edpFastestAction)}</p>` : ""}
        ${edpMainBlocker ? `<p><strong>Main blocker:</strong> ${escapeHtml(edpMainBlocker)}</p>` : ""}
        <p><strong>Sector lens:</strong> ${escapeHtml(scores.sector)}</p>
        <p><strong>Regions searched:</strong> ${escapeHtml(regions)}</p>
        <p><strong>Generated:</strong> ${escapeHtml(formatDate(scan.updated_at))}</p>
        <p><strong>Evidence note:</strong> ${escapeHtml(data?.quality?.warning || "Human verification required before bid decisions.")}</p>
      </div>
    </section>

    ${premiumDashboardHtml(scan)}

    <section class="report">
      ${content}
    </section>

    ${scan.report_markdown ? premiumClosingHtml(scan, parsedEdp) : ""}

    ${data ? renderIncumbentSection(data) : ""}

    <p class="footer">No outcome is guaranteed. This scan is commercial intelligence, not legal, procurement or financial advice. Human verification is required before bid decisions.</p>

    ${scan.status === "completed" ? `
    <div class="no-print" style="margin:40px auto;max-width:680px;padding:24px 28px;background:#fffaf3;border:1px solid #d2b88f;border-radius:8px">
      <h3 style="margin-top:0;font-family:Georgia,serif;color:#24140f">Get weekly opportunity alerts</h3>
      <p style="color:#5a3e28;margin-bottom:16px">We'll re-scan Contracts Finder every 7 days and email you when new tenders match your profile.</p>
      <form id="alert-form" style="display:flex;gap:10px;flex-wrap:wrap">
        <input type="email" id="alert-email" placeholder="your@email.com" required
          style="flex:1;min-width:220px;padding:10px 14px;border:1px solid #c9a87c;border-radius:6px;font-size:15px;background:#fff" />
        <button type="submit"
          style="padding:10px 20px;background:#1a4a2e;color:#fff;border:0;border-radius:6px;font-size:15px;cursor:pointer;white-space:nowrap">
          Subscribe
        </button>
      </form>
      <p id="alert-msg" style="margin-top:12px;font-size:14px;color:#1a4a2e;display:none"></p>
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
            msg.style.color = r.ok ? "#1a4a2e" : "#9b2d20";
            if (r.ok) document.getElementById("alert-form").style.display = "none";
          } catch {
            msg.style.display = "block";
            msg.textContent = "Subscription failed. Please try again.";
            msg.style.color = "#9b2d20";
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
      `INSERT INTO briefing_subscribers (id, email, category, created_at)
       VALUES ($1, $2, NULL, NOW()) ON CONFLICT (email) DO NOTHING`,
      [makeId(), email]
    );
    res.json({ ok: true, alreadySubscribed: (r.rowCount ?? 0) === 0 });
    return;
  }
  const alreadySubscribed = briefMemStore.has(email);
  if (!alreadySubscribed) briefMemStore.set(email, { id: makeId(), email, category: null, created_at: nowIso() });
  res.json({ ok: true, alreadySubscribed });
}));

app.get("/", asyncRoute(async (_req, res) => {
  const [signals, count24h, samplePdfUrl, deskSignals, chartResult] = await Promise.all([
    queryLatestSignals(12).catch(() => [] as HomepageSignal[]),
    count24hSignals().catch(() => 0),
    findSamplePdf().catch(() => null as string | null),
    queryDeskSignals(DESK_PROFILES.filter(d => d.live).map(d => d.slug)).catch(() => new Map<string, HomepageSignal>()),
    queryChartData().catch(() => ({ points: [] as ChartDataPoint[], illustrative: true }))
  ]);

  const heroSignal = signals[0] || null;
  const isLive = heroSignal !== null;

  // Ticker HTML — doubled for seamless CSS scroll, server-rendered
  const tickerSrc = signals.length >= 3 ? signals : null;
  const buildTickerItems = (arr: HomepageSignal[]) =>
    arr.map(s =>
      `<span><b>${escapeHtml(s.source)}</b> ${escapeHtml(s.title.slice(0, 70))}${s.buyer ? ` &middot; ${escapeHtml(s.buyer.slice(0, 40))}` : ""}</span>`
    ).join("");
  const tickerHtml = tickerSrc
    ? buildTickerItems(tickerSrc) + buildTickerItems(tickerSrc)
    : "<span><b>FTS</b> Illustrative signal &middot; data loads on first refresh</span>".repeat(6);

  // Hero card values
  const heroCategory = isLive
    ? (CATEGORY_LABELS[heroSignal!.category] ||
       heroSignal!.category.replace(/-/g, " ").replace(/\b\w/g, c => c.toUpperCase()))
    : "Housing maintenance";
  const heroTitle = isLive ? heroSignal!.title.slice(0, 80) : "Responsive maintenance framework — West Midlands";
  const heroBuyer = isLive ? (heroSignal!.buyer || "Buyer not stated") : "Local Authority Buyer";
  const heroSource = isLive ? heroSignal!.source : "CF";
  const heroDateRaw = isLive ? (heroSignal!.notice_date || null) : null;
  const heroDate = heroDateRaw
    ? new Date(heroDateRaw).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" })
    : "date not stated";
  const heroStatus = isLive ? (heroSignal!.status || "unknown") : "illustrative";
  const heroVal = isLive && heroSignal!.value_amount && heroSignal!.value_amount > 0
    ? (heroSignal!.value_amount >= 1_000_000
        ? `&pound;${(heroSignal!.value_amount / 1_000_000).toFixed(1)}m`
        : `&pound;${Math.round(heroSignal!.value_amount / 1000)}k`)
    : "Value not stated";

  const noticesDisplay = count24h > 0 ? String(count24h) : "—";

  const sampleLink = samplePdfUrl
    ? `<a class="btn-ghost" href="${escapeHtml(samplePdfUrl)}" target="_blank" rel="noreferrer">See a sample report &rarr;</a>`
    : `<span class="btn-ghost" style="opacity:.4;cursor:default" title="Sample report available after first scan">Sample report (soon)</span>`;

  res.type("html").send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>GovRevenue — Public-Sector Revenue Intelligence</title>
<style>
:root{
  --ink:#0B0F14; --paper:#FAF8F3; --paper-2:#F3EFE6;
  --accent:#9B2C2C; --accent-2:#C2553F; --slate:#5A6B7B;
  --line:#1f262e1a; --line-strong:#0F141926;
  --serif:"Spectral","Iowan Old Style",Georgia,"Times New Roman",serif;
  --sans:"Inter","Helvetica Neue",Arial,sans-serif;
  --mono:"IBM Plex Mono","SF Mono",ui-monospace,Menlo,monospace;
}
*{box-sizing:border-box;margin:0;padding:0}
html{scroll-behavior:smooth}
body{background:var(--paper);color:var(--ink);font-family:var(--sans);font-size:17px;line-height:1.55;-webkit-font-smoothing:antialiased;overflow-x:hidden}
a{color:inherit;text-decoration:none}
.wrap{max-width:1180px;margin:0 auto;padding:0 32px}
.eyebrow{font-family:var(--mono);font-size:11px;letter-spacing:.22em;text-transform:uppercase;color:var(--slate)}
.topstrip{background:var(--ink);color:var(--paper);font-family:var(--mono);font-size:11.5px;letter-spacing:.14em;text-transform:uppercase}
.topstrip .wrap{display:flex;justify-content:space-between;align-items:center;height:34px}
.topstrip .live{display:flex;align-items:center;gap:8px}
.dot{width:7px;height:7px;border-radius:50%;background:var(--accent);animation:pulse 2.4s infinite}
@keyframes pulse{0%{box-shadow:0 0 0 0 #9B2C2C66}70%{box-shadow:0 0 0 7px #9B2C2C00}100%{box-shadow:0 0 0 0 #9B2C2C00}}
header.mast{border-bottom:1px solid var(--line-strong)}
.mast .wrap{display:flex;align-items:baseline;justify-content:space-between;padding-top:26px;padding-bottom:18px}
.logo{font-family:var(--serif);font-weight:600;font-size:30px;letter-spacing:-.01em}
.logo b{color:var(--accent)}
nav.primary{display:flex;gap:30px;font-size:13px;letter-spacing:.04em;text-transform:uppercase;font-weight:500}
nav.primary a{color:var(--slate);padding-bottom:3px;border-bottom:1.5px solid transparent;transition:.18s}
nav.primary a:hover{color:var(--ink);border-color:var(--accent)}
.mast-cta{font-family:var(--mono);font-size:12px;letter-spacing:.1em;text-transform:uppercase;border:1px solid var(--ink);padding:9px 16px;transition:.18s}
.mast-cta:hover{background:var(--ink);color:var(--paper)}
.verticals{border-bottom:1px solid var(--line-strong);background:var(--paper-2)}
.verticals .wrap{display:flex;font-family:var(--mono);font-size:12px;letter-spacing:.12em;text-transform:uppercase;overflow-x:auto}
.verticals a{padding:13px 22px 13px 0;color:var(--slate);white-space:nowrap;position:relative;transition:.18s}
.verticals a:not(:last-child){margin-right:22px}
.verticals a:not(:last-child):after{content:"";position:absolute;right:0;top:50%;transform:translateY(-50%);width:1px;height:13px;background:var(--line-strong)}
.verticals a:hover{color:var(--accent)}
.hero{position:relative;background:var(--ink);color:var(--paper);overflow:hidden;border-bottom:1px solid #000}
#globe-canvas{position:absolute;inset:0;width:100%;height:100%;z-index:0;opacity:.92}
.hero-grad{position:absolute;inset:0;z-index:1;pointer-events:none;background:radial-gradient(120% 80% at 78% 42%, transparent 30%, #0B0F14 78%),linear-gradient(90deg,#0B0F14 18%, transparent 60%)}
.hero .wrap{position:relative;z-index:2;display:grid;grid-template-columns:1.05fr .95fr;gap:40px;align-items:center;min-height:560px;padding:64px 32px}
.hero h1{font-family:var(--serif);font-weight:600;font-size:60px;line-height:1.03;letter-spacing:-.02em;margin:18px 0 22px}
.hero h1 em{font-style:italic;color:var(--accent-2)}
.hero .lede{font-size:18.5px;line-height:1.5;color:#c3ccd2;max-width:30em;margin-bottom:30px}
.hero-actions{display:flex;gap:14px;align-items:center;flex-wrap:wrap}
.btn-primary{background:var(--accent);color:#fff;font-family:var(--mono);font-size:13px;letter-spacing:.08em;text-transform:uppercase;padding:14px 24px;transition:.18s}
.btn-primary:hover{background:#7a2121;transform:translateY(-1px)}
.btn-ghost{font-family:var(--mono);font-size:13px;letter-spacing:.06em;color:#aeb8c0;text-decoration:underline;text-underline-offset:4px;text-decoration-color:#ffffff30}
.btn-ghost:hover{color:#fff}
.chips{display:flex;gap:10px;margin-top:30px;flex-wrap:wrap}
.chip{font-family:var(--mono);font-size:11.5px;letter-spacing:.04em;color:#c3ccd2;border:1px solid #ffffff1f;padding:8px 12px;background:#ffffff08;display:flex;gap:8px;align-items:center}
.chip b{color:#fff;font-weight:600}
.chip .up{color:#7ed99a}
.record{position:relative;border:1px solid #ffffff1f;background:#0f151ccc;backdrop-filter:blur(8px);box-shadow:0 30px 60px -30px #000}
.record .rhead{display:flex;justify-content:space-between;align-items:center;padding:11px 16px;border-bottom:1px solid #ffffff14}
.record .rhead .t{font-family:var(--mono);font-size:11px;letter-spacing:.16em;text-transform:uppercase;color:#fff}
.record .rhead .src{font-family:var(--mono);font-size:10.5px;color:#8a949c}
.record .rbody{padding:6px 18px}
.rrow{display:flex;justify-content:space-between;align-items:flex-start;padding:13px 0;border-bottom:1px dashed #ffffff14}
.rrow:last-child{border-bottom:0}
.rrow .k{font-family:var(--mono);font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#8a949c;padding-top:3px}
.rrow .v{text-align:right;font-family:var(--serif);font-size:20px;color:#fff;max-width:62%}
.rrow .v small{display:block;font-family:var(--mono);font-size:10.5px;color:#8a949c;margin-top:4px}
.figure{font-family:var(--mono);font-size:28px;font-weight:500;color:#fff}
.verdict{display:inline-block;font-family:var(--mono);font-size:12px;letter-spacing:.1em;text-transform:uppercase;background:#9B2C2C26;color:#e08a7a;border:1px solid #9B2C2C66;padding:5px 11px}
.caveat{padding:12px 18px 16px;font-family:var(--mono);font-size:10.5px;color:#8a949c;line-height:1.5;border-top:1px solid #ffffff14}
.caveat b{color:#e08a7a}
.spark{width:100%;height:46px;display:block;margin:2px 0 10px}
.ticker{background:#070a0e;color:var(--paper);overflow:hidden;border-bottom:1px solid #000}
.ticker .row{display:flex;gap:48px;white-space:nowrap;font-family:var(--mono);font-size:12px;letter-spacing:.06em;padding:11px 0;animation:scroll 38s linear infinite;width:max-content}
.ticker .row span b{color:var(--accent-2);font-weight:600;margin-right:8px}
@keyframes scroll{from{transform:translateX(0)}to{transform:translateX(-50%)}}
.chartband{background:var(--paper);border-bottom:1px solid var(--line-strong);padding:74px 0}
.chartband .wrap{display:grid;grid-template-columns:.85fr 1.15fr;gap:56px;align-items:center}
.chartband h2{font-family:var(--serif);font-size:34px;font-weight:600;letter-spacing:-.015em;margin:14px 0 16px;line-height:1.1}
.chartband p{color:#3a444d;font-size:16px;max-width:30em;margin-bottom:18px}
.chartband ul{list-style:none;font-family:var(--mono);font-size:12.5px;letter-spacing:.03em;color:var(--slate)}
.chartband li{padding:8px 0;border-bottom:1px dashed var(--line)}
.chartband li b{color:var(--ink)}
.chartwrap{border:1px solid var(--line-strong);background:#fff;padding:22px 24px 14px;box-shadow:0 20px 40px -30px #0f141950}
.chartwrap .ch-head{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:6px}
.chartwrap .ch-head .lab{font-family:var(--mono);font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:var(--slate)}
.chartwrap .ch-head .big{font-family:var(--mono);font-size:26px;font-weight:600}
.chartwrap .ch-head .big .up{color:#1f9d55;font-size:14px;margin-left:6px}
#growthChart{width:100%;height:230px;display:block}
.section{padding:70px 0;border-bottom:1px solid var(--line-strong)}
.section-head{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:34px}
.section-head h2{font-family:var(--serif);font-size:30px;font-weight:600;letter-spacing:-.01em}
.section-head a{font-family:var(--mono);font-size:12px;letter-spacing:.1em;text-transform:uppercase;color:var(--slate);text-decoration:underline;text-underline-offset:4px}
.section-head a:hover{color:var(--accent)}
.desk-grid{display:grid;grid-template-columns:repeat(3,1fr);border-top:1px solid var(--line-strong)}
.desk{padding:24px 26px 26px 0;border-right:1px solid var(--line);border-bottom:1px solid var(--line);transition:.2s}
.desk:nth-child(3n){border-right:0;padding-right:0}
.desk:hover{background:#fff;box-shadow:0 14px 30px -24px #0f141955}
.desk .tag{display:flex;align-items:center;gap:9px;margin-bottom:12px}
.desk .tag em{font-family:var(--mono);font-style:normal;font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--accent)}
.desk .tag time{font-family:var(--mono);font-size:11px;color:var(--slate)}
.desk h3{font-family:var(--serif);font-size:21px;line-height:1.25;font-weight:600;margin-bottom:9px;transition:.15s}
.desk:hover h3{color:var(--accent)}
.desk p{font-size:14.5px;color:#3a444d;line-height:1.5;margin-bottom:14px}
.desk .src{font-family:var(--mono);font-size:10.5px;color:var(--slate)}
.reveal{opacity:0;transform:translateY(22px);transition:opacity .7s ease,transform .7s ease}
.reveal.in{opacity:1;transform:none}
.product{background:var(--ink);color:var(--paper);border-bottom:1px solid #000}
.product .wrap{display:grid;grid-template-columns:1fr 1fr;gap:60px;padding:78px 32px;align-items:center}
.product .eyebrow{color:#9aa6ae}
.product h2{font-family:var(--serif);font-size:40px;line-height:1.08;font-weight:600;letter-spacing:-.015em;margin:16px 0 20px}
.product h2 em{font-style:italic;color:#d98a8a}
.product p{color:#c3ccd2;max-width:34em;margin-bottom:26px;font-size:16.5px}
.steps{border-top:1px solid #ffffff1f}
.step{display:flex;gap:18px;padding:16px 0;border-bottom:1px solid #ffffff14;align-items:baseline}
.step .n{font-family:var(--mono);font-size:12px;color:var(--accent-2);min-width:28px}
.step .x b{font-weight:600}
.step .x small{display:block;font-family:var(--mono);font-size:11px;color:#9aa6ae;margin-top:3px}
.subscribe{padding:80px 0;text-align:center;background:var(--paper-2)}
.subscribe .eyebrow{margin-bottom:14px}
.subscribe h2{font-family:var(--serif);font-size:38px;font-weight:600;letter-spacing:-.015em;margin-bottom:14px}
.subscribe p{color:#3a444d;max-width:34em;margin:0 auto 28px;font-size:16.5px}
.subform{display:flex;max-width:460px;margin:0 auto;border:1px solid var(--ink)}
.subform input{flex:1;border:0;padding:15px 16px;font-family:var(--mono);font-size:14px;background:#fff}
.subform input:focus{outline:2px solid var(--accent);outline-offset:-2px}
.subform button{background:var(--ink);color:var(--paper);border:0;font-family:var(--mono);font-size:12px;letter-spacing:.1em;text-transform:uppercase;padding:0 22px;cursor:pointer;transition:.18s}
.subform button:hover{background:var(--accent)}
.subnote{font-family:var(--mono);font-size:11px;color:var(--slate);margin-top:14px}
footer{background:var(--ink);color:#aeb8c0;padding:54px 0 40px;font-size:13.5px}
footer .wrap{display:grid;grid-template-columns:2fr 1fr 1fr 1fr;gap:40px}
footer .logo{color:var(--paper);font-size:24px;margin-bottom:12px}
footer .logo b{color:var(--accent)}
footer p.bl{max-width:26em;line-height:1.5;color:#8a949c}
footer h4{font-family:var(--mono);font-size:11px;letter-spacing:.16em;text-transform:uppercase;color:var(--paper);margin-bottom:14px}
footer ul{list-style:none}
footer li{margin-bottom:9px}
footer a:hover{color:var(--paper)}
footer .legal{grid-column:1/-1;border-top:1px solid #ffffff14;margin-top:30px;padding-top:22px;display:flex;justify-content:space-between;font-family:var(--mono);font-size:11px;color:#6b757d;flex-wrap:wrap;gap:10px}
@media(max-width:880px){
  .hero .wrap,.chartband .wrap,.product .wrap{grid-template-columns:1fr;gap:36px}
  .hero h1{font-size:42px}
  #globe-canvas{opacity:.4}
  .hero-grad{background:linear-gradient(180deg,#0B0F14cc,#0B0F14)}
  .desk-grid{grid-template-columns:1fr}
  .desk{border-right:0!important;padding-right:0!important}
  footer .wrap{grid-template-columns:1fr 1fr}
  nav.primary,.mast-cta{display:none}
}
@media(max-width:480px){
  .wrap{padding:0 16px}
  .hero .wrap{min-height:auto;padding:40px 16px}
  .hero h1{font-size:32px}
  .hero .lede{font-size:16px}
  .topstrip .wrap > div:last-child{display:none}
  footer .wrap{grid-template-columns:1fr}
  footer{padding:32px 0 24px}
  footer .legal{flex-direction:column;gap:6px}
}
@media(prefers-reduced-motion:reduce){*{animation:none!important;scroll-behavior:auto}.reveal{opacity:1;transform:none}}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
</style>
</head>
<body>
<div class="topstrip"><div class="wrap">
  <div class="live"><span class="dot"></span> UK public procurement intelligence</div>
  <div>United Kingdom &middot; Confidential intelligence</div>
</div></div>
<header class="mast"><div class="wrap">
  <div class="logo">Gov<b>Revenue</b></div>
  <nav class="primary">
    <a href="#desks">Desks</a><a href="#chart">Signals</a><a href="/scan">The Scan</a><a href="#subscribe">Briefing</a>
  </nav>
  <a class="mast-cta" href="/scan">Run a scan</a>
</div></header>
<div class="verticals"><div class="wrap">
  ${DESK_PROFILES.map(d => `<a href="/desk/${d.slug}">${escapeHtml(d.label)}</a>`).join("")}
</div></div>
<section class="hero">
  <canvas id="globe-canvas"></canvas>
  <div class="hero-grad"></div>
  <div class="wrap">
    <div>
      <div class="eyebrow">Public-sector revenue intelligence</div>
      <h1>Stop bidding blind.<br>Read the <em>record</em> first.</h1>
      <p class="lede">Public bodies already spend on what you sell. We map who buys it, who supplies it now, and the one route where your firm can realistically win &mdash; before the tender goes live.</p>
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
        <div class="rrow"><span class="k">Category</span><span class="v" id="hc-cat">${escapeHtml(heroCategory)}<small id="hc-date">${escapeHtml(heroDate)}</small></span></div>
        <div class="rrow"><span class="k">Notice</span><span class="v" id="hc-title" style="font-size:14px;line-height:1.3">${escapeHtml(heroTitle)}</span></div>
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
      <div class="eyebrow">Category signal &middot; illustrative</div>
      <h2>Watch the money move before the tender does.</h2>
      <p>Recurring spend in a category is the leading indicator. When it climbs, re-lets and frameworks follow. We track the curve so you enter on the upswing, not after the award.</p>
      <ul>
        <li><b>Housing maintenance</b> &middot; illustrative trend +34% / 24mo</li>
        <li><b>Re-let signal</b> &middot; illustrative framework expiry cluster</li>
        <li><b>Entry window</b> &middot; illustrative 18-month renewal window</li>
      </ul>
    </div>
    <div class="chartwrap">
      <div class="ch-head">
        <span class="lab">Recurring category spend &middot; &pound;m${chartResult.illustrative ? ' <span style="font-size:9px;opacity:.5;letter-spacing:.06em">&middot; ILLUSTRATIVE</span>' : ''}</span>
        <span class="big" id="chartTotal">&pound;0.0m<span class="up">&#9650; 34%</span></span>
      </div>
      <canvas id="growthChart"></canvas>
    </div>
  </div>
</section>
<section class="section" id="desks">
  <div class="wrap">
    <div class="section-head"><h2>The desks</h2><a href="/desk/construction">All desks &rarr;</a></div>
    <div class="desk-grid">
      ${DESK_PROFILES.filter(d => d.live).map(d => {
        const sig = deskSignals.get(d.slug);
        return sig
          ? renderDeskCard(sig, d.label)
          : `<article class="desk reveal"><div class="tag"><em>${escapeHtml(d.label)}</em><time>—</time></div><h3>${escapeHtml(d.label)} — scanning</h3><p>Signals load on first hourly refresh.</p><div class="src">Source: Contracts Finder &middot; FTS</div></article>`;
      }).join("")}
    </div>
  </div>
</section>
<section class="product" id="product">
  <div class="wrap">
    <div>
      <div class="eyebrow">The product underneath</div>
      <h2>One profile in.<br>A <em>sourced verdict</em> out.</h2>
      <p>Submit your firm&rsquo;s services, region and contract range. The agent scans the public record, scores route-to-revenue fit, and returns a premium report &mdash; every claim timestamped, sourced, and caveated.</p>
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
    document.getElementById('briefing-form').addEventListener('submit',function(e){
      e.preventDefault();
      const email=document.getElementById('briefing-email').value;
      const note=document.getElementById('briefing-note');
      fetch('/api/briefing',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({email})})
        .then(function(r){return r.json();})
        .then(function(d){
          document.getElementById('briefing-form').style.display='none';
          note.textContent=d.alreadySubscribed?'You’re already on the list.':'Done. We’ll write when the money moves.';
          note.style.color='var(--ink)';note.style.fontWeight='600';
        })
        .catch(function(){note.textContent='Something went wrong — try again.';note.style.color='var(--accent)';});
    });
    </script>
  </div>
</section>
<footer><div class="wrap">
  <div><div class="logo">Gov<b>Revenue</b></div><p class="bl">A public-sector revenue intelligence service. We turn fragmented public spend, contract and supplier data into one commercial decision: bid, partner, monitor, prepare, or ignore.</p></div>
  <div><h4>Desks</h4><ul>${DESK_PROFILES.slice(0, 6).map(d => `<li><a href="/desk/${d.slug}">${escapeHtml(d.label)}</a></li>`).join("")}</ul></div>
  <div><h4>Product</h4><ul><li><a href="/scan">The Scan</a></li><li><a href="/scan" title="Buyer Watchlist is included in your scan report">Watchlist</a></li><li><a href="/scan" title="Available after your first scan">Consultant license</a></li></ul></div>
  <div><h4>Sources</h4><ul><li><a href="https://www.gov.uk/contracts-finder" target="_blank" rel="noopener noreferrer">Contracts Finder</a></li><li><a href="https://www.find-tender.service.gov.uk" target="_blank" rel="noopener noreferrer">Find a Tender</a></li><li><a href="https://www.gov.uk/government/publications/local-government-transparency-code-2015" target="_blank" rel="noopener noreferrer">LA transparency</a></li><li><a href="https://find-and-update.company-information.service.gov.uk" target="_blank" rel="noopener noreferrer">Companies House</a></li></ul></div>
  <div class="legal"><span>&copy; 2026 GovRevenue &middot; United Kingdom &middot; Confidential</span><span>Intelligence, not certainty. Public data shows payments, not wrongdoing.</span></div>
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
    atm.addColorStop(0,'rgba(155,44,44,0.30)');
    atm.addColorStop(0.6,'rgba(50,90,130,0.10)');
    atm.addColorStop(1,'rgba(11,15,20,0)');
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
      if(d.r>2){ctx.beginPath();ctx.arc(p.x,p.y,d.r*3,0,6.3);ctx.fillStyle='rgba(220,95,65,'+(alpha*0.25).toFixed(2)+')';ctx.fill();}
      ctx.beginPath(); ctx.arc(p.x,p.y,d.r,0,6.3);
      ctx.fillStyle='rgba(230,100,70,'+alpha.toFixed(2)+')'; ctx.fill();
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
  const data=[1.9,2.1,2.0,2.4,2.7,2.6,3.0,3.3,3.5,3.8,4.0,4.2];
  function fit(){const dpr=Math.min(devicePixelRatio,2);const r=cv.getBoundingClientRect();cv.width=r.width*dpr;cv.height=r.height*dpr;ctx.setTransform(dpr,0,0,dpr,0,0);return r;}
  let r=fit(); window.addEventListener('resize',()=>{r=fit();});
  const pad={l:34,r:8,t:14,b:22},max=4.6,min=1.6;
  function X(i){return pad.l+(i/(data.length-1))*(r.width-pad.l-pad.r);}
  function Y(v){return pad.t+(1-(v-min)/(max-min))*(r.height-pad.t-pad.b);}
  let prog=0,started=false;
  function draw(){
    ctx.clearRect(0,0,r.width,r.height);
    ctx.strokeStyle='#0f14140f';ctx.lineWidth=1;ctx.font='10px monospace';ctx.fillStyle='#5A6B7B';
    for(let g=2;g<=4;g++){const y=Y(g);ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(r.width-pad.r,y);ctx.stroke();ctx.fillText(''+g+'m',6,y+3);}
    const upto=prog*(data.length-1);
    ctx.beginPath();ctx.moveTo(X(0),Y(data[0]));
    for(let i=1;i<=Math.floor(upto);i++) ctx.lineTo(X(i),Y(data[i]));
    const fi=Math.floor(upto),fr=upto-fi;
    if(fi<data.length-1){const cy=data[fi]+(data[fi+1]-data[fi])*fr;ctx.lineTo(X(fi+fr),Y(cy));}
    const lastX=fi<data.length-1?X(fi+fr):X(data.length-1);
    ctx.lineTo(lastX,r.height-pad.b);ctx.lineTo(X(0),r.height-pad.b);ctx.closePath();
    const grad=ctx.createLinearGradient(0,pad.t,0,r.height-pad.b);
    grad.addColorStop(0,'#9B2C2C2e');grad.addColorStop(1,'#9B2C2C00');ctx.fillStyle=grad;ctx.fill();
    ctx.beginPath();ctx.moveTo(X(0),Y(data[0]));
    for(let i=1;i<=Math.floor(upto);i++) ctx.lineTo(X(i),Y(data[i]));
    if(fi<data.length-1){const cy=data[fi]+(data[fi+1]-data[fi])*fr;ctx.lineTo(X(fi+fr),Y(cy));}
    ctx.strokeStyle='#9B2C2C';ctx.lineWidth=2.4;ctx.lineJoin='round';ctx.stroke();
    const hy=fi<data.length-1?(data[fi]+(data[fi+1]-data[fi])*fr):data[data.length-1];
    ctx.beginPath();ctx.arc(lastX,Y(hy),4.5,0,7);ctx.fillStyle='#9B2C2C';ctx.fill();
    ctx.beginPath();ctx.arc(lastX,Y(hy),9,0,7);ctx.fillStyle='#9B2C2C22';ctx.fill();
  }
  function animate(){if(prog<1){prog+=reduce?1:0.018;if(prog>1)prog=1;draw();requestAnimationFrame(animate);}else draw();}
  const io=new IntersectionObserver(es=>es.forEach(e=>{if(e.isIntersecting&&!started){started=true;animate();
    const el=document.getElementById('chartTotal');let v=0;
    const ci=setInterval(()=>{v+=0.12;if(v>=4.2){v=4.2;clearInterval(ci);}el.firstChild.textContent=v.toFixed(1)+'m';},22);
  }}),{threshold:.4});
  io.observe(cv);
})();
(function(){
  const s=document.getElementById('spark'); if(!s) return;
  const d=[6,9,7,12,11,16,14,20,19,26,24,32,30,40];
  const max=42,W=320,H=46; let pts='';
  d.forEach((v,i)=>{pts+=(i?' ':'')+( i/(d.length-1)*W).toFixed(1)+','+(H-(v/max)*H).toFixed(1);});
  s.innerHTML='<polyline points="'+pts+'" fill="none" stroke="#C2553F" stroke-width="1.6" vector-effect="non-scaling-stroke" stroke-linecap="round"/>';
})();
/* hero card value is server-rendered; no count-up needed */
/* ticker + count are server-rendered; no client fill needed */
(function(){
  const io=new IntersectionObserver(es=>es.forEach(e=>{if(e.isIntersecting){e.target.classList.add('in');io.unobserve(e.target);}}),{threshold:.15});
  document.querySelectorAll('.reveal').forEach(el=>io.observe(el));
})();
(function(){
  if(reduce) return;
  function esc(s){const d=document.createElement('div');d.textContent=s;return d.innerHTML;}
  function g(id){return document.getElementById(id);}
  function poll(){
    fetch('/api/signals/latest').then(function(r){return r.ok?r.json():null;}).then(function(d){
      if(!d||!d.hero) return;
      const h=d.hero;
      if(g('hc-type')) g('hc-type').textContent=h.type;
      if(g('hc-src')) g('hc-src').textContent=h.src+' · public record';
      if(g('hc-cat')&&g('hc-cat').childNodes[0]) g('hc-cat').childNodes[0].nodeValue=h.category;
      if(g('hc-date')) g('hc-date').textContent=h.date;
      if(g('hc-title')) g('hc-title').textContent=h.title;
      if(g('hc-buyer')) g('hc-buyer').textContent=h.buyer;
      if(g('hc-val')&&g('hc-val').childNodes[0]) g('hc-val').childNodes[0].nodeValue=h.val;
      if(g('hc-status')) g('hc-status').textContent=h.status;
      if(g('hc-caveat')) g('hc-caveat').innerHTML='<b>Caveat.</b> '+esc(h.caveat);
      if(d.count24h&&g('liveNotices')) g('liveNotices').textContent=String(d.count24h);
    }).catch(function(){});
  }
  const t=setInterval(poll,75000);
  window.addEventListener('pagehide',function(){clearInterval(t);});
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
  const [signals, count24h] = await Promise.all([
    queryLatestSignals(12).catch(() => [] as HomepageSignal[]),
    count24hSignals().catch(() => 0)
  ]);
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
    date: hero.notice_date
      ? new Date(hero.notice_date).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" })
      : "date not stated",
    title: hero.title.slice(0, 80),
    buyer: hero.buyer || "Buyer not stated",
    val: hero.value_amount && hero.value_amount > 0
      ? (hero.value_amount >= 1_000_000
          ? `£${(hero.value_amount / 1_000_000).toFixed(1)}m`
          : `£${Math.round(hero.value_amount / 1000)}k`)
      : "Value not stated",
    status: hero.status || "unknown",
    caveat: "Source: public procurement record. Confidence varies by notice quality — buyer names taken verbatim, not verified."
  } : null;
  res.json({ count24h, hero: heroOut, ticker });
}));

app.post("/form-submit", asyncRoute(async (req, res) => {
  const parsed = intakeSchema.safeParse(req.body);

  if (!parsed.success) {
    res.status(400).send(`<pre>${JSON.stringify(parsed.error.flatten(), null, 2)}</pre>`);
    return;
  }

  const scan = await createScan(parsed.data);
  await enqueueScan(scan.id, parsed.data);

  res.type("html").send(`
    <body style="font-family:Arial;background:#f3eadc;color:#24140f;padding:32px">
      <div style="max-width:760px;margin:auto;background:#fffaf3;border:1px solid #d2b88f;padding:28px">
        <h1 style="font-family:Georgia,serif">Scan started</h1>
        <p>ID: <code>${scan.id}</code></p>
        <p><a href="/scan/${scan.id}">Open scan status page</a></p>
        <p>Refresh the page in 1-3 minutes.</p>
      </div>
    </body>
  `);
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

app.post("/admin/scans/:id/delete", requireAdmin, asyncRoute(async (req, res) => {
  await deleteScan(req.params.id);
  const token = String(req.query.token || "");
  res.redirect(`/admin/scans?token=${encodeURIComponent(token)}`);
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
  res.json(scan.procurement_json);
}));

app.get("/scan", (_req, res) => {
  res.type("html").send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>Run a Scan &mdash; GovRevenue</title>
<style>
:root{--ink:#0B0F14;--paper:#FAF8F3;--paper-2:#F3EFE6;--accent:#9B2C2C;--slate:#5A6B7B;--line:#1f262e1a;--line-strong:#0F141926;--serif:"Spectral","Iowan Old Style",Georgia,"Times New Roman",serif;--sans:"Inter","Helvetica Neue",Arial,sans-serif;--mono:"IBM Plex Mono","SF Mono",ui-monospace,Menlo,monospace;}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--paper);color:var(--ink);font-family:var(--sans);font-size:16px;line-height:1.55;-webkit-font-smoothing:antialiased}
a{color:inherit;text-decoration:none}
.topstrip{background:var(--ink);color:var(--paper);font-family:var(--mono);font-size:11px;letter-spacing:.14em;text-transform:uppercase;padding:0 32px;display:flex;justify-content:space-between;align-items:center;height:34px}
.topstrip a{color:var(--paper);opacity:.7}
.topstrip a:hover{opacity:1}
header{border-bottom:1px solid var(--line-strong);padding:0 32px}
.mast{display:flex;align-items:baseline;justify-content:space-between;padding:22px 0 16px}
.logo{font-family:var(--serif);font-weight:600;font-size:26px;letter-spacing:-.01em}
.logo b{color:var(--accent)}
.back{font-family:var(--mono);font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:var(--slate);text-decoration:underline;text-underline-offset:4px}
.back:hover{color:var(--ink)}
.page{max-width:780px;margin:0 auto;padding:56px 32px 80px}
.page-head{margin-bottom:40px;border-bottom:1px solid var(--line-strong);padding-bottom:28px}
.eyebrow{font-family:var(--mono);font-size:11px;letter-spacing:.22em;text-transform:uppercase;color:var(--slate);margin-bottom:12px}
h1{font-family:var(--serif);font-size:38px;font-weight:600;letter-spacing:-.02em;line-height:1.1;margin-bottom:12px}
.sub{color:#3a444d;font-size:16px;line-height:1.5}
.form-grid{display:grid;gap:0}
.field{padding:18px 0;border-bottom:1px solid var(--line-strong)}
.field:last-of-type{border-bottom:0}
.field label{display:block;font-family:var(--mono);font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--slate);margin-bottom:8px}
.field label span{color:var(--accent)}
.field input,.field textarea,.field select{width:100%;border:1px solid var(--line-strong);background:#fff;padding:12px 14px;font-family:var(--sans);font-size:15px;color:var(--ink);transition:.15s;resize:vertical}
.field input:focus,.field textarea:focus,.field select:focus{outline:2px solid var(--accent);outline-offset:-1px;border-color:transparent}
.field textarea{min-height:76px}
.field .hint{font-family:var(--mono);font-size:10.5px;color:var(--slate);margin-top:6px;line-height:1.5}
.section-label{font-family:var(--mono);font-size:10px;letter-spacing:.2em;text-transform:uppercase;color:var(--accent);background:var(--paper-2);padding:10px 0;margin:28px 0 0;border-top:1px solid var(--line-strong);border-bottom:1px solid var(--line-strong)}
.submit-row{margin-top:36px;display:flex;align-items:center;gap:20px;flex-wrap:wrap}
.btn-submit{background:var(--accent);color:#fff;font-family:var(--mono);font-size:13px;letter-spacing:.08em;text-transform:uppercase;padding:15px 28px;border:0;cursor:pointer;transition:.18s}
.btn-submit:hover{background:#7a2121}
.submit-note{font-family:var(--mono);font-size:11px;color:var(--slate);line-height:1.5}
</style>
</head>
<body>
<div class="topstrip">
  <a href="/">&#8592; GovRevenue</a>
  <span>Public record &middot; updated continuously</span>
</div>
<header><div class="mast">
  <a class="logo" href="/">Gov<b>Revenue</b></a>
  <a class="back" href="/">&#8592; Back to home</a>
</div></header>
<main class="page">
  <div class="page-head">
    <div class="eyebrow">Revenue scan</div>
    <h1>Tell us about your firm.</h1>
    <p class="sub">The more context you give, the sharper the signal. We scan Contracts Finder, Find a Tender and LA spend data, then return a sourced verdict on where the money is and how to reach it.</p>
  </div>
  <form method="POST" action="/form-submit" autocomplete="off" class="form-grid">

    <div class="section-label">Your firm</div>

    <div class="field">
      <label>Company name <span>*</span></label>
      <input name="companyName" required placeholder="e.g. Apex Facilities Ltd">
    </div>
    <div class="field">
      <label>Website</label>
      <input name="website" placeholder="e.g. https://apexfacilities.co.uk">
    </div>
    <div class="field">
      <label>Location / base</label>
      <input name="location" placeholder="e.g. Birmingham, West Midlands">
    </div>
    <div class="field">
      <label>Team size</label>
      <input name="teamSize" placeholder="e.g. 12 FTE">
    </div>

    <div class="section-label">Services &amp; scope</div>

    <div class="field">
      <label>Main services <span>*</span></label>
      <textarea name="mainServices" required placeholder="e.g. facilities management, reactive maintenance, cleaning"></textarea>
      <div class="hint">Be specific &mdash; these become the search terms we use against the public record.</div>
    </div>
    <div class="field">
      <label>Secondary services</label>
      <textarea name="secondaryServices" placeholder="e.g. grounds maintenance, pest control"></textarea>
    </div>
    <div class="field">
      <label>Areas / regions served</label>
      <textarea name="areasServed" placeholder="e.g. West Midlands, East Midlands, national frameworks"></textarea>
    </div>
    <div class="field">
      <label>Services you do NOT want</label>
      <textarea name="excludedServices" placeholder="e.g. residential, defence, high-security sites"></textarea>
    </div>

    <div class="section-label">Contract appetite</div>

    <div class="field">
      <label>Ideal contract size</label>
      <input name="idealContractSize" placeholder="e.g. £100k &ndash; £500k per year">
    </div>
    <div class="field">
      <label>Maximum contract size</label>
      <input name="maximumContractSize" placeholder="e.g. £2m">
    </div>
    <div class="field">
      <label>Ideal public-sector buyers</label>
      <textarea name="idealBuyers" placeholder="e.g. NHS trusts, local authorities, housing associations"></textarea>
    </div>
    <div class="field">
      <label>Regions to scan first</label>
      <textarea name="regionsToScan" placeholder="e.g. West Midlands priority, then national frameworks"></textarea>
    </div>

    <div class="section-label">Track record &amp; credentials</div>

    <div class="field">
      <label>Public-sector experience</label>
      <input name="publicSectorExperience" placeholder="e.g. 3 years, 6 active public contracts">
    </div>
    <div class="field">
      <label>Last public contract won</label>
      <textarea name="lastPublicContract" placeholder="e.g. 2yr cleaning contract, Birmingham City Council, £180k/yr, ended 2024"></textarea>
      <div class="hint">Most recent win &mdash; buyer name, value, and date if known. Helps us assess your evidence grade.</div>
    </div>
    <div class="field">
      <label>Case studies or proof</label>
      <textarea name="caseStudies" placeholder="e.g. Delivered responsive repairs for housing association 2022&ndash;24, 94% satisfaction score"></textarea>
    </div>
    <div class="field">
      <label>Certifications / accreditations</label>
      <textarea name="certifications" placeholder="e.g. ISO 9001, Constructionline Gold, Living Wage employer"></textarea>
    </div>
    <div class="field">
      <label>Framework access</label>
      <textarea name="frameworkStatus" placeholder="e.g. On Crown Commercial Service RM6187, YPO cleaning framework — or none yet"></textarea>
      <div class="hint">Framework memberships unlock fast-track contract routes. List any you hold or are applying for.</div>
    </div>

    <div class="section-label">Goals &amp; context</div>

    <div class="field">
      <label>Main business goal</label>
      <textarea name="mainGoal" placeholder="e.g. Win first NHS contract within 12 months"></textarea>
    </div>
    <div class="field">
      <label>Preferred output</label>
      <textarea name="preferredOutput" placeholder="e.g. Focus on frameworks we can get on now, not long tender processes"></textarea>
      <div class="hint">Tell us what kind of results matter most &mdash; this shapes the report focus.</div>
    </div>
    <div class="field">
      <label>Biggest concern</label>
      <textarea name="biggestConcern" placeholder="e.g. We keep losing to incumbents on price"></textarea>
    </div>

    <div class="submit-row">
      <button type="submit" class="btn-submit">Run GovRevenue Scan &rarr;</button>
      <span class="submit-note">Takes 2&ndash;4 minutes &middot; sourced PDF report returned</span>
    </div>
  </form>
</main>
<script>
window.addEventListener("pageshow", () => {
  document.querySelectorAll("input, textarea").forEach(f => { f.value = ""; f.setAttribute("autocomplete","off"); });
});
</script>
</body>
</html>`);
});

app.get("/scan/:id", asyncRoute(async (req, res) => {
  const scan = await getScan(req.params.id);

  if (!scan) {
    res.status(404).send("Scan not found");
    return;
  }

  res.type("html").send(reportPage(scan));
}));

app.get("/desk/:slug", asyncRoute(async (req, res) => {
  const profile = DESK_PROFILES.find(d => d.slug === req.params.slug);
  if (!profile) { res.status(404).send("Desk not found"); return; }

  const cached = await getDeskCache(profile.slug).catch(() => null);
  const isStale = !cached || (Date.now() - new Date(cached.cached_at).getTime() > DESK_CACHE_TTL_MS);

  if (isStale) {
    // fire-and-forget; if cold the page renders the compiling state
    compileDeskInBackground(profile).catch(err => captureError(err, { desk: { slug: profile.slug } }));
  }

  res.type("html").send(deskPage(profile, cached));
}));

app.get("/desk/:slug/sub/:sub", asyncRoute(async (req, res) => {
  const profile = DESK_PROFILES.find(d => d.slug === req.params.slug);
  if (!profile) { res.status(404).send("Desk not found"); return; }

  let matchCat: DeskCategory | null = null;
  let matchSub: string | null = null;
  for (const cat of profile.categories) {
    for (const s of cat.subcategories) {
      if (slugify(s) === req.params.sub) { matchCat = cat; matchSub = s; break; }
    }
    if (matchCat) break;
  }
  if (!matchCat || !matchSub) { res.status(404).send("Subcategory not found"); return; }

  const cached = await getDeskCache(profile.slug).catch(() => null);
  const isStale = !cached || (Date.now() - new Date(cached.cached_at).getTime() > DESK_CACHE_TTL_MS);
  if (isStale) {
    compileDeskInBackground(profile).catch(err => captureError(err, { desk: { slug: profile.slug } }));
  }

  res.type("html").send(subPage(profile, matchCat, matchSub, cached));
}));

app.get("/desk/:slug/notices", asyncRoute(async (req, res) => {
  const profile = DESK_PROFILES.find(d => d.slug === req.params.slug);
  if (!profile) { res.status(404).send("Desk not found"); return; }
  const cached = await getDeskCache(profile.slug).catch(() => null);
  const isStale = !cached || (Date.now() - new Date(cached.cached_at).getTime() > DESK_CACHE_TTL_MS);
  if (isStale) compileDeskInBackground(profile).catch(err => captureError(err, { desk: { slug: profile.slug } }));
  const buyerFilter = typeof req.query.buyer === "string" ? req.query.buyer : null;
  res.type("html").send(noticesPage(profile, cached, buyerFilter));
}));

app.get("/desk/:slug/buyers", asyncRoute(async (req, res) => {
  const profile = DESK_PROFILES.find(d => d.slug === req.params.slug);
  if (!profile) { res.status(404).send("Desk not found"); return; }
  const cached = await getDeskCache(profile.slug).catch(() => null);
  const isStale = !cached || (Date.now() - new Date(cached.cached_at).getTime() > DESK_CACHE_TTL_MS);
  if (isStale) compileDeskInBackground(profile).catch(err => captureError(err, { desk: { slug: profile.slug } }));
  res.type("html").send(buyersPage(profile, cached));
}));

app.get("/scan/:id/compare", asyncRoute(async (req, res) => {
  const scan = await getScan(req.params.id);
  if (!scan || scan.status !== "completed") {
    res.status(404).send("Scan not found or not completed");
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

const AGGREGATOR_FRAGMENTS = [
  // BIP / procurement portals
  "bip solutions",
  // Palladium
  "palladium group", "palladium international", "palladium international limited",
  // CHIC — Communities and Housing Investment Consortium
  "chic consortium", "housing investment consortium",
  // YPO
  " ypo ", "yorkshire purchasing organisation",
  // EN Procure / North East councils
  "en procure", "enact procurement",
  "the association of north east councils", "north east procurement",
  // ESPO — Eastern Shires
  "eastern shires purchasing", " espo ",
  // Procurement Hub / Pagabo
  "procurement hub", "pagabo",
  // Scape
  "scape group", "scape procure", "scape framework",
  // Crescent
  "crescent purchasing", "crescent group",
  // Laser (Kent)
  "laser (kent)", "laser purchasing",
  // Pro-quote / Proactis
  "pro-quote", "proactis",
  // NHS Supply Chain (legal name: Supply Chain Coordination Limited)
  "nhs supply chain", "supply chain coordination",
  // NHS SBS
  "nhs shared business services", "n h s shared business services",
  // Crown Commercial Service
  "crown commercial service",
  // National Procurement Service
  "national procurement service",
  // Fusion21
  "fusion21",
  // LHC (London Housing Consortium)
  "london housing consortium", " lhc group",
  // NEPO (North East Procurement Organisation)
  "north east procurement organisation", " nepo ",
  // Advantage South West
  "advantage south west",
  // Procurement for Housing
  "procurement for housing",
  // Pro5 / Pro4 frameworks
  " pro5 ", " pro4 ",
  // Westworks / Pretium
  "westworks procurement", "pretium frameworks",
  // NEUPC / JISC / HEPCW (higher ed consortia)
  "neupc", "jisc procurement", "hepcw",
  // OJEU aggregators
  "ojeu ltd"
];

function isAggregatorBuyer(buyer: string): boolean {
  const b = ` ${buyer.toLowerCase()} `;
  return AGGREGATOR_FRAGMENTS.some(f => b.includes(f));
}

function fmtMoney(v: number): string {
  if (v >= 1_000_000_000) return `£${(v / 1_000_000_000).toFixed(2)}bn`;
  if (v >= 1_000_000) return `£${(v / 1_000_000).toFixed(1)}m`;
  if (v >= 1_000) return `£${Math.round(v / 1_000)}k`;
  return `£${Math.round(v)}`;
}

function slugify(text: string): string {
  return text.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
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

function deskPage(profile: DeskProfile, cached: { data: ProcurementData; cached_at: string } | null): string {
  const isCompiling = cached === null;
  const data = cached?.data;

  const allOpen = (data?.contractsFinder.open || []).concat(data?.findTender?.notices || [])
    .sort((a, b) => {
      const da = new Date(a.publishedDate || a.awardedDate || 0).getTime();
      const db = new Date(b.publishedDate || b.awardedDate || 0).getTime();
      return db - da;
    });
  const deskKeywords = profile.categories.flatMap(c => c.keywords);
  const cutoff365 = Date.now() - 365 * 24 * 3_600_000;
  const openNotices = allOpen.filter(n => {
    const t = new Date(n.publishedDate || n.awardedDate || 0).getTime();
    if (t <= cutoff365) return false;
    const text = (n.title + " " + (n.description || "")).toLowerCase();
    return deskKeywords.some(kw => text.includes(kw));
  }).slice(0, 6);
  const awardedNotices = data?.contractsFinder.awarded || [];

  const totalAwarded = awardedNotices.reduce((s, n) => s + (n.awardedValue ?? 0), 0);
  const awardedCount = awardedNotices.length;
  const uniqueBuyerCount = new Set(awardedNotices.map(n => n.buyer).filter(b => b && b !== "Not stated")).size;

  const demandCategories = profile.live && !isCompiling
    ? inferDeskCategories(awardedNotices, profile.categories)
    : [];

  // Buyer map: aggregate awarded value + open notice count per buyer
  const buyerMap = new Map<string, { awardedValue: number; awardedCount: number; openCount: number }>();
  for (const n of awardedNotices) {
    if (!n.buyer || n.buyer === "Not stated") continue;
    const e = buyerMap.get(n.buyer) || { awardedValue: 0, awardedCount: 0, openCount: 0 };
    e.awardedCount++;
    e.awardedValue += n.awardedValue ?? 0;
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

  const navLinks = DESK_PROFILES.map(d =>
    `<a href="/desk/${d.slug}"${d.slug === profile.slug ? ' class="dnav-active"' : ''}>${escapeHtml(d.label)}</a>`
  ).join("");

  const fmtBig = (v: number) => v >= 1_000_000_000
    ? `£${(v / 1_000_000_000).toFixed(2)}bn`
    : `£${(v / 1_000_000).toFixed(2)}m`;

  const topCats = profile.live && !isCompiling
    ? [...demandCategories].sort((a, b) => b.value - a.value).slice(0, 5).filter(c => c.value > 0)
    : [];
  const maxCatVal = topCats[0]?.value || 1;

  // Demand signal panel
  const demandHtml = profile.live && !isCompiling
    ? `<div class="dp-head-row">
        <span class="dp-eyebrow">AWARDED DEMAND SIGNAL</span>
        <span class="dp-info" title="Based on awarded notices in the public record">ⓘ</span>
       </div>
       <p class="dp-caveat-sm">Based on awarded notices found in the public record<br>for this desk profile (last 12 months).</p>
       <div class="dp-stats">
         <div class="dp-stat">
           <span class="dp-val">${escapeHtml(fmtBig(totalAwarded))}</span>
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
       <p style="color:var(--slate);margin-top:16px;font-size:14px;line-height:1.7">${isCompiling ? "Demand data compiles on first request.<br>Refresh after ~90 seconds." : "This desk is coming soon."}</p>
       ${isCompiling ? `<span class="chip chip-amber" style="margin-top:16px;display:inline-block">Compiling &mdash; data within the hour</span>` : ""}`;

  // Live signal panel
  const liveHtml = `<div class="dp-head-row" style="margin-bottom:14px">
    <div style="display:flex;align-items:center;gap:8px">
      <span class="live-dot"></span>
      <span class="dp-eyebrow">LIVE SIGNAL &ndash; CF + FIND A TENDER</span>
    </div>
    <a href="/desk/${profile.slug}/notices" class="dp-link-sm">View all notices &rarr;</a>
  </div>
  ${!profile.live || isCompiling
    ? `<p class="dp-caveat-sm">Opportunity feed compiles on first request.<br>Refresh after ~90 seconds.</p>`
    : openNotices.length
      ? `<table class="ls-table">
          <thead><tr>
            <th>NOTICE</th><th class="ls-buyer">BUYER</th><th class="ls-val ls-th-r">VALUE</th><th class="ls-date ls-th-r">PUBLISHED</th>
          </tr></thead>
          <tbody>${openNotices.map(n => {
            const rawVal = n.valueHigh ?? n.valueLow ?? n.awardedValue;
            const val = rawVal != null && rawVal > 0 ? fmtMoney(rawVal) : "Not public";
            const ago = timeAgo(n.publishedDate || n.awardedDate);
            return `<tr>
              <td class="ls-title-cell"><a href="${escapeHtml(n.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(n.title.slice(0, 90))}</a></td>
              <td class="ls-buyer">${escapeHtml((n.buyer || "—").slice(0, 38))}</td>
              <td class="ls-val">${escapeHtml(val)}</td>
              <td class="ls-date">${escapeHtml(ago)}</td>
            </tr>`;
          }).join("")}</tbody>
        </table>`
      : `<p class="dp-caveat-sm">No open notices at last refresh. Check back after next compile.</p>`
  }
  <p class="ls-foot">Sourced from Contracts Finder and Find a Tender &nbsp;&middot;&nbsp; Updates every 15 minutes</p>`;

  // Buyer watchlist panel
  const watchlistHtml = `<div class="dp-head-row">
    <div>
      <span class="dp-eyebrow">BUYER WATCHLIST</span>
    </div>
    <span class="dp-info" title="Top buyers by estimated 12-month spend on this desk">ⓘ</span>
  </div>
  <a href="/desk/${profile.slug}/buyers" class="dp-link-sm" style="display:block;margin-bottom:16px">View full watchlist &rarr;</a>
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
        return `<div class="bw-row">
          <div class="bw-avatar">${escapeHtml(initials)}</div>
          <div class="bw-info">
            <div class="bw-name">${escapeHtml(buyer.slice(0, 55))}</div>
            ${orgType ? `<span class="bw-tag ${tagClass}">${escapeHtml(orgType)}</span>` : ""}
            <div class="bw-meta">
              <span class="bw-spend">${escapeHtml(spend)}</span>
              <span class="bw-meta-label"> Est. spend (12m)</span>
            </div>
            <div class="bw-meta"><span class="bw-meta-label">Active notices: ${activeCount}</span></div>
          </div>
        </div>`;
      }).join("")
  }
  <p class="dp-caveat-foot">Watchlist rotates daily. Figures are indicative.</p>`;

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
:root{
  --ink:#0B0F14;--paper:#FAF8F3;--paper-2:#F3EFE6;
  --accent:#9B2C2C;--slate:#5A6B7B;
  --line:#1f262e1a;--line-strong:#0F141926;
  --serif:"Spectral","Iowan Old Style",Georgia,serif;
  --sans:"Inter","Helvetica Neue",Arial,sans-serif;
  --mono:"IBM Plex Mono","SF Mono",ui-monospace,Menlo,monospace;
}
*{box-sizing:border-box;margin:0;padding:0}
html{scroll-behavior:smooth}
body{background:var(--paper);color:var(--ink);font-family:var(--sans);font-size:16px;line-height:1.55;-webkit-font-smoothing:antialiased}
a{color:inherit;text-decoration:none}
.chip{font-family:var(--mono);font-size:12px;letter-spacing:.06em;padding:6px 12px;border:1px solid var(--line-strong);background:var(--paper-2)}
.chip-green{border-color:#1d6b4f44;color:#1d6b4f}
.chip-amber{border-color:#a9793244;color:#a97932}
/* Global header */
.gh{background:var(--ink);color:var(--paper)}
.gh-inner{max-width:1440px;margin:0 auto;padding:0 56px}
.gh-top{display:flex;align-items:center;justify-content:space-between;height:52px;gap:24px}
.gh-brand{display:flex;align-items:center;gap:10px;flex-shrink:0}
.gh-logo{font-family:var(--serif);font-weight:600;font-size:21px;letter-spacing:-.01em;color:var(--paper)}
.gh-logo b{color:#d97070}
.gh-tag{font-family:var(--mono);font-size:10.5px;letter-spacing:.07em;text-transform:uppercase;color:#7a909e;border-left:1px solid #ffffff1a;padding-left:10px}
.gh-nav{display:flex;overflow-x:auto;scrollbar-width:none;border-top:1px solid rgba(255,255,255,.07)}
.gh-nav::-webkit-scrollbar{display:none}
.gh-nav a{font-family:var(--mono);font-size:11px;letter-spacing:.09em;text-transform:uppercase;color:#9aabb7;padding:0 16px;height:40px;display:flex;align-items:center;border-bottom:2px solid transparent;white-space:nowrap;transition:.15s}
.gh-nav a:first-child{padding-left:0}
.gh-nav a:hover{color:var(--paper)}
.gh-nav a.dnav-active{color:var(--paper);border-bottom-color:var(--paper)}
.gh-badge{text-align:right;flex-shrink:0;line-height:1.4}
.gh-badge span{font-family:var(--mono);font-size:10.5px;color:#7a909e;display:block}
/* Masthead */
.dm-mast{padding:80px 0 72px;border-bottom:1px solid var(--line-strong)}
.dm-mast-inner{max-width:1440px;margin:0 auto;padding:0 56px;display:grid;grid-template-columns:1fr 360px;gap:72px;align-items:start}
.dm-mast-eyebrow{font-family:var(--mono);font-size:11px;letter-spacing:.18em;text-transform:uppercase;color:var(--accent);margin-bottom:14px}
.dm-mast h1{font-family:var(--serif);font-size:64px;line-height:1.0;letter-spacing:-.01em;text-transform:uppercase;margin-bottom:20px}
.dm-mast-lede{font-size:17px;color:var(--slate);line-height:1.7;margin-bottom:20px}
.dm-source-badge{font-size:13px;color:var(--slate);display:flex;align-items:center;gap:7px}
.dm-mast-cta{border:1px solid var(--line-strong);padding:32px 32px 32px}
.dm-mast-cta p{font-family:var(--serif);font-size:19px;line-height:1.5;margin-bottom:24px}
.btn-cta{display:flex;align-items:center;justify-content:center;gap:8px;background:var(--ink);color:var(--paper);font-family:var(--mono);font-size:12px;letter-spacing:.12em;text-transform:uppercase;padding:15px 20px;transition:.18s}
.btn-cta:hover{background:var(--accent)}
/* Three panels */
.dp-panels{border-bottom:1px solid var(--line-strong)}
.dp-panels-inner{max-width:1440px;margin:0 auto;padding:0 56px;display:grid;grid-template-columns:5fr 4fr 3fr;border-left:1px solid var(--line-strong)}
.dp-panel{padding:64px 60px;border-right:1px solid var(--line-strong)}
.dp-head-row{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}
.dp-eyebrow{font-family:var(--mono);font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--slate)}
.dp-info{font-size:13px;color:var(--slate);cursor:help;margin-left:4px}
.dp-caveat-sm{font-size:13.5px;color:var(--slate);line-height:1.7;margin-bottom:20px}
.dp-caveat-foot{font-family:var(--mono);font-size:11px;color:var(--slate);margin-top:28px;padding-top:18px;border-top:1px solid var(--line)}
.dp-link-sm{font-family:var(--mono);font-size:11px;letter-spacing:.04em;color:var(--accent);text-decoration:underline;text-decoration-color:var(--accent)44}
.dp-link-sm:hover{text-decoration-color:var(--accent)}
.dp-bars-head{font-family:var(--mono);font-size:11px;letter-spacing:.09em;text-transform:uppercase;color:var(--slate);margin:32px 0 20px}
.dp-bars-sub{text-transform:none;letter-spacing:0;font-size:11px}
.dp-map-link{display:inline-block;font-family:var(--mono);font-size:11px;letter-spacing:.04em;color:var(--accent);margin-top:18px;text-decoration:underline;text-decoration-color:var(--accent)44}
.dp-map-link:hover{text-decoration-color:var(--accent)}
/* Demand stats */
.dp-stats{display:grid;grid-template-columns:repeat(3,1fr);border:1px solid var(--line-strong);margin:22px 0 8px}
.dp-stat{padding:28px 24px}
.dp-stat:not(:last-child){border-right:1px solid var(--line-strong)}
.dp-val{display:block;font-family:var(--serif);font-size:40px;font-weight:600;letter-spacing:-.02em;line-height:1.05}
.dp-stat-label{display:block;font-family:var(--mono);font-size:10px;letter-spacing:.07em;text-transform:uppercase;color:var(--slate);margin-top:8px}
/* Bar rows */
.dp-bar-row{display:grid;grid-template-columns:1fr 80px 68px;gap:12px;align-items:center;margin-bottom:15px}
.dp-bar-label{font-size:13px;color:var(--ink)}
.dp-bar-track{height:4px;background:var(--line-strong);border-radius:2px}
.dp-bar-fill{height:4px;background:var(--accent);border-radius:2px}
.dp-bar-val{font-family:var(--mono);font-size:12px;color:var(--slate);text-align:right}
/* Live signal */
.live-dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:#1d6b4f;flex-shrink:0;margin-right:2px}
.ls-table{width:100%;border-collapse:collapse;font-size:13.5px;margin-top:10px}
.ls-table th{font-family:var(--mono);font-size:10px;letter-spacing:.09em;text-transform:uppercase;color:var(--slate);text-align:left;padding:0 8px 12px 0;border-bottom:1px solid var(--line-strong)}
.ls-table td{padding:14px 8px 14px 0;border-bottom:1px solid var(--line);vertical-align:top}
.ls-title-cell{overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}
.ls-table a{color:var(--accent);text-decoration:underline;text-decoration-color:var(--accent)44}
.ls-table a:hover{text-decoration-color:var(--accent)}
.ls-buyer{color:var(--slate);font-size:12.5px}
.ls-val{font-family:var(--mono);font-size:12.5px;white-space:nowrap;text-align:right}
.ls-date{font-family:var(--mono);font-size:12.5px;color:var(--slate);white-space:nowrap;text-align:right}
.ls-th-r{text-align:right}
.ls-foot{font-family:var(--mono);font-size:11px;color:var(--slate);margin-top:18px}
/* Buyer watchlist */
.bw-row{display:flex;gap:14px;padding:18px 0;border-bottom:1px solid var(--line)}
.bw-row:last-of-type{border-bottom:none}
.bw-avatar{width:42px;height:42px;border-radius:4px;background:var(--ink);color:var(--paper);font-family:var(--mono);font-size:10px;font-weight:500;display:flex;align-items:center;justify-content:center;letter-spacing:.04em;flex-shrink:0;margin-top:1px}
.bw-info{flex:1;min-width:0}
.bw-name{font-size:13.5px;font-weight:500;line-height:1.35;margin-bottom:5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.bw-tag{font-family:var(--mono);font-size:9.5px;letter-spacing:.05em;padding:2px 7px;border-radius:2px;display:inline-block;margin-bottom:6px}
.bw-tag-health{background:#e8f5f0;color:#1d6b4f;border:1px solid #1d6b4f33}
.bw-tag-la{background:#eef2f7;color:#2563ab;border:1px solid #2563ab33}
.bw-tag-gov{background:#f3efe8;color:#6b4f1d;border:1px solid #6b4f1d33}
.bw-tag-housing{background:#f0eef7;color:#5b21b6;border:1px solid #5b21b633}
.bw-tag-edu{background:#fef3e2;color:#b45309;border:1px solid #b4530933}
.bw-tag-other{background:var(--paper-2);color:var(--slate);border:1px solid var(--line-strong)}
.bw-meta{font-family:var(--mono);font-size:11px;color:var(--slate);line-height:1.65}
.bw-spend{font-size:15px;color:var(--ink);font-family:var(--serif);font-weight:600;margin-right:2px}
.bw-meta-label{font-size:11px;color:var(--slate)}
.bw-sample{font-weight:400;opacity:.55;letter-spacing:.03em}
/* Demand map */
.dm-section{padding:64px 0;border-bottom:1px solid var(--line-strong)}
.dm-section-inner{max-width:1440px;margin:0 auto;padding:0 56px}
.dm-head-row{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}
.dm-title{font-family:var(--mono);font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--ink)}
.dm-title-info{font-size:13px;color:var(--slate);cursor:help;margin-left:5px}
.dm-sub{font-size:13px;color:var(--slate);margin-bottom:28px}
.dm-open-all{font-family:var(--mono);font-size:10.5px;letter-spacing:.04em;color:var(--slate);border:1px solid var(--line-strong);padding:6px 14px;cursor:pointer;background:var(--paper);transition:.15s}
.dm-open-all:hover{border-color:var(--ink);color:var(--ink)}
.dm-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(230px,1fr));gap:18px}
.dm-card{border:1px solid var(--line-strong);padding:28px 26px 24px;background:var(--paper-2);transition:border-color .15s,background .15s;cursor:default}
.dm-card:hover{border-color:#0B0F1440;background:#F0ECE2}
.dm-card-head{display:flex;align-items:flex-start;gap:12px;margin-bottom:14px}
.dm-icon-wrap{flex-shrink:0;width:36px;height:36px;background:var(--paper);border:1px solid var(--line-strong);display:flex;align-items:center;justify-content:center;padding:7px;color:var(--slate)}
.dm-icon{width:100%;height:100%}
.dm-card-title{flex:1;min-width:0}
.dm-name{display:block;font-size:13.5px;font-weight:600;line-height:1.35;color:var(--ink)}
.dm-count{font-family:var(--mono);font-size:10.5px;color:var(--slate);font-weight:400;margin-top:3px;display:block}
.dm-subs{list-style:none;font-size:12px;color:var(--slate);line-height:1.9;margin-top:4px}
.dm-subs li{padding-left:0}
.dm-subs a{color:var(--slate);text-decoration:none;border-bottom:1px solid var(--line-strong);transition:color .12s,border-color .12s}
.dm-subs a:hover{color:var(--accent);border-bottom-color:var(--accent)}
.dm-more-btn{font-family:var(--mono);font-size:11px;color:var(--accent);background:none;border:none;cursor:pointer;padding:6px 0 0;text-decoration:underline;text-decoration-color:var(--accent)44;display:block}
.dm-more-btn:hover{text-decoration-color:var(--accent)}
/* Sources bar */
.dm-sources-bar{background:var(--paper-2);border-top:1px solid var(--line-strong)}
.dm-sources-inner{max-width:1440px;margin:0 auto;padding:16px 56px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px}
.dm-sources-left{font-size:12.5px;color:var(--slate)}
.dm-sources-right{display:flex;gap:4px;align-items:center;flex-wrap:wrap}
.dm-src-label{font-family:var(--mono);font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:var(--slate);margin-right:6px}
.dm-src-link{font-family:var(--mono);font-size:11px;color:var(--slate);text-decoration:underline;text-decoration-color:var(--line-strong);padding:0 6px}
.dm-src-link:hover{color:var(--ink)}
.dm-foot-copy{text-align:center;font-family:var(--mono);font-size:10.5px;letter-spacing:.06em;color:var(--slate);padding:12px 0 16px;border-top:1px solid var(--line)}
@media(max-width:1100px){
  .dp-panels-inner{grid-template-columns:1fr}
  .dm-mast-inner{grid-template-columns:1fr}
  .dm-mast-cta{display:none}
  .dm-mast h1{font-size:52px}
}
@media(max-width:760px){
  .gh-tag,.gh-badge{display:none}
  .gh-inner,.dm-mast-inner,.dp-panels-inner,.dm-section-inner,.dm-sources-inner{padding-left:16px;padding-right:16px}
  .dm-mast{padding:40px 0 36px}
  .dm-mast-inner{grid-template-columns:1fr;gap:0}
  .dm-mast h1{font-size:34px}
  .dp-panels-inner{grid-template-columns:1fr;border-left:none}
  .dp-panel{padding:28px 0;border-right:none;border-bottom:1px solid var(--line-strong)}
  .dp-stats{grid-template-columns:1fr 1fr}
  .dp-val{font-size:28px}
  .dp-bar-row{grid-template-columns:1fr 60px}
  .dp-bar-track{display:none}
  .dm-grid{grid-template-columns:1fr 1fr}
  .ls-val,.ls-date,.ls-buyer{display:none}
  .dm-sources-inner{flex-direction:column;align-items:flex-start;gap:6px;padding-top:14px;padding-bottom:14px}
  .dm-sources-right{flex-wrap:wrap}
  .ls-table{display:block;overflow-x:auto;-webkit-overflow-scrolling:touch}
}
@media(max-width:480px){
  .dm-grid{grid-template-columns:1fr}
  .dp-stats{grid-template-columns:1fr}
}
</style>
</head>
<body>
<header class="gh">
  <div class="gh-inner">
    <div class="gh-top">
      <div class="gh-brand">
        <a href="/" class="gh-logo">Gov<b>Revenue</b></a>
        <span class="gh-tag">Public-sector revenue intelligence</span>
      </div>
      <div class="gh-badge">
        <span>CF &middot; public record</span>
        <span>Built for public trust</span>
      </div>
    </div>
    <nav class="gh-nav">${navLinks}</nav>
  </div>
</header>

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
      <p>This is the public record for ${escapeHtml(profile.label)}.<br>Run it against your firm.</p>
      <a class="btn-cta" href="/scan">RUN A SCAN &nbsp;&rarr;</a>
    </div>
  </div>
</section>

<div class="dp-panels">
  <div class="dp-panels-inner">
    <div class="dp-panel">${demandHtml}</div>
    <div class="dp-panel">${liveHtml}</div>
    <div class="dp-panel">${watchlistHtml}</div>
  </div>
</div>

<section class="dm-section" id="demand-map">
  <div class="dm-section-inner">
    <div class="dm-head-row">
      <div>
        <span class="dm-title">DEMAND MAP &ndash; ${escapeHtml(profile.label.toUpperCase())}</span>
        <span class="dm-title-info" title="All major categories and sub-categories this desk scans for">ⓘ</span>
      </div>
      <button class="dm-open-all" data-open="0" onclick="var o=this.dataset.open==='1';this.closest('section').querySelectorAll('.dm-sub-x').forEach(e=>e.style.display=o?'none':'list-item');this.closest('section').querySelectorAll('.dm-more-btn').forEach(b=>{b.textContent=o?('+ '+b.dataset.more+' more'):'Show less';b.dataset.open=o?'0':'1'});this.textContent=o?'Open all':'Close all';this.dataset.open=o?'0':'1'">Open all</button>
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
      <a class="dm-src-link" href="https://www.contractsfinder.service.gov.uk" target="_blank" rel="noopener">Contracts Finder &#8599;</a>
      <a class="dm-src-link" href="https://www.find-tender.service.gov.uk" target="_blank" rel="noopener">Find a Tender &#8599;</a>
      <a class="dm-src-link" href="https://www.localspend.co.uk" target="_blank" rel="noopener">Local Authority Transparency &#8599;</a>
      <a class="dm-src-link" href="https://find-and-update.company-information.service.gov.uk" target="_blank" rel="noopener">Companies House &#8599;</a>
    </div>
    <span style="font-family:var(--mono);font-size:10.5px;color:var(--slate)">Caveat: Data is indicative, not exhaustive.</span>
  </div>
</div>
<div class="dm-foot-copy">&copy; GovRevenue</div>

</body>
</html>`;
}

function subPage(
  profile: DeskProfile,
  cat: DeskCategory,
  subLabel: string,
  cached: { data: ProcurementData; cached_at: string } | null
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
    return allKw.some(kw => title.includes(kw));
  };

  const allOpen = (data?.contractsFinder.open || [])
    .concat(data?.findTender?.notices || [])
    .filter(matchTitle)
    .sort((a, b) => new Date(b.publishedDate || b.awardedDate || "").getTime() - new Date(a.publishedDate || a.awardedDate || "").getTime());

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

  const navLinks = DESK_PROFILES.map(d =>
    `<a href="/desk/${d.slug}"${d.slug === profile.slug ? ' class="dnav-active"' : ""}>${escapeHtml(d.label)}</a>`
  ).join("");

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
        <div class="bw-meta"><span class="bw-spend">${escapeHtml(spend)}</span><span class="bw-meta-label"> awarded (12m)</span></div>
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
:root{
  --ink:#0B0F14;--paper:#FAF8F3;--paper-2:#F3EFE6;
  --accent:#9B2C2C;--slate:#5A6B7B;
  --line:#1f262e1a;--line-strong:#0F141926;
  --serif:"Spectral","Iowan Old Style",Georgia,serif;
  --sans:"Inter","Helvetica Neue",Arial,sans-serif;
  --mono:"IBM Plex Mono","SF Mono",ui-monospace,Menlo,monospace;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--paper);color:var(--ink);font-family:var(--sans);font-size:16px;line-height:1.55;-webkit-font-smoothing:antialiased}
a{color:inherit;text-decoration:none}
.gh{background:var(--ink);color:var(--paper)}
.gh-inner{max-width:1440px;margin:0 auto;padding:0 56px}
.gh-top{display:flex;align-items:center;justify-content:space-between;height:52px;gap:24px}
.gh-brand{display:flex;align-items:center;gap:10px;flex-shrink:0}
.gh-logo{font-family:var(--serif);font-weight:600;font-size:21px;letter-spacing:-.01em;color:var(--paper)}
.gh-logo b{color:#d97070}
.gh-tag{font-family:var(--mono);font-size:10.5px;letter-spacing:.07em;text-transform:uppercase;color:#7a909e;border-left:1px solid #ffffff1a;padding-left:10px}
.gh-nav{display:flex;overflow-x:auto;scrollbar-width:none;border-top:1px solid rgba(255,255,255,.07)}
.gh-nav::-webkit-scrollbar{display:none}
.gh-nav a{font-family:var(--mono);font-size:11px;letter-spacing:.09em;text-transform:uppercase;color:#9aabb7;padding:0 16px;height:40px;display:flex;align-items:center;border-bottom:2px solid transparent;white-space:nowrap;transition:.15s}
.gh-nav a:first-child{padding-left:0}
.gh-nav a:hover{color:var(--paper)}
.gh-nav a.dnav-active{color:var(--paper);border-bottom-color:var(--paper)}
.gh-badge{text-align:right;flex-shrink:0;line-height:1.4}
.gh-badge span{font-family:var(--mono);font-size:10.5px;color:#7a909e;display:block}
.sub-mast{padding:52px 0 44px;border-bottom:1px solid var(--line-strong)}
.sub-mast-inner{max-width:1440px;margin:0 auto;padding:0 56px}
.sub-crumb{font-family:var(--mono);font-size:11px;letter-spacing:.06em;color:var(--slate);margin-bottom:18px;display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.sub-crumb a{color:var(--slate);text-decoration:underline;text-decoration-color:var(--line-strong)}
.sub-crumb a:hover{color:var(--accent)}
.sub-crumb-sep{color:var(--line-strong)}
.sub-crumb-active{color:var(--ink)}
.sub-mast h1{font-family:var(--serif);font-size:52px;line-height:1.0;letter-spacing:-.01em;text-transform:uppercase;margin-bottom:16px}
.sub-lede{font-size:16px;color:var(--slate);line-height:1.65;margin-bottom:32px}
.sub-lede strong{color:var(--ink)}
.sub-stats{display:grid;grid-template-columns:repeat(3,1fr);max-width:540px;border:1px solid var(--line-strong)}
.sub-stat{padding:20px 24px}
.sub-stat:not(:last-child){border-right:1px solid var(--line-strong)}
.sub-stat-val{display:block;font-family:var(--serif);font-size:32px;font-weight:600;letter-spacing:-.02em;line-height:1.1}
.sub-stat-label{display:block;font-family:var(--mono);font-size:10px;letter-spacing:.07em;text-transform:uppercase;color:var(--slate);margin-top:6px}
.sub-body{padding:56px 0}
.sub-body-inner{max-width:1440px;margin:0 auto;padding:0 56px}
.sub-two-col{display:grid;grid-template-columns:1fr 320px;gap:48px;margin-bottom:56px}
.sub-sec-head{display:flex;align-items:center;justify-content:space-between;margin-bottom:18px;padding-bottom:14px;border-bottom:1px solid var(--line-strong)}
.sub-sec-eyebrow{font-family:var(--mono);font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--slate);display:flex;align-items:center;gap:6px}
.sub-sec-count{font-family:var(--mono);font-size:11px;color:var(--slate)}
.sub-awarded-sec{margin-bottom:56px}
.sub-empty{font-size:14px;color:var(--slate);padding:28px 0;font-family:var(--mono)}
.sub-cta-row{display:flex;align-items:center;gap:28px;padding:36px;border:1px solid var(--line-strong);background:var(--paper-2)}
.sub-cta-text{font-family:var(--serif);font-size:17px;line-height:1.5;flex:1}
.live-dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:#1d6b4f;flex-shrink:0}
.ls-table{width:100%;border-collapse:collapse;font-size:13.5px;margin-top:4px}
.ls-table th{font-family:var(--mono);font-size:10px;letter-spacing:.09em;text-transform:uppercase;color:var(--slate);text-align:left;padding:0 8px 12px 0;border-bottom:1px solid var(--line-strong)}
.ls-table td{padding:13px 8px 13px 0;border-bottom:1px solid var(--line);vertical-align:top}
.ls-title-cell{overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}
.ls-table a{color:var(--accent);text-decoration:underline;text-decoration-color:var(--accent)44}
.ls-table a:hover{text-decoration-color:var(--accent)}
.ls-buyer{color:var(--slate);font-size:12.5px}
.ls-val{font-family:var(--mono);font-size:12.5px;white-space:nowrap;text-align:right}
.ls-date{font-family:var(--mono);font-size:12.5px;color:var(--slate);white-space:nowrap;text-align:right}
.ls-th-r{text-align:right}
.ls-foot{font-family:var(--mono);font-size:11px;color:var(--slate);margin-top:14px}
.bw-row{display:flex;gap:12px;padding:16px 0;border-bottom:1px solid var(--line)}
.bw-row:last-of-type{border-bottom:none}
.bw-avatar{width:40px;height:40px;border-radius:4px;background:var(--ink);color:var(--paper);font-family:var(--mono);font-size:10px;display:flex;align-items:center;justify-content:center;letter-spacing:.04em;flex-shrink:0;margin-top:1px}
.bw-info{flex:1;min-width:0}
.bw-name{font-size:13px;font-weight:500;line-height:1.35;margin-bottom:5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.bw-tag{font-family:var(--mono);font-size:9.5px;letter-spacing:.05em;padding:2px 7px;border-radius:2px;display:inline-block;margin-bottom:5px}
.bw-tag-health{background:#e8f5f0;color:#1d6b4f;border:1px solid #1d6b4f33}
.bw-tag-la{background:#eef2f7;color:#2563ab;border:1px solid #2563ab33}
.bw-tag-gov{background:#f3efe8;color:#6b4f1d;border:1px solid #6b4f1d33}
.bw-tag-housing{background:#f0eef7;color:#5b21b6;border:1px solid #5b21b633}
.bw-tag-edu{background:#fef3e2;color:#b45309;border:1px solid #b4530933}
.bw-tag-other{background:var(--paper-2);color:var(--slate);border:1px solid var(--line-strong)}
.bw-meta{font-family:var(--mono);font-size:11px;color:var(--slate);line-height:1.65}
.bw-spend{font-size:14px;color:var(--ink);font-family:var(--serif);font-weight:600;margin-right:2px}
.bw-meta-label{font-size:11px;color:var(--slate)}
.btn-cta{display:inline-flex;align-items:center;gap:8px;background:var(--ink);color:var(--paper);font-family:var(--mono);font-size:12px;letter-spacing:.12em;text-transform:uppercase;padding:15px 24px;transition:.18s;flex-shrink:0}
.btn-cta:hover{background:var(--accent)}
.dm-sources-bar{background:var(--paper-2);border-top:1px solid var(--line-strong)}
.dm-sources-inner{max-width:1440px;margin:0 auto;padding:16px 56px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px}
.dm-sources-left{font-size:12.5px;color:var(--slate)}
.dm-sources-right{display:flex;gap:4px;align-items:center;flex-wrap:wrap}
.dm-src-label{font-family:var(--mono);font-size:10px;letter-spacing:.1em;text-transform:uppercase;color:var(--slate);margin-right:6px}
.dm-src-link{font-family:var(--mono);font-size:11px;color:var(--slate);text-decoration:underline;text-decoration-color:var(--line-strong);padding:0 6px}
.dm-src-link:hover{color:var(--ink)}
.dm-foot-copy{text-align:center;font-family:var(--mono);font-size:10.5px;letter-spacing:.06em;color:var(--slate);padding:12px 0 16px;border-top:1px solid var(--line)}
@media(max-width:1100px){.sub-two-col{grid-template-columns:1fr}}
@media(max-width:760px){
  .gh-tag,.gh-badge{display:none}
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

<header class="gh">
  <div class="gh-inner">
    <div class="gh-top">
      <div class="gh-brand">
        <a href="/" class="gh-logo">Gov<b>Revenue</b></a>
        <span class="gh-tag">Public-sector revenue intelligence</span>
      </div>
      <div class="gh-badge">
        <span>CF &middot; public record</span>
        <span>Built for public trust</span>
      </div>
    </div>
    <nav class="gh-nav">${navLinks}</nav>
  </div>
</header>

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
        <span class="sub-stat-val">${isCompiling ? "—" : totalValue > 0 ? fmtMoney(totalValue) : "—"}</span>
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
      <a class="dm-src-link" href="https://www.contractsfinder.service.gov.uk" target="_blank" rel="noopener">Contracts Finder &#8599;</a>
      <a class="dm-src-link" href="https://www.find-tender.service.gov.uk" target="_blank" rel="noopener">Find a Tender &#8599;</a>
    </div>
    <span style="font-family:var(--mono);font-size:10.5px;color:var(--slate)">Caveat: Data is indicative, not exhaustive.</span>
  </div>
</div>
<div class="dm-foot-copy">&copy; GovRevenue</div>

</body>
</html>`;
}

// ─── shared page shell ────────────────────────────────────────────────────────

function pageShellCss(): string {
  return `
:root{--ink:#0B0F14;--paper:#FAF8F3;--paper-2:#F3EFE6;--accent:#9B2C2C;--slate:#5A6B7B;--line:#1f262e1a;--line-strong:#0F141926;--serif:"Spectral","Iowan Old Style",Georgia,serif;--sans:"Inter","Helvetica Neue",Arial,sans-serif;--mono:"IBM Plex Mono","SF Mono",ui-monospace,Menlo,monospace}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--paper);color:var(--ink);font-family:var(--sans);font-size:16px;line-height:1.55;-webkit-font-smoothing:antialiased}
a{color:inherit;text-decoration:none}
.gh{background:var(--ink);color:var(--paper)}
.gh-inner{max-width:1440px;margin:0 auto;padding:0 56px}
.gh-top{display:flex;align-items:center;justify-content:space-between;height:52px;gap:24px}
.gh-brand{display:flex;align-items:center;gap:10px;flex-shrink:0}
.gh-logo{font-family:var(--serif);font-weight:600;font-size:21px;letter-spacing:-.01em;color:var(--paper)}
.gh-logo b{color:#d97070}
.gh-tag{font-family:var(--mono);font-size:10.5px;letter-spacing:.07em;text-transform:uppercase;color:#7a909e;border-left:1px solid #ffffff1a;padding-left:10px}
.gh-nav{display:flex;overflow-x:auto;scrollbar-width:none;border-top:1px solid rgba(255,255,255,.07)}
.gh-nav::-webkit-scrollbar{display:none}
.gh-nav a{font-family:var(--mono);font-size:11px;letter-spacing:.09em;text-transform:uppercase;color:#9aabb7;padding:0 16px;height:40px;display:flex;align-items:center;border-bottom:2px solid transparent;white-space:nowrap;transition:.15s}
.gh-nav a:first-child{padding-left:0}
.gh-nav a:hover{color:var(--paper)}
.gh-nav a.dnav-active{color:var(--paper);border-bottom-color:var(--paper)}
.gh-badge{text-align:right;flex-shrink:0;line-height:1.4}
.gh-badge span{font-family:var(--mono);font-size:10.5px;color:#7a909e;display:block}
.pg-mast{padding:48px 0 40px;border-bottom:1px solid var(--line-strong)}
.pg-mast-inner{max-width:1440px;margin:0 auto;padding:0 56px}
.pg-crumb{font-family:var(--mono);font-size:11px;letter-spacing:.06em;color:var(--slate);margin-bottom:16px;display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.pg-crumb a{color:var(--slate);text-decoration:underline;text-decoration-color:var(--line-strong)}
.pg-crumb a:hover{color:var(--accent)}
.pg-crumb-sep{color:var(--line-strong)}
.pg-crumb-active{color:var(--ink)}
.pg-mast h1{font-family:var(--serif);font-size:44px;line-height:1.05;letter-spacing:-.01em;text-transform:uppercase;margin-bottom:28px}
.pg-stats{display:flex;gap:0;border:1px solid var(--line-strong);width:fit-content;flex-wrap:wrap}
.pg-stat{padding:16px 28px;border-right:1px solid var(--line-strong)}
.pg-stat:last-child{border-right:none}
.pg-stat-val{display:block;font-family:var(--serif);font-size:28px;font-weight:600;letter-spacing:-.02em;line-height:1.1}
.pg-stat-label{display:block;font-family:var(--mono);font-size:10px;letter-spacing:.07em;text-transform:uppercase;color:var(--slate);margin-top:5px}
.pg-body{padding:48px 0 64px}
.pg-body-inner{max-width:1440px;margin:0 auto;padding:0 56px}
.bw-tag{font-family:var(--mono);font-size:9.5px;letter-spacing:.05em;padding:2px 7px;border-radius:2px;display:inline-block}
.bw-tag-health{background:#e8f5f0;color:#1d6b4f;border:1px solid #1d6b4f33}
.bw-tag-la{background:#eef2f7;color:#2563ab;border:1px solid #2563ab33}
.bw-tag-gov{background:#f3efe8;color:#6b4f1d;border:1px solid #6b4f1d33}
.bw-tag-housing{background:#f0eef7;color:#5b21b6;border:1px solid #5b21b633}
.bw-tag-edu{background:#fef3e2;color:#b45309;border:1px solid #b4530933}
.bw-tag-other{background:var(--paper-2);color:var(--slate);border:1px solid var(--line-strong)}
.pg-empty{font-family:var(--mono);font-size:13px;color:var(--slate);padding:40px 0}
.pg-foot{background:var(--paper-2);border-top:1px solid var(--line-strong)}
.pg-foot-inner{max-width:1440px;margin:0 auto;padding:14px 56px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;font-size:12px;color:var(--slate)}
.pg-copy{text-align:center;font-family:var(--mono);font-size:10.5px;letter-spacing:.06em;color:var(--slate);padding:10px 0 14px;border-top:1px solid var(--line)}
@media(max-width:760px){
  .gh-tag,.gh-badge{display:none}
  .gh-inner,.pg-mast-inner,.pg-body-inner,.pg-foot-inner{padding-left:16px;padding-right:16px}
  .pg-mast{padding:28px 0 24px}
  .pg-mast h1{font-size:26px}
  .pg-stats{width:100%;flex-wrap:wrap}
  .pg-stat{flex:1 1 45%;border-bottom:1px solid var(--line-strong)}
}
@media(max-width:480px){
  .pg-stat{flex:1 1 100%}
}`;
}

function pageShellHeader(profile: DeskProfile): string {
  const navLinks = DESK_PROFILES.map(d =>
    `<a href="/desk/${d.slug}"${d.slug === profile.slug ? ' class="dnav-active"' : ""}>${escapeHtml(d.label)}</a>`
  ).join("");
  return `<header class="gh">
  <div class="gh-inner">
    <div class="gh-top">
      <div class="gh-brand">
        <a href="/" class="gh-logo">Gov<b>Revenue</b></a>
        <span class="gh-tag">Public-sector revenue intelligence</span>
      </div>
      <div class="gh-badge"><span>CF &middot; public record</span><span>Built for public trust</span></div>
    </div>
    <nav class="gh-nav">${navLinks}</nav>
  </div>
</header>`;
}

function pageShellFoot(): string {
  return `<footer class="pg-foot">
  <div class="pg-foot-inner">
    <span>Public record only. Always verify on the source.</span>
    <span>Contracts Finder &middot; Find a Tender</span>
    <span>Data is indicative, not exhaustive.</span>
  </div>
</footer>
<div class="pg-copy">&copy; GovRevenue</div>`;
}

// ─── /desk/:slug/notices ──────────────────────────────────────────────────────

function noticesPage(
  profile: DeskProfile,
  cached: { data: ProcurementData; cached_at: string } | null,
  buyerFilter: string | null = null
): string {
  const data = cached?.data;
  const isCompiling = cached === null;

  const allOpen = (data?.contractsFinder.open || [])
    .concat(data?.findTender?.notices || [])
    .sort((a, b) => new Date(b.publishedDate || b.awardedDate || "").getTime() - new Date(a.publishedDate || a.awardedDate || "").getTime());

  const allAwarded = (data?.contractsFinder.awarded || [])
    .sort((a, b) => new Date(b.awardedDate || b.publishedDate || "").getTime() - new Date(a.awardedDate || a.publishedDate || "").getTime());

  const totalValue = allAwarded.reduce((s, n) => s + (n.awardedValue ?? 0), 0);
  const uniqueBuyers = new Set([...allOpen, ...allAwarded].map(n => n.buyer).filter(Boolean)).size;

  const renderRow = (n: ProcurementNotice, status: "open" | "awarded") => {
    const rawVal = status === "open" ? (n.valueHigh ?? n.valueLow ?? n.awardedValue) : n.awardedValue;
    const val = rawVal != null && rawVal > 0 ? fmtMoney(rawVal) : "Not public";
    const date = status === "open" ? (n.publishedDate || n.awardedDate) : (n.awardedDate || n.publishedDate);
    const dateStr = date ? new Date(date).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" }) : "—";
    const src = n.source === "Find a Tender" ? "FTS" : "CF";
    return `<tr data-status="${status}" data-search="${escapeHtml((n.title + " " + (n.buyer || "")).toLowerCase())}">
      <td class="nt-status"><span class="nt-chip nt-chip-${status}">${status === "open" ? "OPEN" : "AWARDED"}</span></td>
      <td class="nt-title"><a href="${escapeHtml(n.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(n.title.slice(0, 100))}</a></td>
      <td class="nt-buyer">${escapeHtml((n.buyer || "—").slice(0, 50))}</td>
      <td class="nt-val">${escapeHtml(val)}</td>
      <td class="nt-date">${escapeHtml(dateStr)}</td>
      <td class="nt-src"><span class="nt-src-badge">${escapeHtml(src)}</span></td>
    </tr>`;
  };

  const openRows = allOpen.map(n => renderRow(n, "open")).join("");
  const awardedRows = allAwarded.map(n => renderRow(n, "awarded")).join("");

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>All Notices &mdash; ${escapeHtml(profile.label)} &mdash; GovRevenue</title>
<style>
${pageShellCss()}
.nt-toolbar{display:flex;align-items:center;gap:12px;margin-bottom:28px;flex-wrap:wrap}
.nt-tabs{display:flex;border:1px solid var(--line-strong);overflow:hidden}
.nt-tab{font-family:var(--mono);font-size:11px;letter-spacing:.09em;text-transform:uppercase;padding:9px 18px;cursor:pointer;background:var(--paper);color:var(--slate);border:none;border-right:1px solid var(--line-strong);transition:.15s}
.nt-tab:last-child{border-right:none}
.nt-tab.active,.nt-tab:hover{background:var(--ink);color:var(--paper)}
.nt-search{flex:1;min-width:200px;max-width:360px;font-family:var(--mono);font-size:12px;padding:9px 14px;border:1px solid var(--line-strong);background:var(--paper);color:var(--ink);outline:none}
.nt-search:focus{border-color:var(--ink)}
.nt-count{font-family:var(--mono);font-size:11px;color:var(--slate);margin-left:auto}
.nt-table{width:100%;border-collapse:collapse;font-size:13.5px}
.nt-table th{font-family:var(--mono);font-size:10px;letter-spacing:.09em;text-transform:uppercase;color:var(--slate);text-align:left;padding:0 10px 12px 0;border-bottom:2px solid var(--line-strong)}
.nt-table td{padding:13px 10px 13px 0;border-bottom:1px solid var(--line);vertical-align:top}
.nt-table tr:hover td{background:#F7F4EE}
.nt-table tr[style*="display:none"]{display:none!important}
.nt-title{max-width:480px}
.nt-title a{color:var(--accent);text-decoration:underline;text-decoration-color:var(--accent)44;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.nt-title a:hover{text-decoration-color:var(--accent)}
.nt-buyer{color:var(--slate);font-size:12.5px;max-width:200px}
.nt-val{font-family:var(--mono);font-size:12.5px;white-space:nowrap;text-align:right}
.nt-date{font-family:var(--mono);font-size:12.5px;color:var(--slate);white-space:nowrap;text-align:right}
.nt-src{text-align:right}
.nt-src-badge{font-family:var(--mono);font-size:9.5px;letter-spacing:.06em;padding:2px 6px;border:1px solid var(--line-strong);color:var(--slate);background:var(--paper-2)}
.nt-status{white-space:nowrap}
.nt-chip{font-family:var(--mono);font-size:9.5px;letter-spacing:.07em;padding:2px 7px;border-radius:2px;font-weight:500}
.nt-chip-open{background:#e8f5f0;color:#1d6b4f;border:1px solid #1d6b4f33}
.nt-chip-awarded{background:var(--paper-2);color:var(--slate);border:1px solid var(--line-strong)}
@media(max-width:760px){
  .nt-buyer,.nt-val,.nt-src{display:none}
  .nt-title{max-width:none}
  .nt-table{display:block;overflow-x:auto;-webkit-overflow-scrolling:touch}
}
</style>
</head>
<body>
${pageShellHeader(profile)}

<section class="pg-mast">
  <div class="pg-mast-inner">
    <div class="pg-crumb">
      <a href="/desk/${profile.slug}">${escapeHtml(profile.label)}</a>
      <span class="pg-crumb-sep">&rsaquo;</span>
      <span class="pg-crumb-active">All Notices</span>
    </div>
    <h1>ALL NOTICES</h1>
    <div class="pg-stats">
      <div class="pg-stat">
        <span class="pg-stat-val">${isCompiling ? "—" : String(allOpen.length)}</span>
        <span class="pg-stat-label">Open opportunities</span>
      </div>
      <div class="pg-stat">
        <span class="pg-stat-val">${isCompiling ? "—" : String(allAwarded.length)}</span>
        <span class="pg-stat-label">Awarded contracts</span>
      </div>
      <div class="pg-stat">
        <span class="pg-stat-val">${isCompiling ? "—" : totalValue > 0 ? fmtMoney(totalValue) : "—"}</span>
        <span class="pg-stat-label">Total awarded value</span>
      </div>
      <div class="pg-stat">
        <span class="pg-stat-val">${isCompiling ? "—" : String(uniqueBuyers)}</span>
        <span class="pg-stat-label">Unique buyers</span>
      </div>
    </div>
  </div>
</section>

<section class="pg-body">
  <div class="pg-body-inner">
    ${isCompiling
      ? `<p class="pg-empty">Compiling &mdash; data ready within 90 seconds. Refresh to check.</p>`
      : `<div class="nt-toolbar">
          <div class="nt-tabs">
            <button class="nt-tab active" data-filter="all">All (${allOpen.length + allAwarded.length})</button>
            <button class="nt-tab" data-filter="open">Open (${allOpen.length})</button>
            <button class="nt-tab" data-filter="awarded">Awarded (${allAwarded.length})</button>
          </div>
          <input class="nt-search" type="search" placeholder="Search notices or buyers…" id="nt-search"${buyerFilter ? ` value="${escapeHtml(buyerFilter)}"` : ""}>
          <span class="nt-count" id="nt-count">${allOpen.length + allAwarded.length} notices</span>
        </div>
        <table class="nt-table" id="nt-table">
          <thead><tr>
            <th></th>
            <th>Notice</th>
            <th class="nt-buyer">Buyer</th>
            <th class="nt-val" style="text-align:right">Value</th>
            <th style="text-align:right">Date</th>
            <th class="nt-src" style="text-align:right">Src</th>
          </tr></thead>
          <tbody id="nt-body">${openRows}${awardedRows}</tbody>
        </table>
        <script>
        (function(){
          var activeFilter='all';
          var searchVal='';
          function update(){
            var rows=document.querySelectorAll('#nt-body tr');
            var vis=0;
            rows.forEach(function(r){
              var statusOk=activeFilter==='all'||r.dataset.status===activeFilter;
              var searchOk=!searchVal||r.dataset.search.includes(searchVal);
              var show=statusOk&&searchOk;
              r.style.display=show?'':'none';
              if(show)vis++;
            });
            document.getElementById('nt-count').textContent=vis+' notices';
          }
          document.querySelectorAll('.nt-tab').forEach(function(btn){
            btn.addEventListener('click',function(){
              document.querySelectorAll('.nt-tab').forEach(function(b){b.classList.remove('active')});
              btn.classList.add('active');
              activeFilter=btn.dataset.filter;
              update();
            });
          });
          var t;
          var inp=document.getElementById('nt-search');
          inp.addEventListener('input',function(e){
            clearTimeout(t);
            t=setTimeout(function(){searchVal=e.target.value.toLowerCase().trim();update();},120);
          });
          if(inp.value){searchVal=inp.value.toLowerCase().trim();update();}
        })();
        </script>`
    }
  </div>
</section>

${pageShellFoot()}
</body>
</html>`;
}

// ─── /desk/:slug/buyers ───────────────────────────────────────────────────────

function buyersPage(
  profile: DeskProfile,
  cached: { data: ProcurementData; cached_at: string } | null
): string {
  const data = cached?.data;
  const isCompiling = cached === null;

  const allOpen  = (data?.contractsFinder.open  || []).concat(data?.findTender?.notices || []);
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
.bi-search{flex:1;min-width:200px;max-width:360px;font-family:var(--mono);font-size:12px;padding:9px 14px;border:1px solid var(--line-strong);background:var(--paper);color:var(--ink);outline:none}
.bi-search:focus{border-color:var(--ink)}
.bi-count{font-family:var(--mono);font-size:11px;color:var(--slate);margin-left:auto}
.bi-card{display:flex;align-items:flex-start;gap:16px;padding:22px 0;border-bottom:1px solid var(--line)}
.bi-card:first-child{border-top:1px solid var(--line-strong)}
.bi-card:last-child{border-bottom:1px solid var(--line-strong)}
.bi-card-left{display:flex;flex-direction:column;align-items:center;gap:6px;flex-shrink:0}
.bi-rank{font-family:var(--mono);font-size:10px;color:var(--slate);letter-spacing:.04em}
.bi-avatar{width:44px;height:44px;border-radius:4px;background:var(--ink);color:var(--paper);font-family:var(--mono);font-size:11px;display:flex;align-items:center;justify-content:center;letter-spacing:.04em}
.bi-card-body{flex:1;min-width:0}
.bi-name{font-size:15px;font-weight:600;line-height:1.3;margin-bottom:8px}
.bi-tags{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px;align-items:center}
.bi-cat-tag{font-family:var(--mono);font-size:9.5px;letter-spacing:.04em;padding:2px 7px;border-radius:2px;background:var(--paper-2);color:var(--slate);border:1px solid var(--line-strong)}
.bi-meta-row{font-family:var(--mono);font-size:11.5px;color:var(--slate);display:flex;flex-wrap:wrap;gap:4px;align-items:center}
.bi-spend{font-family:var(--serif);font-size:16px;font-weight:600;color:var(--ink)}
.bi-spend-label{font-size:11px}
.bi-sep{color:var(--line-strong);margin:0 2px}
.bi-open{color:#1d6b4f;font-weight:600}
.bi-card-right{flex-shrink:0;align-self:center}
.bi-cta{font-family:var(--mono);font-size:11px;letter-spacing:.07em;text-transform:uppercase;color:var(--accent);text-decoration:underline;text-decoration-color:var(--accent)44;white-space:nowrap}
.bi-cta:hover{text-decoration-color:var(--accent)}
@media(max-width:760px){.bi-card-right{display:none}}
</style>
</head>
<body>
${pageShellHeader(profile)}

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
        <span class="pg-stat-val">${isCompiling ? "—" : totalSpend > 0 ? fmtMoney(totalSpend) : "—"}</span>
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
    const map: Record<string, string> = { A: "#1d6b4f", B: "#2a7a3b", C: "#a97932", D: "#c05c20", E: "#9b2d20" };
    return map[g?.charAt(0).toUpperCase()] || "#24140f";
  }
  function row(label: string, cur: string, pri: string, changed: boolean) {
    return `<tr>
      <td style="padding:10px 14px;font-size:13px;color:#6f5b50;border-bottom:1px solid #e8d9c4;font-family:monospace;text-transform:uppercase;letter-spacing:.06em">${escapeHtml(label)}</td>
      <td style="padding:10px 14px;font-size:15px;font-weight:600;border-bottom:1px solid #e8d9c4;color:${escapeHtml(gradeColour(cur))}">${escapeHtml(cur)}</td>
      <td style="padding:10px 14px;font-size:15px;border-bottom:1px solid #e8d9c4;color:#8a6f5a">${escapeHtml(pri)}</td>
      <td style="padding:10px 14px;border-bottom:1px solid #e8d9c4">${changed ? '<span style="background:#fdf0ee;color:#9b2d20;font-size:11px;font-family:monospace;padding:2px 7px">CHANGED</span>' : '<span style="font-size:11px;font-family:monospace;color:#b0a090">same</span>'}</td>
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
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Compare &mdash; ${escapeHtml(current.company_name)} &mdash; GovRevenue</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#f3eadc;color:#24140f;font-family:Arial,sans-serif;padding:40px 24px}
.page{max-width:900px;margin:0 auto}
.brand{font-size:11px;letter-spacing:.2em;text-transform:uppercase;color:#8a6f5a;margin-bottom:8px}
h1{font-family:Georgia,serif;font-size:28px;font-weight:600;margin-bottom:4px}
.sub{font-size:13px;color:#8a6f5a;font-family:monospace;margin-bottom:32px}
.back{font-size:12px;color:#8a6f5a;text-decoration:underline;font-family:monospace;display:inline-block;margin-bottom:24px}
table{width:100%;border-collapse:collapse;background:#fffaf3;border:1px solid #d2b88f}
thead tr{background:#f3eadc}
th{padding:9px 14px;text-align:left;font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#8a6f5a;font-weight:600}
.section-head{font-family:Georgia,serif;font-size:18px;font-weight:600;margin:32px 0 12px;padding-bottom:8px;border-bottom:1px solid #d2b88f}
.buyer-list{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px}
.buyer{font-size:12px;font-family:monospace;padding:4px 10px;border-radius:2px}
.buyer.new{background:#e8f5ee;color:#1d6b4f;border:1px solid #a8d4b8}
.buyer.gone{background:#fdf0ee;color:#9b2d20;border:1px solid #e0a090}
.none{font-size:13px;color:#8a6f5a;font-family:monospace}
${prior ? "" : ".no-prior{background:#fffaf3;border:1px solid #d2b88f;padding:24px 28px;font-size:14px;color:#6f5b50}"}
</style>
</head>
<body>
<div class="page">
  <div class="brand">GovRevenue &mdash; Scan comparison</div>
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
    res.status(404).send("Report not found or not complete yet.");
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
        background: #f3eadc !important;
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
    res.status(404).send("Report not found or not complete yet.");
    return;
  }

  const filename = `${scan.company_name.replace(/[^a-z0-9]+/gi, "_").toLowerCase()}_govrevenue_scan.md`;
  res.setHeader("Content-Type", "text/markdown; charset=utf-8");
  res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
  res.send(scan.report_markdown);
}));

app.get("/admin/scans", requireAdmin, asyncRoute(async (req, res) => {
  const scans = await listScans();
  const token = String(req.query.token || "");

  res.type("html").send(`<!doctype html>
<html>
<body style="font-family:Arial;background:#f3eadc;color:#24140f;padding:32px">
<h1 style="font-family:Georgia,serif">GovRevenue Scans</h1>
<table border="1" cellpadding="10" cellspacing="0" style="background:#fff;width:100%;max-width:1120px">
<tr><th>Created</th><th>Company</th><th>Status</th><th>Open</th><th>Data</th><th>Delete</th></tr>
${scans
  .map(
    s =>
      `<tr>
        <td>${escapeHtml(formatDate(s.created_at))}</td>
        <td>${escapeHtml(s.company_name)}</td>
        <td>${escapeHtml(s.status)}</td>
        <td><a href="/scan/${s.id}">Open</a></td>
        <td><a href="/api/scans/${s.id}/data.json">Data</a></td>
        <td>
          <form method="POST" action="/admin/scans/${s.id}/delete?token=${encodeURIComponent(token)}" onsubmit="return confirm('Delete this scan permanently?');">
            <button style="background:#9b2d20;color:#fff;border:0;padding:7px 10px;cursor:pointer">Delete</button>
          </form>
        </td>
      </tr>`
  )
  .join("")}
</table>
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
    res.status(404).type("html").send(`<body style="font-family:Arial;padding:40px"><p>Subscription not found.</p></body>`);
    return;
  }
  await deactivateSubscription(sub.id);
  res.type("html").send(`<!doctype html>
<html>
<body style="font-family:Arial;background:#f3eadc;color:#24140f;padding:40px">
<div style="max-width:600px;margin:auto;background:#fffaf3;border:1px solid #d2b88f;padding:32px;border-radius:8px">
  <h1 style="font-family:Georgia,serif;margin-top:0">Unsubscribed</h1>
  <p>Weekly alerts for <strong>${escapeHtml(sub.company_name)}</strong> have been cancelled.</p>
  <p><a href="/" style="color:#1a4a2e">Back to GovRevenue</a></p>
</div>
</body></html>`);
}));

app.get("/admin/subscriptions", requireAdmin, asyncRoute(async (req, res) => {
  const subs = await listAllSubscriptions();
  const token = String(req.query.token || "");
  res.type("html").send(`<!doctype html>
<html>
<body style="font-family:Arial;background:#f3eadc;color:#24140f;padding:32px">
<h1 style="font-family:Georgia,serif">Weekly Alert Subscriptions</h1>
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
        <button style="background:#1a4a2e;color:#fff;border:0;padding:6px 10px;cursor:pointer">Fire Now</button>
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
      startSignalsWorker();
    } else {
      console.log("[queue] worker disabled by RUN_WORKER=false");
    }

    if (RUN_WEB) {
      app.listen(PORT, () => {
        console.log(`[server] GovRevenue Agent running on port ${PORT}`);
        // Warm up all live desk caches on startup — compile any that are cold or stale
        for (const desk of DESK_PROFILES.filter(d => d.live)) {
          getDeskCache(desk.slug)
            .then(cached => {
              const isStale = !cached || (Date.now() - new Date(cached.cached_at).getTime() > DESK_CACHE_TTL_MS);
              if (isStale) {
                console.log(`[desk] warm-up: compiling ${desk.slug}`);
                compileDeskInBackground(desk).catch(err => captureError(err, { desk: { slug: desk.slug } }));
              }
            })
            .catch(() => {
              compileDeskInBackground(desk).catch(err => captureError(err, { desk: { slug: desk.slug } }));
            });
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
