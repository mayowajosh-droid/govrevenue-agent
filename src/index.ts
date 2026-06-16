import {
generateGovRevenueReport,
  GovRevenueQualityGateError,
  type CompanyIntake,
  type ProcurementRecord,
} from "./lib/govrevenue/govrevenue-report-engine.js";
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
import { buildScanLinks, isEmailConfigured, notifyScanCompleted, notifyScanFailed } from "./lib/emailNotifications.js";
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
  preferredOutput: z.string().optional().default("")
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




function buildKeywords(input: z.infer<typeof intakeSchema>) {
  const text = [
    input.companyName,
    input.mainServices,
    input.secondaryServices,
    input.idealBuyers,
    input.mainGoal,
    input.preferredOutput
  ]
    .join(" ")
    .toLowerCase();

  const catalog = [
    {
      triggers: ["quantity surveying", "quantity surveyor", "cost management", "cost consultancy", "cost consultant", "qs"],
      keywords: ["quantity surveying", "cost management", "cost consultancy", "construction consultancy"]
    },
    {
      triggers: ["construction project management", "project management", "employer's agent", "employer agent", "contract administration", "project controls", "programme management"],
      keywords: ["construction project management", "project management", "programme management", "employer's agent", "contract administration", "project controls"]
    },
    {
      triggers: ["building surveying", "building survey", "condition survey", "six facet", "estate", "estates", "asset management", "property consultancy", "strategic estate", "built asset"],
      keywords: ["building surveying", "condition survey", "estate consultancy", "asset management", "property consultancy", "built asset consultancy"]
    },
    {
      triggers: ["retrofit", "decarbonisation", "decarbonization", "net zero", "energy efficiency", "sustainability"],
      keywords: ["retrofit consultancy", "decarbonisation", "net zero consultancy", "energy efficiency", "sustainability consultancy"]
    },
    {
      triggers: ["facilities management", "fm", "maintenance", "hard fm", "soft fm"],
      keywords: ["facilities management", "estate maintenance", "hard FM"]
    },
    {
      triggers: ["roofing", "roof", "cladding", "building envelope"],
      keywords: ["roofing", "cladding", "building envelope"]
    },
    {
      triggers: ["solar", "photovoltaic", "pv", "ev charging", "heat pump"],
      keywords: ["solar PV", "EV charging", "renewable energy", "heat pumps"]
    },
    {
      triggers: ["photography", "photographer", "event photography", "portrait", "graduation", "wedding", "property photography"],
      keywords: ["photography", "event photography", "corporate photography", "property photography", "creative services", "media services"]
    },
    {
      triggers: ["marketing", "communications", "content", "campaign", "creative", "video production", "film"],
      keywords: ["marketing services", "communications", "creative services", "content production", "video production"]
    }
  ];

  const selected: string[] = [];

  for (const group of catalog) {
    if (group.triggers.some(trigger => text.includes(trigger))) {
      selected.push(...group.keywords);
    }
  }

  if (!selected.length) {
    const servicePhrases = [input.mainServices, input.secondaryServices]
      .join(",")
      .split(/[,./;|]+/)
      .map(value => value.trim().toLowerCase())
      .filter(value => value.length >= 4 && value.length <= 60)
      .slice(0, 6);

    selected.push(...servicePhrases);
  }

  return Array.from(new Set(selected)).slice(0, 8);
}

function buildRegion(input: z.infer<typeof intakeSchema>) {
  const raw = `${input.regionsToScan} ${input.areasServed} ${input.location}`.toLowerCase();
  const regions: string[] = [];

  if (raw.includes("west midlands") || raw.includes("birmingham")) regions.push("West Midlands");
  if (raw.includes("london")) regions.push("London");
  if (raw.includes("north west") || raw.includes("manchester")) regions.push("North West");
  if (raw.includes("east midlands")) regions.push("East Midlands");
  if (raw.includes("south east")) regions.push("South East");

  return regions.length ? Array.from(new Set(regions)).join(",") : "West Midlands,London";
}

function noticeUrl(id: string) {
  return id ? `https://www.contractsfinder.service.gov.uk/Notice/${encodeURIComponent(id)}` : "https://www.contractsfinder.service.gov.uk/";
}

function normaliseNotice(raw: any, keyword: string): ProcurementNotice | null {
  const item = raw?.item || raw;
  if (!item) return null;

  const id = String(item.id || item.noticeIdentifier || "");
  const title = String(item.title || "").trim();
  if (!title) return null;

  return {
    source: "Contracts Finder",
    id,
    title,
    buyer: String(item.organisationName || "Not stated"),
    description: String(item.description || "").slice(0, 900),
    status: String(item.noticeStatus || ""),
    type: String(item.noticeType || ""),
    region: String(item.regionText || item.region || ""),
    publishedDate: item.publishedDate || null,
    deadlineDate: item.deadlineDate || null,
    awardedDate: item.awardedDate || null,
    valueLow: typeof item.valueLow === "number" ? item.valueLow : null,
    valueHigh: typeof item.valueHigh === "number" ? item.valueHigh : null,
    awardedValue: typeof item.awardedValue === "number" ? item.awardedValue : null,
    awardedSupplier: String(item.awardedSupplier || ""),
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

  const buyerName = String(release?.buyer?.name || release?.parties?.find?.((party: any) => Array.isArray(party.roles) && party.roles.includes("buyer"))?.name || "Not stated");
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

    for (const release of releases) {
      const haystack = [
        release?.tender?.title,
        release?.tender?.description,
        release?.buyer?.name,
        release?.parties?.map?.((party: any) => party?.name).join(" ")
      ].filter(Boolean).join(" ").toLowerCase();

      const matchedKeyword = keywordSet.find(keyword => haystack.includes(keyword));
      if (!matchedKeyword) continue;

      const notice = normaliseFindTenderRelease(release, matchedKeyword);
      if (notice) notices.push(notice);
    }

    return { notices: dedupeNotices(notices).map(notice => enrichNoticeQuality(notice, keywords)), errors };
  } catch (error: any) {
    return { notices: [], errors: [error?.message || String(error)] };
  }
}


async function contractsFinderSearch(params: any, keyword: string) {
  const endpoints = [
    "https://www.contractsfinder.service.gov.uk/api/rest/2/search_notices/json",
    "https://www.contractsfinder.service.gov.uk/api/rest/2/search_notices/JSON",
    "https://www.contractsfinder.service.gov.uk/api/rest/2/search_notices"
  ];

  let lastError = "";

  for (const url of endpoints) {
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json"
        },
        body: JSON.stringify(params)
      });

      if (!response.ok) {
        lastError = `${response.status} ${response.statusText}`;
        continue;
      }

      const data = await response.json();
      const list = Array.isArray(data.noticeList) ? data.noticeList : [];
      return list.map((entry: any) => normaliseNotice(entry, keyword)).filter(Boolean) as ProcurementNotice[];
    } catch (error: any) {
      lastError = error?.message || String(error);
    }
  }

  throw new Error(lastError || "Contracts Finder search failed");
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

  return output.slice(0, 18);
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
  console.log("[db] ready");
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

  let level = "Weak";
  let warning = "The data pull returned limited or noisy matches. Treat named buyer suggestions as strategy targets unless linked to pulled records or verified source URLs.";

  if (total === 0) {
    level = "Critical";
    warning = "No structured Contracts Finder records were returned for this scan. The report must rely on strategy mapping and verified web facts only.";
  } else if (strong >= 6 && average >= 60) {
    level = "Strong";
    warning = "The structured data pull returned several relevant records. Pulled records can be used as source-backed market signals.";
  } else if (moderate >= 6 && average >= 45) {
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

async function pullProcurementData(input: z.infer<typeof intakeSchema>): Promise<ProcurementData> {
  const keywords = buildKeywords(input);
  const regions = buildRegion(input);
  const open: ProcurementNotice[] = [];
  const awarded: ProcurementNotice[] = [];
  const errors: string[] = [];

  for (const keyword of keywords) {
    const base = {
      keyword,
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
      cpvCodes: null
    };

    try {
      open.push(
        ...(await contractsFinderSearch(
          { searchCriteria: { ...base, types: ["Contract"], statuses: ["Open"] }, size: 10 },
          keyword
        ))
      );
    } catch (error: any) {
      captureError(error, { dataPull: { source: "contracts_finder", status: "open", keyword } });
      errors.push(`Open search failed for "${keyword}": ${error?.message || error}`);
    }

    try {
      awarded.push(
        ...(await contractsFinderSearch(
          { searchCriteria: { ...base, types: ["Contract"], statuses: ["Awarded"] }, size: 10 },
          keyword
        ))
      );
    } catch (error: any) {
      captureError(error, { dataPull: { source: "contracts_finder", status: "awarded", keyword } });
      errors.push(`Awarded search failed for "${keyword}": ${error?.message || error}`);
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

  const matches = [...text.matchAll(/£?\s?([0-9]+(?:\.[0-9]+)?)\s?(k|m|million|thousand)?/g)];
  const values = matches
    .map(match => {
      const n = Number(match[1]);
      const unit = match[2] || "";
      if (unit === "m" || unit === "million") return n * 1_000_000;
      if (unit === "k" || unit === "thousand") return n * 1_000;
      return n;
    })
    .filter(value => value > 0);

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

function getClientSector(input: any) {
  const text = [
    input?.companyName,
    input?.mainServices,
    input?.secondaryServices,
    input?.mainGoal,
    input?.preferredOutput
  ]
    .join(" ")
    .toLowerCase();

  if (text.includes("construction") || text.includes("quantity surveying") || text.includes("cost management") || text.includes("employer") || text.includes("building surveying") || text.includes("estate")) {
    return {
      label: "Built environment / construction consultancy",
      terms: ["construction", "quantity surveying", "cost management", "project management", "employer", "building surveying", "estate", "asset management", "contract administration", "programme management"]
    };
  }

  if (text.includes("marketing") || text.includes("creative") || text.includes("campaign") || text.includes("video") || text.includes("film") || text.includes("communications") || text.includes("event production") || text.includes("drpg")) {
    return {
      label: "Creative / marketing production / events",
      terms: ["marketing", "creative", "campaign", "video", "film", "communications", "event", "production", "digital content", "media services"]
    };
  }

  if (text.includes("photography") || text.includes("portrait") || text.includes("graduation") || text.includes("property photography") || text.includes("wedding")) {
    return {
      label: "Photography / visual content / public communications",
      terms: ["photography", "event photography", "corporate photography", "graduation", "portrait", "property photography", "visual content", "creative services"]
    };
  }

  if (text.includes("cleaning") || text.includes("facilities") || text.includes("maintenance")) {
    return {
      label: "Facilities / cleaning / property services",
      terms: ["cleaning", "facilities management", "maintenance", "soft fm", "hard fm", "property services"]
    };
  }

  return {
    label: "General public-sector services",
    terms: text.split(/\s+/).filter((word: string) => word.length > 5).slice(0, 12)
  };
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
  const sector = getClientSector(input);
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
    sectorLens: getClientSector(input).label,
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

function premiumPct(value: number, max: number) {
  if (!max || max <= 0) return 0;
  return Math.max(4, Math.min(100, Math.round((value / max) * 100)));
}

function premiumShort(value: number) {
  if (!value || Number.isNaN(value)) return "0";
  return new Intl.NumberFormat("en-GB", { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

function premiumTrustPill(label: string) {
  const safe = escapeHtml(label || "Not confirmed");
  const cls = safe.toLowerCase().replace(/[^a-z]+/g, "-");
  return `<span class="trust-pill trust-${cls}">${safe}</span>`;
}

function premiumMetricCard(label: string, value: string, note: string) {
  return `
    <div class="premium-metric">
      <small>${escapeHtml(label)}</small>
      <strong>${escapeHtml(value)}</strong>
      <span>${escapeHtml(note)}</span>
    </div>
  `;
}

function premiumHorizontalBar(label: string, valueLabel: string, width: number, note: string) {
  return `
    <div class="premium-bar-row">
      <div class="premium-bar-head">
        <strong>${escapeHtml(label)}</strong>
        <b>${escapeHtml(valueLabel)}</b>
      </div>
      <div class="premium-bar-track"><i style="width:${Math.max(4, Math.min(100, width))}%"></i></div>
      <small>${escapeHtml(note)}</small>
    </div>
  `;
}

function premiumSignalGraphic(trust: any) {
  const relevant = premiumPct(trust.relevantCount, Math.max(trust.pulledCount, 1));
  const verified = premiumPct(trust.verifiedCount, Math.max(trust.relevantCount, 1));
  const addressable = premiumPct(trust.addressableOpportunityValue, Math.max(trust.totalRelevantRecordValue, 1));

  return `
    <div class="signal-graphic" aria-label="Generated commercial signal map">
      <svg viewBox="0 0 720 260" role="img">
        <defs>
          <linearGradient id="govGold" x1="0" x2="1">
            <stop offset="0%" stop-color="#B8842D"/>
            <stop offset="100%" stop-color="#24140F"/>
          </linearGradient>
          <radialGradient id="govGlow" cx="50%" cy="50%" r="60%">
            <stop offset="0%" stop-color="#F4E6C8"/>
            <stop offset="100%" stop-color="#FFF9EF"/>
          </radialGradient>
        </defs>

        <rect x="0" y="0" width="720" height="260" rx="22" fill="url(#govGlow)" stroke="#D8BE8C"/>
        <path d="M70 185 C180 70, 300 210, 430 90 S610 115, 660 55" fill="none" stroke="#D8BE8C" stroke-width="3"/>
        <path d="M70 185 C180 70, 300 210, 430 90 S610 115, 660 55" fill="none" stroke="url(#govGold)" stroke-width="8" stroke-linecap="round" stroke-dasharray="${relevant * 9} 900"/>

        <circle cx="90" cy="170" r="22" fill="#24140F"/>
        <circle cx="265" cy="128" r="${16 + relevant / 4}" fill="#B8842D" opacity=".9"/>
        <circle cx="445" cy="95" r="${16 + verified / 4}" fill="#6F4B1F" opacity=".9"/>
        <circle cx="625" cy="68" r="${16 + addressable / 4}" fill="#24140F" opacity=".95"/>

        <text x="64" y="218" font-size="14" font-weight="700" fill="#6F5B50">Pulled</text>
        <text x="232" y="218" font-size="14" font-weight="700" fill="#6F5B50">Relevant</text>
        <text x="412" y="218" font-size="14" font-weight="700" fill="#6F5B50">Verified</text>
        <text x="582" y="218" font-size="14" font-weight="700" fill="#6F5B50">Addressable</text>

        <text x="78" y="176" text-anchor="middle" font-size="18" font-weight="800" fill="#FFF">${trust.pulledCount}</text>
        <text x="265" y="134" text-anchor="middle" font-size="18" font-weight="800" fill="#FFF">${trust.relevantCount}</text>
        <text x="445" y="101" text-anchor="middle" font-size="18" font-weight="800" fill="#FFF">${trust.verifiedCount}</text>
        <text x="625" y="74" text-anchor="middle" font-size="15" font-weight="800" fill="#FFF">${premiumShort(trust.addressableOpportunityValue)}</text>
      </svg>
    </div>
  `;
}

function premiumSourceTable(records: any[]) {
  const rows = records.slice(0, 6).map((record: any) => {
    const title = String(record.title || "Untitled record").slice(0, 76);
    const buyer = String(record.buyer || "Not stated").slice(0, 42);
    const value = trustMoney(record.addressableValue || record.recordValue || 0);
    const id = record.recordId || "—";
    const url = record.sourceUrl || record.url || "#";

    return `
      <tr>
        <td>${escapeHtml(title)}</td>
        <td>${escapeHtml(buyer)}</td>
        <td>${premiumTrustPill(record.trustStatus)}</td>
        <td>${escapeHtml(record.confidence || "—")}</td>
        <td>${escapeHtml(value)}</td>
        <td><a href="${escapeHtml(url)}" target="_blank">${escapeHtml(id)}</a></td>
      </tr>
    `;
  }).join("");

  return `
    <div class="premium-source-panel">
      <h3>Top source-backed records</h3>
      <table class="premium-source-table">
        <tr>
          <th>Record</th>
          <th>Buyer</th>
          <th>Trust</th>
          <th>Confidence</th>
          <th>Value signal</th>
          <th>ID / URL</th>
        </tr>
        ${rows || `<tr><td colspan="6">No relevant source records passed the trust filter.</td></tr>`}
      </table>
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

## 1. Brutally Honest Verdict
Give 7 bullets. Each bullet must include one trust label:
- Can they win now?
- Best first revenue route
- Biggest blocker
- Buyer type most likely to care
- Buyer type to avoid for now
- Whether to bid, partner, subcontract, or prepare
- What would make them credible in 30 days

## 2. Data Pulled vs Relevant Evidence
Create a table:
Metric | Number | What It Means | Trust Note

Include pulled records, relevant records, excluded/noisy records, verified records, inferred records, strategic target records, addressable opportunity value, and relevant pulled-record value.

## 3. Money Route Ranking
Create a table:
Rank | Route | Trust Label | Source Record ID / URL | Why Money Exists | Fit Now | What To Do This Week | Score /100

## 4. Buyer Fit Matrix
Table:
Buyer Type | Fit | Spend Logic | Trust Label | Best Entry Route | Priority

## 5. Named Buyer Watchlist
List 8-12 named buyers.
Table:
Buyer | Why They May Buy | Service to Pitch | Best Route | Trust Label | Source Record ID / URL | Confidence

If no source record supports the buyer, label it [Strategic target] or [Not confirmed].

## 6. Opportunity Map
List 8-12 opportunities.
Table:
Opportunity | Likely Buyer | Trust Label | Source Record ID / URL | Why Buyer Spends | Route | Addressable Value Signal | Evidence Needed | Next Action This Week

## 7. Contract and Award Signals From Pulled Data
Only use relevant pulled source records.
For each:
- Trust label
- Source-backed fact
- Source record ID
- Source URL
- Why it matters commercially
- Fit for this company
- Recommended action

## 8. Competitor and Supplier Landscape
Name suppliers only when sourced from pulled records or verified web search.
Each named supplier must include a trust label.

## 9. Evidence Gap Checklist
Use this table:
Asset | Status Green/Amber/Red | Why It Matters | Fix This Week

## 10. Bid / Partner / Subcontract / Ignore Rules
Give strict decision rules. Include trust labels where recommendations depend on data.

## 11. 30-Day Activation Plan
Week 1:
Week 2:
Week 3:
Week 4:

## 12. Outreach Pack
Write:
1. Capability positioning statement
2. Partner outreach email
3. Public buyer intro email
4. LinkedIn message

## 13. Risks and Caveats
Be clear about uncertainty, noisy data, weak evidence, unconfirmed facts, and human-verification needs.

## 14. Source Appendix
List source record IDs, URLs, buyer, supplier if available, trust label, and relevance reason.

## 15. Human Verification Layer
Create a final table:
Claim / Recommendation | Status: Verified/Inferred/Strategic target/Not confirmed | Evidence | Human Check Needed
`;
}


async function createReport(input: z.infer<typeof intakeSchema>) {
  const data = await pullProcurementData(input);
  const prompt = buildPrompt(input, data);

  try {
    const response = await openai.responses.create({
      model: OPENAI_MODEL,
      tools: [{ type: "web_search" } as any],
      input: prompt
    });

    return { data, report: enforceDataQualityLanguage(response.output_text || "No report returned.") };
  } catch (firstError: any) {
    try {
      const response = await openai.responses.create({
        model: OPENAI_MODEL,
        tools: [{ type: "web_search_preview" } as any],
        input: prompt
      });

      return { data, report: enforceDataQualityLanguage(response.output_text || "No report returned.") };
    } catch (secondError: any) {
      captureError(secondError, {
        openai: {
          model: OPENAI_MODEL,
          fallbackAfterPrimaryFailure: true,
          primaryError: firstError?.message || String(firstError)
        }
      });
      throw secondError;
    }
  }
}

async function runScan(id: string, input: z.infer<typeof intakeSchema>) {
  await updateScan(id, { status: "running", error_message: null });

  try {
    const { data, report } = await createReport(input);

    await updateScan(id, {
      status: "completed",
      procurement_json: data,
      report_markdown: report,
      error_message: null
    });

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



type ScoreItem = {
  label: string;
  value: number;
  note?: string;
};

type SectorProfile = {
  sector: string;
  title: string;
  accent: string;
  accentSoft: string;
  lead: string;
  marketingLine: string;
  buyerFit: ScoreItem[];
  routeRanking: ScoreItem[];
};



function average(values: number[]) {
  if (!values.length) return 0;
  return Math.round(values.reduce((sum, value) => sum + value, 0) / values.length);
}

function scanInputText(scan: ScanRecord) {
  const input = scan.input_json || {};
  return [
    input.companyName,
    input.website,
    input.location,
    input.areasServed,
    input.mainServices,
    input.secondaryServices,
    input.idealBuyers,
    input.mainGoal,
    input.preferredOutput,
    input.biggestConcern
  ]
    .join(" ")
    .toLowerCase();
}

function detectSector(scan: ScanRecord) {
  const text = scanInputText(scan);

  if (
    text.includes("construction") ||
    text.includes("quantity surveying") ||
    text.includes("cost management") ||
    text.includes("employer") ||
    text.includes("building surveying") ||
    text.includes("estate consultancy") ||
    text.includes("project management")
  ) {
    return "built-environment";
  }

  if (
    text.includes("photography") ||
    text.includes("photographer") ||
    text.includes("portrait") ||
    text.includes("event photography") ||
    text.includes("graduation") ||
    text.includes("wedding")
  ) {
    return "creative-services";
  }

  if (
    text.includes("retrofit") ||
    text.includes("solar") ||
    text.includes("ev charging") ||
    text.includes("decarbonisation") ||
    text.includes("net zero")
  ) {
    return "energy-retrofit";
  }

  if (
    text.includes("facilities") ||
    text.includes("maintenance") ||
    text.includes("cleaning") ||
    text.includes("security")
  ) {
    return "facilities";
  }

  return "professional-services";
}

function sectorProfile(scan: ScanRecord): SectorProfile {
  const sector = detectSector(scan);

  if (sector === "built-environment") {
    return {
      sector,
      title: "Built Environment / Construction Consultancy",
      accent: "#8A5A23",
      accentSoft: "#F1E4D0",
      lead:
        "This scan is tuned for construction consultancy, public estate spend, capital programmes, frameworks and multi-disciplinary advisory routes.",
      marketingLine:
        "GovRevenue helps built-environment firms turn fragmented estate, capital programme and framework signals into buyer-specific revenue routes.",
      buyerFit: [
        { label: "NHS Estates", value: 88, note: "Large estates, surveys, capital planning" },
        { label: "Local Authorities", value: 84, note: "Capital programmes and regeneration" },
        { label: "Universities", value: 72, note: "Campus estates and decarbonisation" },
        { label: "Housing", value: 68, note: "Asset condition and refurbishment" },
        { label: "Frameworks", value: 86, note: "Best route for mature firms" },
        { label: "Subcontract", value: 61, note: "Useful for narrow specialist gaps" }
      ],
      routeRanking: [
        { label: "Framework activation", value: 90, note: "Highest leverage if evidence is ready" },
        { label: "Direct estate buyers", value: 82, note: "NHS/council/university estate teams" },
        { label: "Partner on major programmes", value: 76, note: "Useful on multi-disciplinary work" },
        { label: "Subcontract routes", value: 58, note: "Lower control but faster entry" },
        { label: "Small ad hoc tenders", value: 28, note: "Often low-margin distraction" }
      ]
    };
  }

  if (sector === "creative-services") {
    return {
      sector,
      title: "Creative / Photography Services",
      accent: "#9B6A2D",
      accentSoft: "#F3E6D8",
      lead:
        "This scan is tuned for photography, campaign visuals, events, university ceremonies, council communications and subcontract entry routes.",
      marketingLine:
        "GovRevenue helps creative businesses convert public-sector demand into targeted buyer lists, proof gaps and small-contract entry routes.",
      buyerFit: [
        { label: "Local Councils", value: 72, note: "Events, campaigns, civic comms" },
        { label: "Universities", value: 68, note: "Graduation and event visuals" },
        { label: "Cultural Bodies", value: 62, note: "Campaign storytelling" },
        { label: "Housing", value: 50, note: "Property/community visuals" },
        { label: "NHS Comms", value: 42, note: "Compliance-heavy route" },
        { label: "Large Frameworks", value: 31, note: "Low fit until proof improves" }
      ],
      routeRanking: [
        { label: "Small direct projects", value: 76, note: "Fastest route to first proof" },
        { label: "Subcontracting", value: 70, note: "Build public-sector case studies" },
        { label: "University/event teams", value: 65, note: "Recurring demand but competitive" },
        { label: "Council comms teams", value: 58, note: "Useful if capability pack is ready" },
        { label: "Large frameworks", value: 30, note: "Avoid until compliance is mature" }
      ]
    };
  }

  if (sector === "energy-retrofit") {
    return {
      sector,
      title: "Energy / Retrofit / Net Zero",
      accent: "#2C6E49",
      accentSoft: "#DDEDE4",
      lead:
        "This scan is tuned for retrofit, decarbonisation, energy efficiency, public estate transition and infrastructure-linked buyer demand.",
      marketingLine:
        "GovRevenue helps energy and retrofit firms see where public estate pressure is turning into funded procurement routes.",
      buyerFit: [
        { label: "Councils", value: 86, note: "Decarbonisation and estate targets" },
        { label: "Housing", value: 82, note: "Retrofit and compliance pressure" },
        { label: "Schools", value: 70, note: "Estate upgrades and energy savings" },
        { label: "NHS", value: 74, note: "Large estate energy demand" },
        { label: "Frameworks", value: 78, note: "Strong route if accreditations exist" },
        { label: "Partners", value: 72, note: "Useful for delivery capacity" }
      ],
      routeRanking: [
        { label: "Frameworks", value: 84, note: "Best for repeat public work" },
        { label: "Housing retrofit", value: 82, note: "High recurring demand" },
        { label: "Council estate pilots", value: 76, note: "Good entry if proof exists" },
        { label: "Partner delivery", value: 70, note: "Reduces compliance burden" },
        { label: "Cold tender chasing", value: 38, note: "Low efficiency without proof" }
      ]
    };
  }

  if (sector === "facilities") {
    return {
      sector,
      title: "Facilities / Property Services",
      accent: "#5B6770",
      accentSoft: "#E5E8EA",
      lead:
        "This scan is tuned for facilities management, estate maintenance, property services and recurring public-sector operational spend.",
      marketingLine:
        "GovRevenue helps property-service firms find recurring public estate routes instead of chasing random tenders.",
      buyerFit: [
        { label: "Councils", value: 78, note: "Operational estate spend" },
        { label: "Housing", value: 80, note: "Recurring repairs and planned works" },
        { label: "Schools", value: 66, note: "Local estate needs" },
        { label: "NHS", value: 62, note: "Compliance-heavy but valuable" },
        { label: "Frameworks", value: 76, note: "Important for recurring work" },
        { label: "Subcontract", value: 68, note: "Good entry under prime suppliers" }
      ],
      routeRanking: [
        { label: "Framework entry", value: 78, note: "Repeatable route" },
        { label: "Housing associations", value: 76, note: "Recurring asset need" },
        { label: "Council property teams", value: 70, note: "Accessible buyer route" },
        { label: "Subcontracting", value: 64, note: "Good if proof is thin" },
        { label: "One-off tenders", value: 44, note: "Often admin-heavy" }
      ]
    };
  }

  return {
    sector,
    title: "Professional Services",
    accent: "#7A5A35",
    accentSoft: "#EFE4D3",
    lead:
      "This scan is tuned for service-led firms selling expertise, delivery capability, advisory support or specialist operational work into public-sector buyers.",
    marketingLine:
      "GovRevenue helps service businesses convert public procurement noise into a focused revenue map.",
    buyerFit: [
      { label: "Councils", value: 70, note: "Broad service demand" },
      { label: "Universities", value: 62, note: "Specialist services" },
      { label: "NHS", value: 58, note: "Higher compliance" },
      { label: "Housing", value: 60, note: "Operational need" },
      { label: "Frameworks", value: 66, note: "Repeatable but competitive" },
      { label: "Partners", value: 64, note: "Useful for entry" }
    ],
    routeRanking: [
      { label: "Partner routes", value: 70, note: "Best entry if proof is weak" },
      { label: "Direct buyer outreach", value: 68, note: "Good for targeted needs" },
      { label: "Small tenders", value: 58, note: "Selective only" },
      { label: "Frameworks", value: 55, note: "Needs readiness" },
      { label: "Large tenders", value: 30, note: "Avoid until credible" }
    ]
  };
}

function keywordAlignmentScore(scan: ScanRecord, profile: SectorProfile) {
  const data = scan.procurement_json as ProcurementData | null;
  const keywords = (data?.keywords || []).join(" ").toLowerCase();

  if (!keywords) return 40;

  if (profile.sector === "built-environment") {
    if (keywords.includes("photography") || keywords.includes("creative")) return 25;
    if (keywords.includes("construction") || keywords.includes("cost") || keywords.includes("survey") || keywords.includes("project")) return 88;
  }

  if (profile.sector === "creative-services") {
    if (keywords.includes("photography") || keywords.includes("creative") || keywords.includes("media")) return 84;
    if (keywords.includes("construction") || keywords.includes("survey")) return 25;
  }

  if (profile.sector === "energy-retrofit") {
    if (keywords.includes("retrofit") || keywords.includes("energy") || keywords.includes("solar") || keywords.includes("net zero")) return 86;
  }

  return 62;
}

function computePremiumScores(scan: ScanRecord) {
  const data = scan.procurement_json as ProcurementData | null;
  const profile = sectorProfile(scan);
  const input = scan.input_json || {};
  const report = String(scan.report_markdown || "").toLowerCase();

  const openCount = data?.contractsFinder?.open?.length || 0;
  const awardedCount = data?.contractsFinder?.awarded?.length || 0;

  const publicExperience = String(input.publicSectorExperience || "").toLowerCase();
  const certifications = String(input.certifications || "").toLowerCase();
  const caseStudies = String(input.caseStudies || "").toLowerCase();

  const dataConfidence = clampScore(
    average([
      keywordAlignmentScore(scan, profile),
      Math.min(90, 35 + openCount * 3 + awardedCount * 2),
      data?.contractsFinder?.errors?.length ? 55 : 78
    ])
  );

  let readiness = 50;
  if (publicExperience.includes("strong")) readiness += 24;
  if (publicExperience.includes("some")) readiness += 10;
  if (publicExperience.includes("none") || publicExperience.includes("early")) readiness -= 14;

  if (certifications.includes("public liability")) readiness += 10;
  if (certifications.includes("professional indemnity")) readiness += 8;
  if (certifications.includes("gdpr")) readiness += 6;
  if (certifications.includes("to be confirmed") || certifications.includes("needs")) readiness -= 10;

  if (caseStudies.length > 80) readiness += 8;
  if (report.includes("red")) readiness -= 6;
  if (report.includes("amber")) readiness -= 3;

  const buyerFit = clampScore(average(profile.buyerFit.slice(0, 4).map(item => item.value)));
  const routeStrength = clampScore(average(profile.routeRanking.slice(0, 3).map(item => item.value)));
  const evidenceStrength = clampScore(readiness);
  const revenueOpportunity = clampScore(average([buyerFit, routeStrength, dataConfidence, evidenceStrength]));
  const competitivePressure = profile.sector === "creative-services" ? 72 : profile.sector === "built-environment" ? 68 : 62;

  return {
    buyerFit,
    procurementReadiness: evidenceStrength,
    evidenceStrength,
    revenueOpportunity,
    dataConfidence,
    competitivePressure
  };
}

function ringScoreHtml(label: string, value: number, note: string, accent: string) {
  const score = clampScore(value);
  return `
    <div class="ring-card">
      <svg class="ring" viewBox="0 0 44 44" aria-hidden="true">
        <circle class="ring-bg" cx="22" cy="22" r="16"></circle>
        <circle class="ring-fg" cx="22" cy="22" r="16" pathLength="100" stroke-dasharray="${score} 100" style="stroke:${accent}"></circle>
        <text x="22" y="24" text-anchor="middle">${score}</text>
      </svg>
      <div>
        <b>${escapeHtml(label)}</b>
        <span>${escapeHtml(note)}</span>
      </div>
    </div>
  `;
}

function barChartSvg(title: string, items: ScoreItem[], accent: string) {
  const width = 820;
  const rowHeight = 48;
  const height = 78 + items.length * rowHeight;
  const maxBar = 500;

  const rows = items
    .map((item, index) => {
      const y = 58 + index * rowHeight;
      const barWidth = Math.max(10, Math.round((clampScore(item.value) / 100) * maxBar));

      return `
        <text x="22" y="${y + 12}" class="chart-label">${escapeHtml(item.label)}</text>
        <rect x="255" y="${y}" width="${maxBar}" height="16" rx="8" class="chart-track"></rect>
        <rect x="255" y="${y}" width="${barWidth}" height="16" rx="8" fill="${accent}"></rect>
        <text x="${265 + maxBar}" y="${y + 13}" class="chart-score">${clampScore(item.value)}</text>
        <text x="255" y="${y + 36}" class="chart-note">${escapeHtml(item.note || "")}</text>
      `;
    })
    .join("");

  return `
    <svg class="svg-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(title)}">
      <text x="22" y="30" class="chart-title">${escapeHtml(title)}</text>
      ${rows}
    </svg>
  `;
}

function opportunityMatrixSvg(profile: SectorProfile) {
  const width = 820;
  const height = 410;
  const items = profile.routeRanking.slice(0, 5);

  const points = items
    .map((item, index) => {
      const x = 120 + Math.round((clampScore(item.value) / 100) * 580);
      const y = 310 - index * 48 - Math.round((clampScore(item.value) - 50) * 0.8);
      const safeY = Math.max(80, Math.min(320, y));

      return `
        <circle cx="${x}" cy="${safeY}" r="10" fill="${profile.accent}"></circle>
        <text x="${x + 16}" y="${safeY + 5}" class="matrix-label">${escapeHtml(item.label)}</text>
      `;
    })
    .join("");

  return `
    <svg class="matrix-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="Opportunity priority matrix">
      <text x="22" y="30" class="chart-title">Opportunity Priority Matrix</text>
      <rect x="90" y="60" width="640" height="280" rx="12" class="matrix-bg"></rect>
      <line x1="410" y1="60" x2="410" y2="340" class="matrix-line"></line>
      <line x1="90" y1="200" x2="730" y2="200" class="matrix-line"></line>
      <text x="112" y="88" class="matrix-zone">Strategic bets</text>
      <text x="492" y="88" class="matrix-zone">Prioritise now</text>
      <text x="112" y="322" class="matrix-zone">Watch / ignore</text>
      <text x="492" y="322" class="matrix-zone">Quick wins</text>
      ${points}
      <text x="275" y="378" class="axis-label">Win likelihood / route fit →</text>
      <text x="22" y="214" transform="rotate(-90 22 214)" class="axis-label">Revenue potential →</text>
    </svg>
  `;
}

function sectorHeroSvg(profile: SectorProfile, openCount: number, awardedCount: number) {
  const total = Math.max(1, openCount + awardedCount);
  const openWidth = Math.round((openCount / total) * 420);
  const awardedWidth = Math.round((awardedCount / total) * 420);

  return `
    <svg class="sector-visual" viewBox="0 0 860 270" role="img" aria-label="Generated sector intelligence visual">
      <defs>
        <linearGradient id="govrevGrad" x1="0" x2="1">
          <stop offset="0%" stop-color="${profile.accent}" stop-opacity=".95"/>
          <stop offset="100%" stop-color="#24140f" stop-opacity=".96"/>
        </linearGradient>
      </defs>
      <rect x="0" y="0" width="860" height="270" rx="24" fill="${profile.accentSoft}"></rect>
      <circle cx="720" cy="56" r="105" fill="${profile.accent}" opacity=".09"></circle>
      <circle cx="790" cy="210" r="130" fill="#24140f" opacity=".07"></circle>

      <text x="34" y="48" class="visual-kicker">Generated intelligence layer</text>
      <text x="34" y="86" class="visual-title">${escapeHtml(profile.title)}</text>
      <text x="34" y="118" class="visual-copy">${escapeHtml(profile.lead)}</text>

      <rect x="34" y="152" width="420" height="16" rx="8" fill="#fff" opacity=".78"></rect>
      <rect x="34" y="152" width="${openWidth}" height="16" rx="8" fill="${profile.accent}"></rect>
      <text x="34" y="192" class="visual-label">Open records: ${openCount}</text>

      <rect x="34" y="212" width="420" height="16" rx="8" fill="#fff" opacity=".78"></rect>
      <rect x="34" y="212" width="${awardedWidth}" height="16" rx="8" fill="#24140f"></rect>
      <text x="34" y="252" class="visual-label">Award / history signals: ${awardedCount}</text>

      <g opacity=".95">
        <path d="M570 204 C610 130 660 160 694 86 C726 20 782 64 812 34" fill="none" stroke="url(#govrevGrad)" stroke-width="8" stroke-linecap="round"></path>
        <circle cx="570" cy="204" r="12" fill="${profile.accent}"></circle>
        <circle cx="694" cy="86" r="12" fill="${profile.accent}"></circle>
        <circle cx="812" cy="34" r="12" fill="#24140f"></circle>
        <text x="548" y="238" class="node-label">buyer</text>
        <text x="672" y="120" class="node-label">signal</text>
        <text x="764" y="70" class="node-label">route</text>
      </g>
    </svg>
  `;
}

function evidenceCardsHtml(scan: ScanRecord) {
  const input = scan.input_json || {};
  const text = [
    input.publicSectorExperience,
    input.caseStudies,
    input.certifications,
    input.biggestConcern
  ]
    .join(" ")
    .toLowerCase();

  const rows = [
    {
      asset: "Public-sector case studies",
      status: text.includes("strong") || text.includes("public-sector") ? "Amber" : "Red",
      action: "Package 2-3 proof stories into buyer-ready case studies."
    },
    {
      asset: "Insurance / compliance",
      status: text.includes("public liability") || text.includes("professional indemnity") ? "Amber" : "Red",
      action: "Confirm public liability, PI where needed, privacy/GDPR and usage terms."
    },
    {
      asset: "Capability statement",
      status: text.includes("capability statement") ? "Amber" : "Red",
      action: "Create a one-page public-sector capability statement."
    },
    {
      asset: "Buyer-specific pitch",
      status: "Amber",
      action: "Turn the report’s top 5 buyer routes into tailored outreach."
    },
    {
      asset: "Pricing / capacity",
      status: text.includes("team") || text.includes("capacity") ? "Amber" : "Red",
      action: "Set contract size limits, delivery team and response process."
    },
    {
      asset: "Procurement readiness",
      status: text.includes("strong") ? "Green" : "Amber",
      action: "Only chase routes that match current proof and compliance."
    }
  ];

  return `
    <div class="evidence-grid">
      ${rows
        .map(
          row => `
            <div class="evidence-card ${row.status.toLowerCase()}">
              <div class="status-dot"></div>
              <b>${escapeHtml(row.asset)}</b>
              <span>${escapeHtml(row.status)}</span>
              <p>${escapeHtml(row.action)}</p>
            </div>
          `
        )
        .join("")}
    </div>
  `;
}

function routeCardsHtml(profile: SectorProfile) {
  return `
    <div class="route-grid">
      ${profile.routeRanking
        .map(
          (item, index) => `
            <div class="route-card">
              <small>Route ${index + 1}</small>
              <b>${escapeHtml(item.label)}</b>
              <div class="route-score">
                <span style="width:${clampScore(item.value)}%"></span>
              </div>
              <p>${escapeHtml(item.note || "")}</p>
            </div>
          `
        )
        .join("")}
    </div>
  `;
}

function govRevenueCloseHtml(scan: ScanRecord, profile: SectorProfile, scores: ReturnType<typeof computePremiumScores>) {
  const company = escapeHtml(scan.company_name);
  const route = escapeHtml(profile.routeRanking[0]?.label || "targeted public-sector route");

  return `
    <section class="pdf-section close-section">
      <div class="close-mark">GovRevenue</div>
      <div class="close-grid">
        <div>
          <h2>From procurement noise to a revenue map.</h2>
          <p class="big-copy">
            For ${company}, the value is not just finding tenders. The value is knowing which public-sector routes are worth chasing, which ones to ignore, who already buys similar services, and what proof must exist before outreach.
          </p>
          <p>
            Recommended immediate focus: <strong>${route}</strong>. Current revenue opportunity score: <strong>${scores.revenueOpportunity}/100</strong>. Data confidence score: <strong>${scores.dataConfidence}/100</strong>.
          </p>
        </div>
        <div class="close-panel">
          <b>GovRevenue turns public data into commercial action.</b>
          <ul>
            <li>Buyer fit and opportunity scoring</li>
            <li>Public procurement signal mapping</li>
            <li>Evidence gap diagnosis</li>
            <li>Subcontract, partner and bid route planning</li>
            <li>30-day activation plan</li>
          </ul>
          <p>${escapeHtml(profile.marketingLine)}</p>
        </div>
      </div>
    </section>
  `;
}




function includesAny(text: string, terms: string[]) {
  const lower = text.toLowerCase();
  return terms.some(term => lower.includes(term.toLowerCase()));
}

function buildPremiumScores(scan: ScanRecord) {
  const input = scan.input_json || {};
  const data = scan.procurement_json as ProcurementData | null;
  const text = [
    input.companyName,
    input.mainServices,
    input.secondaryServices,
    input.publicSectorExperience,
    input.caseStudies,
    input.certifications,
    input.biggestConcern,
    input.mainGoal
  ].join(" ").toLowerCase();

  const openCount = data?.contractsFinder?.open?.length || 0;
  const awardedCount = data?.contractsFinder?.awarded?.length || 0;
  const totalData = openCount + awardedCount;

  const hasPublicExperience = includesAny(text, ["strong", "public-sector", "nhs", "council", "university", "framework", "case study"]);
  const hasInsurance = includesAny(text, ["public liability", "professional indemnity", "insurance", "rics", "iso", "accreditation", "accredited"]);
  const hasCaseStudies = includesAny(text, ["case stud", "project example", "portfolio", "nhs", "council", "university", "hospital"]);
  const isEstablished = !includesAny(text, ["none", "early-stage", "no public-sector", "no confirmed"]);

  const buyerFit = clampScore(45 + Math.min(30, totalData * 1.2) + (hasPublicExperience ? 15 : 0) + (isEstablished ? 10 : 0));
  const procurementReadiness = clampScore(30 + (hasInsurance ? 25 : 0) + (hasCaseStudies ? 20 : 0) + (hasPublicExperience ? 15 : 0));
  const evidenceStrength = clampScore(25 + (hasCaseStudies ? 30 : 0) + (hasPublicExperience ? 25 : 0) + (hasInsurance ? 10 : 0));
  const revenuePotential = clampScore(40 + Math.min(35, openCount * 2) + Math.min(20, awardedCount));
  const dataConfidence = clampScore(25 + Math.min(45, totalData * 2) + (data?.keywords?.length ? 10 : 0));

  const recommendedRoute =
    procurementReadiness >= 70 && evidenceStrength >= 65
      ? "Bid + framework activation"
      : buyerFit >= 65
      ? "Partner / subcontract first"
      : "Prepare before bidding";

  const verdict =
    procurementReadiness >= 70
      ? "Bid-ready with targeted public-sector activation."
      : buyerFit >= 65
      ? "Commercially interesting, but credibility must be packaged before serious bids."
      : "Preparation-stage: build evidence before chasing formal tenders.";

  return {
    buyerFit,
    procurementReadiness,
    evidenceStrength,
    revenuePotential,
    dataConfidence,
    recommendedRoute,
    verdict
  };
}

function scoreClass(score: number) {
  if (score >= 75) return "strong";
  if (score >= 50) return "medium";
  return "weak";
}

function scoreBar(label: string, score: number, note: string) {
  return `
    <div class="score-row">
      <div class="score-label">
        <strong>${escapeHtml(label)}</strong>
        <span>${escapeHtml(note)}</span>
      </div>
      <div class="score-track">
        <div class="score-fill ${scoreClass(score)}" style="width:${score}%"></div>
      </div>
      <div class="score-number">${score}</div>
    </div>
  `;
}

function buildPremiumDashboard(scan: ScanRecord) {
  const scores = buildPremiumScores(scan);
  const data = scan.procurement_json as ProcurementData | null;
  const openCount = data?.contractsFinder?.open?.length || 0;
  const awardedCount = data?.contractsFinder?.awarded?.length || 0;
  const errors = data?.contractsFinder?.errors?.length || 0;

  return `
    <section class="premium-dashboard">
      <div class="premium-section-label">Executive intelligence dashboard</div>

      <div class="premium-grid">
        <div class="premium-card hero-card">
          <span class="eyebrow">Recommended route</span>
          <h2>${escapeHtml(scores.recommendedRoute)}</h2>
          <p>${escapeHtml(scores.verdict)}</p>
        </div>

        <div class="premium-card">
          <span class="eyebrow">Buyer fit</span>
          <div class="giant-score">${scores.buyerFit}</div>
          <p>How naturally the client maps to public-sector buyers and spend logic.</p>
        </div>

        <div class="premium-card">
          <span class="eyebrow">Revenue potential</span>
          <div class="giant-score">${scores.revenuePotential}</div>
          <p>Weighted signal from open opportunities, award history and route relevance.</p>
        </div>
      </div>

      <div class="premium-card scorecard">
        <h3>Readiness and opportunity scoring</h3>
        ${scoreBar("Buyer fit score", scores.buyerFit, "Demand logic and buyer relevance")}
        ${scoreBar("Procurement readiness", scores.procurementReadiness, "Policies, insurance and bid readiness")}
        ${scoreBar("Evidence strength", scores.evidenceStrength, "Case studies, proof and credibility")}
        ${scoreBar("Revenue opportunity", scores.revenuePotential, "Near-term commercial opportunity")}
        ${scoreBar("Data confidence", scores.dataConfidence, "Strength of pulled procurement records")}
      </div>

      <div class="visual-grid">
        <div class="premium-card chart-card">
          <h3>Procurement signal mix</h3>
          <div class="mini-bars">
            <div>
              <span>Open records</span>
              <b>${openCount}</b>
              <div class="mini-track"><i style="width:${Math.min(100, openCount * 5)}%"></i></div>
            </div>
            <div>
              <span>Award signals</span>
              <b>${awardedCount}</b>
              <div class="mini-track"><i style="width:${Math.min(100, awardedCount * 5)}%"></i></div>
            </div>
            <div>
              <span>Data errors</span>
              <b>${errors}</b>
              <div class="mini-track danger"><i style="width:${Math.min(100, errors * 20)}%"></i></div>
            </div>
          </div>
        </div>

        <div class="premium-card chart-card">
          <h3>Route logic</h3>
          <div class="route-pills">
            <span>Bid</span>
            <span>Partner</span>
            <span>Subcontract</span>
            <span>Prepare</span>
            <span>Ignore traps</span>
          </div>
          <p>The scan ranks each route by buyer fit, proof strength, complexity and realistic time-to-revenue.</p>
        </div>
      </div>
    </section>
  `;
}

function buildGovRevenueClose(scan: ScanRecord) {
  const input = scan.input_json || {};
  const services = String(input.mainServices || "public-sector revenue opportunities");
  const company = scan.company_name;

  return `
    <section class="govrevenue-close">
      <div class="close-inner">
        <span class="premium-section-label">GovRevenue commercial note</span>
        <h2>Turn public-sector noise into a route-to-revenue system.</h2>
        <p>
          For <strong>${escapeHtml(company)}</strong>, the value is not simply finding tenders.
          The value is knowing which buyers matter, which routes are realistic, which evidence gaps block revenue,
          and where to act before competitors waste months chasing the wrong opportunities.
        </p>
        <p>
          GovRevenue converts fragmented procurement data into a practical commercial map for
          <strong>${escapeHtml(services)}</strong>: who buys, who wins, what to prepare, and what to do next.
        </p>
        <div class="close-points">
          <div><b>01</b><span>Buyer intelligence</span></div>
          <div><b>02</b><span>Contract and award signals</span></div>
          <div><b>03</b><span>Evidence gap diagnosis</span></div>
          <div><b>04</b><span>30-day activation plan</span></div>
        </div>
      </div>
    </section>
  `;
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

  const construction = hasAny(serviceText, [
    "construction",
    "quantity surveying",
    "cost management",
    "project management",
    "building surveying",
    "estate",
    "employer",
    "contract administration",
    "asset management"
  ]);

  const creative = hasAny(serviceText, [
    "photography",
    "creative",
    "media",
    "event",
    "portrait",
    "graduation",
    "wedding",
    "campaign",
    "content"
  ]);

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
      (construction ? 12 : 0) +
      (creative ? 6 : 0)
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
      (construction ? 15 : 0) +
      (creative ? 7 : 0)
  );

  const dataConfidence = clampScore(
    35 +
      (openCount > 0 ? 22 : 0) +
      (awardCount > 0 ? 22 : 0) -
      (errorCount * 8) +
      (construction || creative ? 12 : 0)
  );

  const route =
    procurementReadiness >= 72 && evidenceStrength >= 70
      ? "Bid + framework activation"
      : procurementReadiness >= 55
        ? "Targeted bid + partner route"
        : "Partner/subcontract first";

  const sector =
    construction ? escapeHtml(inferSectorLens(scan)) :
    creative ? "Creative, media and visual services" :
    "Professional services";

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

function routeRows(scan: ScanRecord) {
  const input = scan.input_json || {};
  const text = [input.mainServices, input.secondaryServices, input.idealBuyers, input.mainGoal].join(" ").toLowerCase();
  const scores = calcPremiumScores(scan);

  if (hasAny(text, ["construction", "quantity surveying", "cost management", "project management", "building surveying", "estate"])) {
    return [
      { route: "Framework activation", score: clampScore(scores.procurementReadiness + 8), note: "Best for mature consultancy appointments." },
      { route: "NHS / estate buyers", score: clampScore(scores.buyerFit + 5), note: "Strongest route where estate proof exists." },
      { route: "Local authority capital programmes", score: clampScore(scores.revenueOpportunity), note: "Useful where regeneration and asset spend exists." },
      { route: "Partner on multi-disciplinary bids", score: 72, note: "Reduces bid risk on complex programmes." },
      { route: "Low-value reactive work", score: 28, note: "Usually poor fit for premium consultancy." }
    ];
  }

  if (hasAny(text, ["photography", "creative", "media", "event", "campaign", "content"])) {
    return [
      { route: "Small direct buyer outreach", score: 76, note: "Fastest proof route for early-stage public-sector entry." },
      { route: "Subcontract via incumbent agencies", score: 72, note: "Best route to build first public-sector case studies." },
      { route: "University/event teams", score: 64, note: "Recurring visual-content demand, but buyer access matters." },
      { route: "Council comms teams", score: 58, note: "Useful for campaigns and civic events." },
      { route: "Large frameworks", score: 31, note: "Usually too admin-heavy until evidence improves." }
    ];
  }

  return [
    { route: "Direct buyer mapping", score: scores.buyerFit, note: "Find buyer teams with known spend logic." },
    { route: "Partner/subcontract", score: 70, note: "Reduces trust gap while evidence is built." },
    { route: "Framework watchlist", score: scores.procurementReadiness, note: "Useful after compliance assets are ready." },
    { route: "Large tender pursuit", score: 35, note: "Avoid unless proof and capacity are strong." }
  ];
}

function miniBar(label: string, value: number, note: string) {
  return `
    <div class="mini-bar-row">
      <div class="mini-bar-label">
        <strong>${escapeHtml(label)}</strong>
        <span>${escapeHtml(note)}</span>
      </div>
      <div class="mini-bar-track">
        <div class="mini-bar-fill" style="width:${clampScore(value)}%"></div>
      </div>
      <b>${clampScore(value)}</b>
    </div>
  `;
}

function premiumDashboardHtml(scan: ScanRecord) {
  const scores = calcPremiumScores(scan);
  const routes = routeRows(scan);

  return `
    ${evidenceDashboard(scan)}
  `;
}

function premiumClosingHtml(scan: ScanRecord) {
  const scores = calcPremiumScores(scan);
  const company = scan.company_name;

  return `
    <section class="marketing-close">
      <div class="close-kicker">GovRevenue commercial close</div>
      <h2>From public-sector noise to a route-to-revenue system.</h2>
      <p>For <strong>${escapeHtml(company)}</strong>, this scan translates public procurement data into a practical commercial map: where demand exists, which buyers matter, what evidence is missing, and which route should be pursued first.</p>
      <div class="close-grid">
        <div>
          <b>Recommended route</b>
          <span>${escapeHtml(scores.route)}</span>
        </div>
        <div>
          <b>Sector lens</b>
          <span>${escapeHtml(scores.sector)}</span>
        </div>
        <div>
          <b>Next value unlock</b>
          <span>Turn this scan into a 30-day buyer action campaign.</span>
        </div>
      </div>
      <p class="close-note">GovRevenue helps businesses stop guessing at public-sector opportunities. The product turns buyer signals, contract records and readiness gaps into a focused revenue plan that teams can act on immediately.</p>
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








function inferSectorLens(scan: ScanRecord) {
  const input: any = scan.input_json || {};
  const text = [
    input.companyName,
    input.mainServices,
    input.secondaryServices,
    input.idealBuyers,
    input.mainGoal,
    input.preferredOutput
  ].join(" ").toLowerCase();

  if (
    text.includes("marketing") ||
    text.includes("creative") ||
    text.includes("campaign") ||
    text.includes("video") ||
    text.includes("film") ||
    text.includes("event production") ||
    text.includes("communications") ||
    text.includes("drpg")
  ) {
    return "Creative / marketing production / events";
  }

  if (
    text.includes("photography") ||
    text.includes("portrait") ||
    text.includes("graduation") ||
    text.includes("wedding") ||
    text.includes("property photography")
  ) {
    return "Photography / visual content / public communications";
  }

  if (
    text.includes("construction") ||
    text.includes("quantity surveying") ||
    text.includes("cost management") ||
    text.includes("project management") ||
    text.includes("employer") ||
    text.includes("building surveying") ||
    text.includes("estate")
  ) {
   const scanData = scan as Record<string, any>;
const explicitSectorLens = String(scanData.sector_lens ?? scanData.sectorLens ?? "").trim();

if (explicitSectorLens) return explicitSectorLens;

const sector = inferGovRevenueSector(scan).toLowerCase();

if (sector.includes("clean")) {
  return "Specialist cleaning, healthcare deep cleaning, education facilities cleaning, local authority estate cleaning and reactive hygiene support";
}

return inferGovRevenueSector(scan);
  }

  if (
    text.includes("retrofit") ||
    text.includes("solar") ||
    text.includes("energy") ||
    text.includes("decarbonisation") ||
    text.includes("net zero")
  ) {
    return "Energy / retrofit / built-environment decarbonisation";
  }

  return "General public-sector services";
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

function reportPage(scan: ScanRecord) {
  const data = scan.procurement_json as ProcurementData | null;
  const sectorLens = inferSectorLens(scan);
  const dataQuality = assessDataQuality(scan);
  const openCount = data?.contractsFinder?.open?.length || 0;
  const awardedCount = data?.contractsFinder?.awarded?.length || 0;
  const keywords = data?.keywords?.join(", ") || "Pending";
  const regions = data?.regions || "Pending";
  const scores = calcPremiumScores(scan);

  const content = scan.report_markdown
    ? markdownToHtml(scan.report_markdown)
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
      </div>
    </div>

    <section class="cover">
      <h1>GovRevenue Scan</h1>
      <p class="subtitle">Commercial public-sector opportunity scan for <strong>${escapeHtml(scan.company_name)}</strong>. Built from intake data, Contracts Finder records, verified web research and analyst scoring.</p>

      <div class="meta">
        <div class="metric"><b>Status</b><span>${escapeHtml(scan.status)}</span><small>Latest scan state</small></div>
        <div class="metric"><b>Open records</b><span>${openCount}</span><small>Contracts Finder matches</small></div>
        <div class="metric"><b>Award signals</b><span>${awardedCount}</span><small>Historical award matches</small></div>
        <div class="metric"><b>Route</b><span style="font-size:20px">${escapeHtml(scores.route)}</span><small>Recommended route</small></div>
      </div>

      <div class="data-strip">
        <p><strong>Sector lens:</strong> ${escapeHtml(scores.sector)}</p>
        <p><strong>Regions searched:</strong> ${escapeHtml(regions)}</p>
        <p><strong>Keywords searched:</strong> ${escapeHtml(keywords)}</p>
        <p><strong>Generated:</strong> ${escapeHtml(formatDate(scan.updated_at))}</p>
        <p><strong>Data quality:</strong> ${escapeHtml(data?.quality?.level || "Pending")} — ${escapeHtml(data?.quality?.warning || "Data quality summary pending.")}</p>
      </div>
    </section>

    ${premiumDashboardHtml(scan)}

    <section class="report">
      ${content}
    </section>

    ${scan.report_markdown ? premiumClosingHtml(scan) : ""}

    <p class="footer">No outcome is guaranteed. This scan is commercial intelligence, not legal, procurement or financial advice. Human verification is required before bid decisions.</p>
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

app.get("/", (_req, res) => {
  res.type("html").send(`<!doctype html>
<html>
<head>
  <title>GovRevenue Agent</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <!-- GOVREVENUE_PREMIUM_HOME -->
  <style>
    :root {
      --ink:#20110c;
      --muted:#705c50;
      --paper:#fffaf3;
      --sand:#f3eadc;
      --gold:#b99155;
      --line:#d7bd92;
      --dark:#24140f;
    }
    * { box-sizing:border-box; }
    body {
      margin:0;
      font-family: Arial, sans-serif;
      background:
        radial-gradient(circle at top left, rgba(185,145,85,.28), transparent 34%),
        linear-gradient(135deg, #f7efe3 0%, #ead7bb 100%);
      color:var(--ink);
    }
    .page { max-width:1180px; margin:0 auto; padding:34px 22px 56px; }
    .nav { display:flex; justify-content:space-between; align-items:center; margin-bottom:42px; }
    .brand { font-weight:900; letter-spacing:-.03em; font-size:22px; }
    .pill { border:1px solid var(--line); border-radius:999px; padding:10px 14px; color:var(--muted); background:rgba(255,250,243,.72); font-size:13px; }
    .hero {
      display:grid;
      grid-template-columns:1.05fr .95fr;
      gap:26px;
      align-items:start;
    }
    .card {
      background:rgba(255,250,243,.92);
      border:1px solid var(--line);
      box-shadow:0 26px 90px rgba(36,20,15,.12);
      border-radius:28px;
    }
    .hero-copy { padding:44px; min-height:620px; }
    .kicker { color:#8a6330; font-weight:900; text-transform:uppercase; letter-spacing:.12em; font-size:12px; margin-bottom:18px; }
    h1 {
      font-family: Georgia, serif;
      font-size:64px;
      line-height:.96;
      letter-spacing:-.055em;
      margin:0 0 20px;
    }
    .lede { font-size:19px; line-height:1.62; color:var(--muted); max-width:680px; }
    .cta-row { display:flex; gap:12px; flex-wrap:wrap; margin-top:30px; }
    .btn {
      display:inline-block;
      background:var(--dark);
      color:#fff;
      text-decoration:none;
      border-radius:999px;
      padding:15px 20px;
      font-weight:900;
    }
    .btn.secondary { background:transparent; color:var(--dark); border:1px solid var(--line); }
    .proof {
      display:grid;
      grid-template-columns:repeat(3, 1fr);
      gap:12px;
      margin-top:36px;
    }
    .proof div {
      border:1px solid rgba(215,189,146,.8);
      border-radius:18px;
      padding:16px;
      background:#fff7ec;
    }
    .proof strong { display:block; font-size:24px; margin-bottom:5px; }
    .proof span { color:var(--muted); font-size:13px; line-height:1.35; }
    .form-card { padding:28px; }
    .form-title { font-family:Georgia,serif; font-size:32px; margin:0 0 6px; letter-spacing:-.04em; }
    .form-sub { margin:0 0 20px; color:var(--muted); line-height:1.5; }
    label { display:block; font-weight:900; margin-top:15px; font-size:13px; color:#3b241a; }
    input, textarea {
      width:100%;
      margin-top:7px;
      padding:13px 14px;
      border:1px solid var(--line);
      border-radius:14px;
      background:#fff;
      font-size:15px;
      color:var(--ink);
    }
    textarea { min-height:82px; resize:vertical; }
    button {
      width:100%;
      margin-top:22px;
      padding:16px 20px;
      background:var(--dark);
      color:#fff;
      border:0;
      border-radius:999px;
      font-weight:900;
      cursor:pointer;
      font-size:15px;
    }
    .small { font-size:12px; color:var(--muted); margin-top:16px; }
    .section {
      margin-top:24px;
      padding:30px;
    }
    .section h2 {
      font-family:Georgia,serif;
      font-size:36px;
      letter-spacing:-.04em;
      margin:0 0 12px;
    }
    .grid {
      display:grid;
      grid-template-columns:repeat(3, 1fr);
      gap:14px;
      margin-top:18px;
    }
    .mini {
      background:#fff7ec;
      border:1px solid rgba(215,189,146,.8);
      border-radius:20px;
      padding:18px;
    }
    .mini strong { display:block; margin-bottom:8px; }
    .mini p { margin:0; color:var(--muted); line-height:1.5; font-size:14px; }
    @media (max-width:900px) {
      .hero { grid-template-columns:1fr; }
      h1 { font-size:46px; }
      .hero-copy { padding:30px; min-height:auto; }
      .proof, .grid { grid-template-columns:1fr; }
    }
  </style>
</head>
<body>
  <main class="page">
    <nav class="nav">
      <div class="brand">GovRevenue Agent</div>
      <div class="pill">Commercial demand intelligence for UK public-sector revenue</div>
    </nav>

    <section class="hero">
      <div class="card hero-copy">
        <div class="kicker">GovRevenue Scan</div>
        <h1>Find where your business can win before demand reaches Google.</h1>
        <p class="lede">
          GovRevenue Agent scans UK public-sector demand signals, procurement routes and buyer patterns,
          then turns them into a practical commercial report your business can act on.
        </p>

        <div class="cta-row">
          <a class="btn" href="#scan-intake">Run a scan</a>
          <a class="btn secondary" href="/health">Check system health</a>
        </div>

        <div class="proof">
          <div><strong>1</strong><span>Submit your company profile and target services.</span></div>
          <div><strong>2</strong><span>The agent scans opportunity, buyer and route-to-market signals.</span></div>
          <div><strong>3</strong><span>You receive a commercial intelligence report with next actions.</span></div>
        </div>
      </div>

      <div id="scan-intake" class="card form-card">
        <h2 class="form-title">Scan intake</h2>
        <p class="form-sub">Give the agent enough context to judge fit, route and commercial priority.</p>

        <form method="POST" action="/form-submit" autocomplete="off">
          <label>Company name</label>
          <input name="companyName" required />

          <label>Website</label>
          <input name="website" />

          <label>Location / base</label>
          <input name="location" />

          <label>Areas served</label>
          <textarea name="areasServed"></textarea>

          <label>Main services</label>
          <textarea name="mainServices" required></textarea>

          <label>Secondary services</label>
          <textarea name="secondaryServices"></textarea>

          <label>Ideal public-sector buyers</label>
          <textarea name="idealBuyers"></textarea>

          <label>Ideal contract size</label>
          <input name="idealContractSize" />

          <label>Maximum contract size</label>
          <input name="maximumContractSize" />

          <label>Team size</label>
          <input name="teamSize" />

          <label>Public-sector experience</label>
          <input name="publicSectorExperience" />

          <label>Case studies or proof</label>
          <textarea name="caseStudies"></textarea>

          <label>Certifications / policies / accreditations</label>
          <textarea name="certifications"></textarea>

          <label>Services they do NOT want</label>
          <textarea name="excludedServices"></textarea>

          <label>Regions to scan first</label>
          <textarea name="regionsToScan"></textarea>

          <label>Main business goal</label>
          <textarea name="mainGoal"></textarea>

          <label>Biggest concern</label>
          <textarea name="biggestConcern"></textarea>

          <label>Preferred output</label>
          <textarea name="preferredOutput"></textarea>

          <button type="submit">Run GovRevenue Scan</button>
        </form>

        
      </div>
    </section>

    <section class="card section">
      <h2>Built for companies selling into the built environment.</h2>
      <div class="grid">
        <div class="mini"><strong>Demand signals</strong><p>Spot where public money, planning needs and procurement activity point to future demand.</p></div>
        <div class="mini"><strong>Buyer routes</strong><p>Understand whether to pursue tenders, frameworks, subcontracting, partnerships or pre-market positioning.</p></div>
        <div class="mini"><strong>Commercial action</strong><p>Turn scan findings into practical next steps, not generic AI research.</p></div>
      </div>
    </section>
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
</html>`);
});


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

app.get("/scan/:id", asyncRoute(async (req, res) => {
  const scan = await getScan(req.params.id);

  if (!scan) {
    res.status(404).send("Scan not found");
    return;
  }

  res.type("html").send(reportPage(scan));
}));


app.get("/api/scans/:id/report.pdf", asyncRoute(async (req, res) => {
  const scan = await getScan(req.params.id);

  if (!scan || !scan.report_markdown) {
    res.status(404).send("Report not found or not complete yet.");
    return;
  }
let html: string;

const scanData = scan as Record<string, any>;
try {
  const result = generateGovRevenueReport({
  intake: {
    companyName:
      scanData.company_name ??
      scanData.companyName ??
      scanData.name ??
      "Unnamed company",

    website:
      scanData.website ??
      scanData.company_website ??
      undefined,

    sector:
      scanData.sector ??
      scanData.industry ??
      inferGovRevenueSector(scan),

    sectorLens:
      scanData.sector_lens ??
      scanData.sectorLens ??
      undefined,

    baseLocation:
      scanData.location ??
      scanData.base_location ??
      undefined,

    regions:
      toStringArray(scanData.regions ?? scanData.region) ??
      ["West Midlands"],

    services:
      toStringArray(scanData.services ?? scanData.main_services) ??
      [inferGovRevenueSector(scan)],

    secondaryServices:
      toStringArray(scanData.secondary_services) ??
      [],

    excludedServices:
      toStringArray(scanData.excluded_services) ??
      [],

    idealBuyerTypes:
      toStringArray(scanData.ideal_buyer_types) ??
      [],

    idealContractMin:
      toMoney(scanData.ideal_contract_min),

    idealContractMax:
      toMoney(scanData.ideal_contract_max),

    maxDeliverableContractValue:
      toMoney(scanData.max_deliverable_contract_value),

    currentTeamSize:
      toNumber(scanData.current_team_size),

    publicSectorExperience:
      scanData.public_sector_experience ?? undefined,

    accreditations:
      toStringArray(scanData.accreditations) ??
      [],

    insuranceConfirmed:
      Boolean(scanData.insurance_confirmed),

    tupeReady:
      Boolean(scanData.tupe_ready),

    mobilisationReady:
      Boolean(scanData.mobilisation_ready ?? scanData.mobilization_ready),

    caseStudiesConfirmed:
      Boolean(scanData.case_studies_confirmed),

    mainGoal:
      scanData.main_goal ?? undefined,

    biggestConcern:
      scanData.biggest_concern ?? undefined,
  },
  rawRecords: extractRecordsFromScan(scan),
  strict: true,
});

  html = result.html;

  console.log("[GovRevenue QA]", {
    passed: result.qa.passed,
    errors: result.qa.errors,
    warnings: result.qa.warnings,
    dataQuality: result.model.dataQuality.level,
    pulledRecords: result.model.dataQuality.pulledRecords,
    relevantRecords: result.model.dataQuality.relevantRecords,
    noiseRecords: result.model.dataQuality.quarantinedNoiseRecords,
    addressableOpportunityValue: result.model.valueSummary.addressableOpportunityValue,
  });
} catch (error) {
  if (error instanceof GovRevenueQualityGateError) {
    console.error("[GovRevenue blocked PDF export]", error.qa);

    res.status(422).json({
      ok: false,
      blocked: true,
      message: "GovRevenue report blocked by quality gate.",
      qa: error.qa,
    });

    return;
  }

  throw error;
}
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
    } else {
      console.log("[queue] worker disabled by RUN_WORKER=false");
    }

    if (RUN_WEB) {
      app.listen(PORT, () => {
        console.log(`[server] GovRevenue Agent running on port ${PORT}`);
      });
    } else {
      console.log("[server] web disabled by RUN_WEB=false");
    }
  })
  .catch(err => {
    console.error("[startup] failed", err);
    process.exit(1);
  });
function inferGovRevenueSector(scan: any): string {
  const text = [
    scan.company_name,
    scan.companyName,
    scan.sector,
    scan.industry,
    scan.report_markdown,
    scan.services,
    scan.main_services,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  if (
    text.includes("cleaning") ||
    text.includes("deep clean") ||
    text.includes("hygiene") ||
    text.includes("clinical")
  ) {
    return "cleaning";
  }

  return "public-sector services";
}

function extractRecordsFromScan(scan: any): ProcurementRecord[] {
  const arrays = [
    scan.records,
    scan.procurement_records,
    scan.procurementRecords,
    scan.contracts,
    scan.contract_records,
    scan.contractRecords,
    scan.contracts_finder_records,
    scan.contractsFinderRecords,
    scan.open_records,
    scan.openRecords,
    scan.award_records,
    scan.awardRecords,
    scan.matches,
    scan.results,
    scan.data?.records,
    scan.data?.contracts,
    scan.data?.results,
    scan.raw?.records,
    scan.raw?.contracts,
    scan.raw?.results,
  ];

  const combined = arrays.filter(Array.isArray).flat();

  return combined.map((record: any, index: number) => ({
    id: String(
      record.id ??
        record.noticeId ??
        record.notice_id ??
        record.ocid ??
        record.url ??
        record.sourceUrl ??
        `record-${index + 1}`,
    ),
    source: record.source ?? "Contracts Finder",
    sourceUrl: record.sourceUrl ?? record.source_url ?? record.url ?? record.noticeUrl,
    title:
      record.title ??
      record.name ??
      record.noticeTitle ??
      record.notice_title ??
      "Untitled procurement record",
    buyerName:
      record.buyerName ??
      record.buyer_name ??
      record.buyer ??
      record.authority ??
      record.authorityName,
    supplierName:
      record.supplierName ??
      record.supplier_name ??
      record.supplier ??
      record.awardedSupplier,
    status:
      record.status ??
      record.noticeStatus ??
      record.notice_status ??
      record.stage ??
      "unknown",
    description:
      record.description ??
      record.summary ??
      record.details ??
      record.text ??
      "",
    region: record.region ?? record.location ?? record.place,
    publishedDate: record.publishedDate ?? record.published_date,
    deadline: record.deadline ?? record.closeDate ?? record.close_date,
    awardDate: record.awardDate ?? record.award_date,
    startDate: record.startDate ?? record.start_date,
    endDate: record.endDate ?? record.end_date,
    value:
      toMoney(record.value) ??
      toMoney(record.contractValue) ??
      toMoney(record.contract_value) ??
      toMoney(record.awardValue) ??
      toMoney(record.award_value),
    valueLow: toMoney(record.valueLow ?? record.value_low ?? record.minValue),
    valueHigh: toMoney(record.valueHigh ?? record.value_high ?? record.maxValue),
    cpvCodes: toStringArray(record.cpvCodes ?? record.cpv_codes ?? record.cpv) ?? [],
    raw: record,
  }));
}

function toStringArray(value: any): string[] | undefined {
  if (!value) return undefined;

  if (Array.isArray(value)) {
    const cleaned = value.map((item) => String(item ?? "").trim()).filter(Boolean);
    return cleaned.length ? cleaned : undefined;
  }

  if (typeof value === "string") {
    const cleaned = value
      .split(/[,;\n]/g)
      .map((item) => item.trim())
      .filter(Boolean);

    return cleaned.length ? cleaned : undefined;
  }

  return undefined;
}

function toMoney(value: any): number | undefined {
  if (value === undefined || value === null || value === "") return undefined;
  if (typeof value === "number" && Number.isFinite(value)) return value;

  const cleaned = String(value).replace(/[£,\s]/g, "");
  const parsed = Number(cleaned);

  return Number.isFinite(parsed) ? parsed : undefined;
}

function toNumber(value: any): number | undefined {
  if (value === undefined || value === null || value === "") return undefined;

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}
