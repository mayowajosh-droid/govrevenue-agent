# GovRevenue Agent — Project Instructions

UK public-sector procurement intelligence tool. Companies submit a profile → agent scans Contracts Finder + Find a Tender → LLM generates a structured commercial report → PDF export.

**Live URL:** https://govrevenue-agent-production.up.railway.app
**GitHub:** mayowajosh-droid/govrevenue-agent (main branch → Railway auto-deploy)

---

## Stack

- Node 22 + TypeScript ESM + Express
- PostgreSQL (Railway) with in-memory Map fallback
- BullMQ + Redis for scan queue (in-process fallback)
- Report generation: **Claude** (`@anthropic-ai/sdk`, `claude-opus-4-8` default) with server-side web_search — primary when `ANTHROPIC_API_KEY` is set; **OpenAI** (`openai.responses.create()` with web_search) is the automatic fallback
- Keyword generation: OpenAI `gpt-4.1-mini` chat completions
- Puppeteer for PDF (A4)
- Resend for email notifications
- AWS S3-compatible PDF storage (optional)
- Sentry for error tracking (optional)

---

## Commands

```bash
npm run dev      # tsx dev server, no build required
npm run build    # tsc → dist/
npm start        # node dist/index.js
```

After any code change: `npm run build` must pass clean before committing. Zero tolerance for TypeScript errors.

---

## File map

| File | What it does |
|---|---|
| `src/index.ts` | Everything: routes, data pull, scoring, HTML rendering, PDF, worker (~7,500 lines) |
| `src/designEngine.ts` | ECharts SSR dashboard SVGs rendered into reports |
| `src/lib/govrevenue/govrevenue-report-engine.ts` | Imported but NOT called — do not delete, do not call without understanding it first |
| `src/lib/pdfStorage.ts` | S3 PDF upload |
| `src/lib/emailNotifications.ts` | Resend scan-complete/scan-failed emails |

---

## Architecture rules — do not break these

### EDP is the single source of truth for Page 1
`parseEdpFromMarkdown()` extracts Verdict, Evidence Grade, Can they win now?, Recommended route, etc. from the LLM report markdown. These values drive all Page 1 metric cards. Never use `data.quality.level` or any score function for Page 1 values — that was the dual-source bug that caused wrong verdicts.

### LLM keyword generation is primary
`generateSearchKeywords()` calls `gpt-4.1-mini` to produce accurate Contracts Finder search terms. Falls back to `buildKeywords()` only on failure. Never swap this order. The static `buildKeywords()` causes sector mismatches; LLM keywords are semantically accurate.

### Report generation is provider-abstracted (Claude primary, OpenAI fallback)
`callLlmReport(prompt)` is the single report chokepoint. If `anthropic` is configured (env `ANTHROPIC_API_KEY`), it calls `callClaudeReport()` (Anthropic Messages API + `web_search_20250305` tool, 150s timeout); on any error it logs to Sentry and falls back to `callOpenAiReport()` (the original `responses.create` path, 90s). The same `buildPrompt()` output drives both — it ends with "Return clean Markdown only" + the exact 10-section structure, so `parseEdpFromMarkdown()` works identically regardless of provider. Do not give Claude a different prompt or structure. `/health` reports the active `reportProvider`/`reportModel`.

### PDF consistency guard is mandatory
`validateReportConsistency()` checks for conflicting grades/verdicts before PDF export. Returns HTTP 422 if invalid. Never bypass or remove this check.

### Title-only matching for live signals
`deskPage` live signal filter, `subPage` open notices, and `refreshHomepageSignals()` all use title-only keyword matching. Description-level matching caused false positives (home care on Facilities desk, property consultancy on Energy desk, landscaping on Finance desk). Do not revert.

### CPV search is supplementary
`SECTOR_CPV` map runs a parallel CPV-code pass on Contracts Finder after the keyword loop. Kept intentionally tight — broad CPV codes cause noisy cross-sector matches. Do not widen without testing against known-good results.

### OpenAI calls have a 90s timeout
`withOpenAiTimeout()` wraps all OpenAI calls with an `AbortController` at 90 seconds to prevent queue stalls. Do not remove or bypass.

### Report structure (10 sections — do not reorder or drop)
1. Executive Decision Panel
2. Evidence Grade and Scan Basis
3. Intelligence Dashboard Summary
4. Source-Backed Evidence
5. Money Map: Best Routes to Revenue
6. Buyer Watchlist
7. Bid Readiness Score
8. Do Not Chase These Yet
9. 30-Day Activation Pack
10. QA Notes / Integrity Checks

---

## Known debt — don't make worse, fix when touching

- **`govrevenue-report-engine.ts` unused** — Stage A migration target: use its scorer as preprocessing before `buildPrompt()` so the LLM gets structured data, not raw notices. Stage B: replace `reportPage()` with a typed struct renderer. Do not call it without reading it first.
- **Desk page data visualisation gap** — desk pages have no analytical charts (spend by buyer, open vs awarded breakdown, category trend). This is the biggest remaining product gap; treat it as a feature build, not a quick fix.
- **Short keyword substring matching** — keywords ≤4 chars ("erp", "soc", "mis") match substrings in unrelated words. Partially addressed by expanding specific terms. Remaining fix: word-boundary regex for short keywords.

---

## Env vars

| Var | Required | Notes |
|---|---|---|
| `DATABASE_URL` | Yes (prod) | PostgreSQL — falls back to memory store |
| `REDIS_URL` | Yes (prod) | Redis — falls back to in-process scan |
| `OPENAI_API_KEY` | Yes | OpenAI — report fallback + keyword generation |
| `OPENAI_MODEL` | No | Defaults to `gpt-4.1-mini` |
| `ANTHROPIC_API_KEY` | No | Enables Claude as primary report generator (recommended — the report is the product) |
| `ANTHROPIC_MODEL` | No | Defaults to `claude-opus-4-8`. Set to `claude-sonnet-4-6` for faster/cheaper reports |
| `ADMIN_TOKEN` | Yes | Protects `/admin/*` routes |
| `COMPANIES_HOUSE_API_KEY` | No | Companies House search |
| `SENTRY_DSN` | No | Error tracking |
| `PORT` | No | Defaults to 3000 |
| `RUN_WEB` | No | Defaults to true |
| `RUN_WORKER` | No | Defaults to true |
| `RESEND_API_KEY` | No | Email notifications |
| `FROM_EMAIL` | No | Sender address for emails |
| `SLACK_WEBHOOK_URL` | No | Opportunity bot — pushes newly-discovered signals to a Slack/Discord incoming webhook each hourly refresh |
| `SAMPLE_PDF_URL` | No | Public sample PDF shown on homepage; never serves real customer PDFs |
| `BASE_URL` | No | Base URL for scan links in emails |

PDF storage (all optional, enables S3 upload):
`PDF_STORAGE_ENDPOINT`, `PDF_STORAGE_BUCKET`, `PDF_STORAGE_ACCESS_KEY_ID`, `PDF_STORAGE_SECRET_ACCESS_KEY`, `PDF_STORAGE_PUBLIC_BASE_URL`

---

## Data sources

- **Contracts Finder API v2** — `/api/rest/2/search_notices/json` — primary procurement data
- **Find a Tender OCDS** — `/api/1.0/ocdsReleasePackages` — above-threshold tenders
- **Companies House** — `/search/companies` — company profile verification

Searches are keyword-driven with a parallel CPV-code pass per sector (`SECTOR_CPV` map).

---

## Product roadmap

| Status | Feature |
|---|---|
| ✅ Done | Weekly opportunity alerts — BullMQ repeat job, email diff of new notices |
| ✅ Done | Scan comparison — `GET /scan/:id/compare`, diffs verdict/grade/buyers |
| ✅ Done | Competitor intelligence — incumbent map table in completed scan result page |
| ✅ Done | CPV code search layer — parallel CPV pass on Contracts Finder per sector |
| ✅ Done | SSE scan progress — live stage UI via EventEmitter + DB poll fallback |
| Next | **Framework pre-qualification assistant** — identify open frameworks, eligibility check, checklist |
| Next | **Scan-to-bid-pack** — capability statement + outreach email from top 3 routes |
| Next | **Desk page data visualisation** — spend trends, buyer concentration, category movement charts |
| Future | **govrevenue-report-engine Stage A** — wire engine scorer as preprocessing before LLM prompt |

---

## Coding rules

- TypeScript strict mode. Build must pass clean — no `any` unless explicitly justified.
- No comments on obvious code. Add a comment only when the WHY is non-obvious.
- No new duplicate functions. Before adding a function, search for existing ones.
- HTML is rendered server-side as template strings. No frontend framework.
- `escapeHtml()` on every user-derived value before inserting into HTML. No exceptions.
- Security: never expose `ADMIN_TOKEN` in HTML, never log full scan inputs if they contain PII.
- When changing report structure, update both `buildPrompt()` and `reportPage()` — they must stay in sync.
- After any change to `reportPage()`, verify `parseEdpFromMarkdown()` still extracts correctly.
- 404 responses for HTML routes use `notFoundHtml()` — never `res.send("raw text")`. API routes use `res.json({ error: "..." })`.
