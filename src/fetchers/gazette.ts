const GAZETTE_BASE = "https://www.thegazette.co.uk";
const TIMEOUT_MS = 15_000;
const DEFAULT_LIMIT = 50;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type GazetteNotice = {
  id: string;
  title: string;
  link: string;
  category: string | null;
  publishedDate: string;
  description: string | null;
  noticeType: string | null;
};

// --- JSON approach (primary) ---

type JsonNoticeItem = {
  "notice-code"?: string;
  title?: string;
  link?: string | Array<{ "@href"?: string; "@rel"?: string }>;
  category?: string | { "@term"?: string };
  published?: string;
  updated?: string;
  content?: string | { "#text"?: string };
  id?: string;
};

type GazetteJsonResponse = {
  "notice-list"?: {
    notice?: JsonNoticeItem[] | JsonNoticeItem;
  };
};

function noticeTypeFromCode(code: string | undefined): string | null {
  if (!code) return null;
  const c = code.toLowerCase();
  if (c.includes("insolvenc") || c.includes("winding")) return "insolvency";
  if (c.includes("procurement") || c.includes("contract")) return "procurement";
  if (c.includes("planning")) return "planning";
  if (c.includes("compan")) return "company";
  return code;
}

function parseJsonNotices(body: GazetteJsonResponse): GazetteNotice[] {
  const list = body["notice-list"];
  if (!list) return [];

  const raw = list.notice;
  const items: JsonNoticeItem[] = Array.isArray(raw)
    ? raw
    : raw
      ? [raw]
      : [];

  return items
    .map((n): GazetteNotice | null => {
      let link = "";
      if (typeof n.link === "string") {
        link = n.link;
      } else if (Array.isArray(n.link)) {
        const self = n.link.find(
          (l) => l["@rel"] === "self" || l["@rel"] === "alternate",
        );
        link = self?.["@href"] || n.link[0]?.["@href"] || "";
      }

      const id =
        n.id || (link ? link.replace(/.*\/notice\//, "").replace(/\?.*/, "") : "");
      if (!id) return null;

      const cat =
        typeof n.category === "string"
          ? n.category
          : n.category?.["@term"] || null;

      const desc =
        typeof n.content === "string"
          ? n.content
          : n.content?.["#text"] || null;

      return {
        id: String(id),
        title: String(n.title || ""),
        link: link.startsWith("http") ? link : `${GAZETTE_BASE}${link}`,
        category: cat,
        publishedDate: String(n.published || n.updated || ""),
        description: desc ? String(desc).slice(0, 500) : null,
        noticeType: noticeTypeFromCode(n["notice-code"] || cat || undefined),
      };
    })
    .filter((n): n is GazetteNotice => n !== null);
}

// --- Atom feed fallback ---

function tag(xml: string, name: string): string {
  const open = `<${name}`;
  const start = xml.indexOf(open);
  if (start === -1) return "";

  // Find end of opening tag (handles attributes)
  const tagClose = xml.indexOf(">", start + open.length);
  if (tagClose === -1) return "";

  const afterOpen = tagClose + 1;
  const close = `</${name}>`;
  const end = xml.indexOf(close, afterOpen);
  if (end === -1) return "";

  let value = xml.slice(afterOpen, end).trim();
  if (value.startsWith("<![CDATA[") && value.endsWith("]]>")) {
    value = value.slice(9, -3).trim();
  }
  return value;
}

function attrValue(element: string, attr: string): string {
  const re = new RegExp(`${attr}\\s*=\\s*["']([^"']*)["']`);
  const m = element.match(re);
  return m?.[1] || "";
}

function parseAtomNotices(xml: string): GazetteNotice[] {
  const notices: GazetteNotice[] = [];
  let cursor = 0;

  while (true) {
    const entryStart = xml.indexOf("<entry", cursor);
    if (entryStart === -1) break;

    const entryEnd = xml.indexOf("</entry>", entryStart);
    if (entryEnd === -1) break;

    const entry = xml.slice(entryStart, entryEnd + 8);
    cursor = entryEnd + 8;

    const id = tag(entry, "id");
    const title = tag(entry, "title");
    const published = tag(entry, "published") || tag(entry, "updated");

    // Get link href from <link> element attributes
    const linkMatch = entry.match(/<link[^>]*rel\s*=\s*["']alternate["'][^>]*>/);
    const linkEl = linkMatch?.[0] || "";
    let link = attrValue(linkEl, "href");
    if (!link) {
      const anyLink = entry.match(/<link[^>]*href\s*=\s*["']([^"']*)["'][^>]*>/);
      link = anyLink?.[1] || "";
    }

    const catMatch = entry.match(/<category[^>]*term\s*=\s*["']([^"']*)["'][^>]*>/);
    const category = catMatch?.[1] || null;

    const content = tag(entry, "content") || tag(entry, "summary");

    if (!id && !title) continue;

    const cleanId = id
      ? id.replace(/.*\/notice\//, "").replace(/\?.*/, "")
      : "";

    notices.push({
      id: cleanId || title.slice(0, 60),
      title,
      link: link.startsWith("http") ? link : link ? `${GAZETTE_BASE}${link}` : "",
      category,
      publishedDate: published,
      description: content ? content.replace(/<[^>]*>/g, "").slice(0, 500) : null,
      noticeType: noticeTypeFromCode(category || undefined),
    });
  }

  return notices;
}

/**
 * Search the London Gazette for notices matching a query.
 * Tries JSON endpoint first, falls back to Atom feed parsing.
 */
export async function searchGazetteNotices(
  query: string,
  limit?: number,
): Promise<GazetteNotice[]> {
  const pageSize = Math.min(limit || DEFAULT_LIMIT, 100);
  const q = query.trim();
  if (!q) return [];

  // Try JSON first
  try {
    const url = new URL(`${GAZETTE_BASE}/all-notices/content/data.json`);
    url.searchParams.set("text", q);
    url.searchParams.set("results-page-size", String(pageSize));

    const ac = makeAbort();
    const res = await fetch(url.toString(), {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });

    if (res.ok) {
      const contentType = res.headers.get("content-type") || "";
      if (contentType.includes("json")) {
        const body = (await res.json()) as GazetteJsonResponse;
        const notices = parseJsonNotices(body);
        if (notices.length > 0) return notices;
      }
    }
  } catch {
    // Fall through to Atom
  }

  // Atom feed fallback
  try {
    const url = new URL(`${GAZETTE_BASE}/all-notices/notice`);
    url.searchParams.set("text", q);
    url.searchParams.set("results-page-size", String(pageSize));
    url.searchParams.set("categorycode-all", "all");

    const ac = makeAbort();
    const res = await fetch(url.toString(), {
      headers: { Accept: "application/atom+xml, application/xml" },
      signal: ac.signal,
    });
    if (!res.ok) return [];

    const xml = await res.text();
    return parseAtomNotices(xml);
  } catch {
    return [];
  }
}

/** Fetch recent procurement-related Gazette notices. */
export async function fetchRecentProcurementNotices(): Promise<GazetteNotice[]> {
  return searchGazetteNotices("CONTRACT procurement");
}

/** Fetch recent insolvency notices — useful for supplier risk intelligence. */
export async function fetchRecentInsolvencyNotices(): Promise<GazetteNotice[]> {
  return searchGazetteNotices("insolvency winding-up");
}
