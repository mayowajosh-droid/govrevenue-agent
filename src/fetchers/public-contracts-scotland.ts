// Public Contracts Scotland — RSS feed (no key required)
// https://www.publiccontractsscotland.gov.uk

const FEED_URL = "https://www.publiccontractsscotland.gov.uk/NoticeSearchResults.aspx?NoticeType=3&Format=RSS";
const TIMEOUT_MS = 15_000;

export type PcsNotice = {
  title: string;
  buyer: string | null;
  description: string | null;
  publishedDate: string | null;
  deadlineDate: string | null;
  url: string | null;
  region: "Scotland";
};

function extractTag(xml: string, tag: string): string {
  const m = xml.match(new RegExp(`<${tag}[^>]*>(?:<!\\[CDATA\\[)?(.*?)(?:\\]\\]>)?</${tag}>`, "s"));
  return m ? m[1].trim() : "";
}

export async function fetchPcsNotices(): Promise<PcsNotice[]> {
  try {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
    let xml: string;
    try {
      const resp = await fetch(FEED_URL, {
        headers: { "User-Agent": "AtlasRevenue/1.0 (procurement intelligence)" },
        signal: ctrl.signal,
      });
      if (!resp.ok) return [];
      xml = await resp.text();
    } finally {
      clearTimeout(timer);
    }

    const items = xml.match(/<item>([\s\S]*?)<\/item>/g) || [];
    return items.slice(0, 50).map(item => ({
      title: extractTag(item, "title") || "Untitled",
      buyer: extractTag(item, "author") || extractTag(item, "dc:creator") || null,
      description: extractTag(item, "description") || null,
      publishedDate: extractTag(item, "pubDate") || null,
      deadlineDate: null,
      url: extractTag(item, "link") || null,
      region: "Scotland" as const,
    })).filter(n => n.title && n.title !== "Untitled");
  } catch {
    return [];
  }
}
