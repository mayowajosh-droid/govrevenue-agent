const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type BbcNewsItem = {
  title: string;
  link: string;
  description: string;
  pubDate: string;
  guid: string | null;
};

function extractTag(xml: string, tag: string): string {
  const open = `<${tag}>`;
  const openCdata = `<${tag}><![CDATA[`;
  const close = `</${tag}>`;
  let start = xml.indexOf(openCdata);
  if (start !== -1) {
    start += openCdata.length;
    const end = xml.indexOf("]]>", start);
    return end !== -1 ? xml.slice(start, end).trim() : "";
  }
  start = xml.indexOf(open);
  if (start === -1) return "";
  start += open.length;
  const end = xml.indexOf(close, start);
  return end !== -1 ? xml.slice(start, end).trim() : "";
}

function parseRssItems(xml: string): BbcNewsItem[] {
  const items: BbcNewsItem[] = [];
  const itemRegex = /<item>([\s\S]*?)<\/item>/g;
  let match: RegExpExecArray | null;
  while ((match = itemRegex.exec(xml)) !== null) {
    const block = match[1];
    items.push({
      title: extractTag(block, "title"),
      link: extractTag(block, "link") || extractTag(block, "guid"),
      description: extractTag(block, "description"),
      pubDate: extractTag(block, "pubDate"),
      guid: extractTag(block, "guid") || null,
    });
  }
  return items.filter(i => i.title);
}

async function fetchFeed(feedUrl: string): Promise<BbcNewsItem[]> {
  try {
    const ac = makeAbort();
    const res = await fetch(feedUrl, {
      headers: { Accept: "application/rss+xml, application/xml, text/xml" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const xml = await res.text();
    return parseRssItems(xml);
  } catch { return []; }
}

export async function fetchBbcBusinessNews(): Promise<BbcNewsItem[]> {
  return fetchFeed("https://feeds.bbci.co.uk/news/business/rss.xml");
}

export async function fetchBbcPoliticsNews(): Promise<BbcNewsItem[]> {
  return fetchFeed("https://feeds.bbci.co.uk/news/politics/rss.xml");
}

export async function fetchBbcUkNews(): Promise<BbcNewsItem[]> {
  return fetchFeed("https://feeds.bbci.co.uk/news/uk/rss.xml");
}

export async function fetchBbcTechnologyNews(): Promise<BbcNewsItem[]> {
  return fetchFeed("https://feeds.bbci.co.uk/news/technology/rss.xml");
}
