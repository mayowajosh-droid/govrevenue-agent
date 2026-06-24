const RSS_URL =
  "https://www.sell2wales.gov.wales/rss/rss_switch.aspx?Type=2&ID=&Status=0";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type Sell2WalesNotice = {
  title: string;
  link: string;
  description: string;
  pubDate: string;
  buyer: string | null;
  category: string | null;
};

/** Extract text content between an XML open/close tag pair. */
function tag(xml: string, name: string): string {
  const open = `<${name}>`;
  const openCdata = `<${name}><![CDATA[`;
  const close = `</${name}>`;

  const start = xml.indexOf(open);
  if (start === -1) return "";

  const afterOpen = start + open.length;
  const end = xml.indexOf(close, afterOpen);
  if (end === -1) return "";

  let value = xml.slice(afterOpen, end).trim();

  // Strip CDATA wrapper if present
  if (value.startsWith("<![CDATA[") && value.endsWith("]]>")) {
    value = value.slice(9, -3).trim();
  }

  return value;
}

/** Try to extract a buyer name from the description text. */
function extractBuyer(desc: string): string | null {
  // Common patterns: "Organisation: Foo Council" or "Buyer: Foo"
  const patterns = [
    /(?:organisation|buyer|contracting authority|awarding body)\s*:\s*(.+?)(?:\n|<br|$)/i,
    /(?:published by|issued by|on behalf of)\s+(.+?)(?:\.|,|\n|<br|$)/i,
  ];
  for (const re of patterns) {
    const m = desc.match(re);
    if (m?.[1]) {
      const cleaned = m[1].replace(/<[^>]*>/g, "").trim();
      if (cleaned.length > 2 && cleaned.length < 200) return cleaned;
    }
  }
  return null;
}

/** Try to extract a category from the description text. */
function extractCategory(desc: string): string | null {
  const m = desc.match(
    /(?:category|cpv|type of contract|contract type)\s*:\s*(.+?)(?:\n|<br|$)/i,
  );
  if (m?.[1]) {
    const cleaned = m[1].replace(/<[^>]*>/g, "").trim();
    if (cleaned.length > 1 && cleaned.length < 200) return cleaned;
  }
  return null;
}

/**
 * Fetch current Sell2Wales procurement notices from the RSS feed.
 * Returns an empty array on any failure.
 */
export async function fetchSell2WalesNotices(): Promise<Sell2WalesNotice[]> {
  try {
    const ac = makeAbort();
    const res = await fetch(RSS_URL, {
      headers: { Accept: "application/rss+xml, application/xml, text/xml" },
      signal: ac.signal,
    });
    if (!res.ok) return [];

    const xml = await res.text();

    const notices: Sell2WalesNotice[] = [];
    let cursor = 0;

    while (true) {
      const itemStart = xml.indexOf("<item>", cursor);
      if (itemStart === -1) break;

      const itemEnd = xml.indexOf("</item>", itemStart);
      if (itemEnd === -1) break;

      const item = xml.slice(itemStart, itemEnd + 7);
      cursor = itemEnd + 7;

      const title = tag(item, "title");
      const link = tag(item, "link");
      const description = tag(item, "description");
      const pubDate = tag(item, "pubDate");

      if (!title && !link) continue;

      notices.push({
        title,
        link,
        description,
        pubDate,
        buyer: extractBuyer(description),
        category: extractCategory(description),
      });
    }

    return notices;
  } catch {
    return [];
  }
}
