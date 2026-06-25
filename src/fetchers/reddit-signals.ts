const BBC_BASE = "https://feeds.bbci.co.uk/news";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type RedditPost = {
  subreddit: string;
  title: string;
  score: number;
  numComments: number;
  url: string;
  created: string;
  flair: string | null;
};

function parseRssItems(xml: string, category: string): RedditPost[] {
  const items: RedditPost[] = [];
  const itemMatches = xml.matchAll(/<item>([\s\S]*?)<\/item>/g);
  for (const m of itemMatches) {
    const block = m[1];
    const title = (block.match(/<title><!\[CDATA\[(.*?)\]\]><\/title>/) ?? block.match(/<title>(.*?)<\/title>/))?.[1] ?? "";
    const link = (block.match(/<link>(.*?)<\/link>/))?.[1] ?? "";
    const pubDate = (block.match(/<pubDate>(.*?)<\/pubDate>/))?.[1] ?? "";
    if (!title) continue;
    items.push({
      subreddit: category,
      title: title.trim(),
      score: 0,
      numComments: 0,
      url: link.trim(),
      created: pubDate ? new Date(pubDate).toISOString() : new Date().toISOString(),
      flair: null,
    });
  }
  return items;
}

/** Fetch trending headlines from a BBC News RSS feed. */
export async function fetchSubredditPosts(
  category: string,
  _sort: "hot" | "rising" | "new" | "top" = "hot",
  limit = 10,
): Promise<RedditPost[]> {
  try {
    const ac = makeAbort();
    const feedPath = category === "UKBusiness" ? "/business"
      : category === "unitedkingdom" ? ""
      : category === "ukpersonalfinance" ? "/business/economy"
      : category === "LegalAdviceUK" ? "/uk"
      : category === "AskUK" ? "/uk"
      : category === "ecommerce" ? "/technology"
      : category === "Entrepreneur" ? "/business"
      : "/business";
    const res = await fetch(`${BBC_BASE}${feedPath}/rss.xml`, {
      headers: { "User-Agent": "AtlasRevenue/1.0 signal-aggregator" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const xml = await res.text();
    return parseRssItems(xml, category).slice(0, limit);
  } catch { return []; }
}

/** Aggregate UK consumer demand signals via BBC News RSS across business, tech, UK feeds. */
export async function fetchUkDemandSignals(): Promise<RedditPost[]> {
  const feeds = [
    { category: "UKBusiness", path: "/business" },
    { category: "unitedkingdom", path: "" },
    { category: "ukpersonalfinance", path: "/business/economy" },
    { category: "LegalAdviceUK", path: "/uk" },
    { category: "ecommerce", path: "/technology" },
    { category: "Entrepreneur", path: "/business/your_money" },
    { category: "AskUK", path: "/uk/england" },
  ];
  const results = await Promise.all(
    feeds.map(async f => {
      try {
        const ac = makeAbort();
        const res = await fetch(`${BBC_BASE}${f.path}/rss.xml`, {
          headers: { "User-Agent": "AtlasRevenue/1.0 signal-aggregator" },
          signal: ac.signal,
        });
        if (!res.ok) return [];
        const xml = await res.text();
        return parseRssItems(xml, f.category).slice(0, 5);
      } catch { return []; }
    }),
  );
  return results.flat();
}
