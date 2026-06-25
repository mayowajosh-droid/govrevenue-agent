const BASE = "https://wikimedia.org/api/rest_v1/metrics/pageviews";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type WikiPageview = {
  article: string;
  views: number;
  rank: number;
};

/** Top 200 most-viewed English Wikipedia articles for a given date (defaults to yesterday). */
export async function fetchTopUKPageviews(date?: string): Promise<WikiPageview[]> {
  try {
    const d = date ? new Date(date) : new Date(Date.now() - 86_400_000);
    const year = d.getFullYear();
    const month = String(d.getMonth() + 1).padStart(2, "0");
    const day = String(d.getDate()).padStart(2, "0");
    const ac = makeAbort();
    const res = await fetch(`${BASE}/top/en.wikipedia/all-access/${year}/${month}/${day}`, {
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data = await res.json() as { items?: { articles?: { article?: string; views?: number; rank?: number }[] }[] };
    const items = data?.items?.[0]?.articles ?? [];
    return items.slice(0, 200).map(a => ({
      article: String(a.article ?? ""),
      views: Number(a.views ?? 0),
      rank: Number(a.rank ?? 0),
    }));
  } catch { return []; }
}

/** Pageview history for a specific topic — useful for trend detection. */
export async function fetchTopicPageviews(
  article: string,
  start: string,
  end: string,
): Promise<{ date: string; views: number }[]> {
  try {
    const ac = makeAbort();
    const slug = encodeURIComponent(article.replace(/ /g, "_"));
    const res = await fetch(
      `${BASE}/per-article/en.wikipedia/all-access/all-agents/${slug}/daily/${start}/${end}`,
      { signal: ac.signal },
    );
    if (!res.ok) return [];
    const data = await res.json() as { items?: { timestamp?: string; views?: number }[] };
    return (data?.items ?? []).map(i => ({
      date: String(i.timestamp ?? "").slice(0, 8),
      views: Number(i.views ?? 0),
    }));
  } catch { return []; }
}
