const BASE = "https://www.googleapis.com/youtube/v3";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type YouTubeVideo = {
  id: string;
  title: string;
  channelTitle: string;
  categoryId: string;
  viewCount: number;
  likeCount: number;
  publishedAt: string;
};

export type YouTubeCategory = {
  id: string;
  title: string;
};

/** Trending UK videos. Requires YOUTUBE_API_KEY env var (free Google Cloud account). */
export async function fetchTrendingVideos(regionCode = "GB", maxResults = 20): Promise<YouTubeVideo[]> {
  const apiKey = process.env.YOUTUBE_API_KEY ?? "";
  if (!apiKey) return [];
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({
      part: "snippet,statistics",
      chart: "mostPopular",
      regionCode,
      maxResults: String(maxResults),
      key: apiKey,
    });
    const res = await fetch(`${BASE}/videos?${params}`, { signal: ac.signal });
    if (!res.ok) return [];
    const data = await res.json() as { items?: Record<string, unknown>[] };
    return (data?.items ?? []).map(v => ({
      id: String(v["id"] ?? ""),
      title: String((v["snippet"] as Record<string, unknown> | undefined)?.["title"] ?? ""),
      channelTitle: String((v["snippet"] as Record<string, unknown> | undefined)?.["channelTitle"] ?? ""),
      categoryId: String((v["snippet"] as Record<string, unknown> | undefined)?.["categoryId"] ?? ""),
      viewCount: Number((v["statistics"] as Record<string, unknown> | undefined)?.["viewCount"] ?? 0),
      likeCount: Number((v["statistics"] as Record<string, unknown> | undefined)?.["likeCount"] ?? 0),
      publishedAt: String((v["snippet"] as Record<string, unknown> | undefined)?.["publishedAt"] ?? ""),
    }));
  } catch { return []; }
}

export async function fetchVideoCategories(regionCode = "GB"): Promise<YouTubeCategory[]> {
  const apiKey = process.env.YOUTUBE_API_KEY ?? "";
  if (!apiKey) return [];
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({ part: "snippet", regionCode, hl: "en_GB", key: apiKey });
    const res = await fetch(`${BASE}/videoCategories?${params}`, { signal: ac.signal });
    if (!res.ok) return [];
    const data = await res.json() as { items?: Record<string, unknown>[] };
    return (data?.items ?? []).map(c => ({
      id: String(c["id"] ?? ""),
      title: String((c["snippet"] as Record<string, unknown> | undefined)?.["title"] ?? ""),
    }));
  } catch { return []; }
}
