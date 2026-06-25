const BASE = "https://www.eventbriteapi.com/v3";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type EventbriteEvent = {
  id: string;
  name: string;
  description: string;
  start: string;
  end: string;
  category: string;
  city: string;
  url: string;
  isFree: boolean;
  capacity: number | null;
};

/** Search Eventbrite events in the UK. Requires EVENTBRITE_API_KEY env var (free). */
export async function searchEvents(query: string, city?: string): Promise<EventbriteEvent[]> {
  const token = process.env.EVENTBRITE_API_KEY ?? "";
  if (!token) return [];
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({
      q: query,
      "location.address": city ?? "United Kingdom",
      "location.within": "100km",
      expand: "venue,category",
      "page_size": "20",
    });
    const res = await fetch(`${BASE}/events/search/?${params}`, {
      headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data = await res.json() as { events?: Record<string, unknown>[] };
    return (data?.events ?? []).map(e => ({
      id: String(e["id"] ?? ""),
      name: String((e["name"] as Record<string, unknown> | undefined)?.["text"] ?? ""),
      description: String((e["description"] as Record<string, unknown> | undefined)?.["text"] ?? "").slice(0, 300),
      start: String((e["start"] as Record<string, unknown> | undefined)?.["utc"] ?? ""),
      end: String((e["end"] as Record<string, unknown> | undefined)?.["utc"] ?? ""),
      category: String((e["category"] as Record<string, unknown> | undefined)?.["name"] ?? ""),
      city: String(((e["venue"] as Record<string, unknown> | undefined)?.["address"] as Record<string, unknown> | undefined)?.["city"] ?? ""),
      url: String(e["url"] ?? ""),
      isFree: Boolean(e["is_free"]),
      capacity: e["capacity"] != null ? Number(e["capacity"]) : null,
    }));
  } catch { return []; }
}

export async function fetchUkEvents(): Promise<EventbriteEvent[]> {
  const topics = ["business", "food", "music", "fashion", "technology", "health"];
  const results = await Promise.all(topics.map(t => searchEvents(t)));
  return results.flat();
}
