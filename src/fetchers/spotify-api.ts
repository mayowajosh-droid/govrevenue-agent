const AUTH_BASE = "https://accounts.spotify.com";
const API_BASE = "https://api.spotify.com/v1";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

let _cachedToken: string | null = null;
let _tokenExpiry = 0;

async function getToken(): Promise<string | null> {
  const clientId = process.env.SPOTIFY_CLIENT_ID ?? "";
  const clientSecret = process.env.SPOTIFY_CLIENT_SECRET ?? "";
  if (!clientId || !clientSecret) return null;
  if (_cachedToken && Date.now() < _tokenExpiry) return _cachedToken;
  try {
    const ac = makeAbort();
    const res = await fetch(`${AUTH_BASE}/api/token`, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Authorization: `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString("base64")}`,
      },
      body: "grant_type=client_credentials",
      signal: ac.signal,
    });
    if (!res.ok) return null;
    const data = await res.json() as { access_token?: string; expires_in?: number };
    _cachedToken = String(data.access_token ?? "");
    _tokenExpiry = Date.now() + (Number(data.expires_in ?? 3600) - 60) * 1000;
    return _cachedToken;
  } catch { return null; }
}

export type SpotifyCategory = {
  id: string;
  name: string;
};

export type SpotifyPlaylist = {
  id: string;
  name: string;
  description: string;
  trackCount: number;
  followers: number;
};

/** Browse music categories popular in the UK. Requires SPOTIFY_CLIENT_ID + SPOTIFY_CLIENT_SECRET. */
export async function fetchSpotifyCategories(country = "GB", limit = 30): Promise<SpotifyCategory[]> {
  const token = await getToken();
  if (!token) return [];
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({ country, limit: String(limit), locale: "en_GB" });
    const res = await fetch(`${API_BASE}/browse/categories?${params}`, {
      headers: { Authorization: `Bearer ${token}` },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data = await res.json() as { categories?: { items?: Record<string, unknown>[] } };
    return (data?.categories?.items ?? []).map(c => ({
      id: String(c["id"] ?? ""),
      name: String(c["name"] ?? ""),
    }));
  } catch { return []; }
}

/** Featured playlists in UK — signals trending genres and moods. */
export async function fetchFeaturedPlaylists(country = "GB", limit = 10): Promise<SpotifyPlaylist[]> {
  const token = await getToken();
  if (!token) return [];
  try {
    const ac = makeAbort();
    const params = new URLSearchParams({ country, limit: String(limit), locale: "en_GB" });
    const res = await fetch(`${API_BASE}/browse/featured-playlists?${params}`, {
      headers: { Authorization: `Bearer ${token}` },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data = await res.json() as { playlists?: { items?: Record<string, unknown>[] } };
    return (data?.playlists?.items ?? []).map(p => ({
      id: String(p["id"] ?? ""),
      name: String(p["name"] ?? ""),
      description: String(p["description"] ?? ""),
      trackCount: Number((p["tracks"] as Record<string, unknown> | undefined)?.["total"] ?? 0),
      followers: Number((p["followers"] as Record<string, unknown> | undefined)?.["total"] ?? 0),
    }));
  } catch { return []; }
}
