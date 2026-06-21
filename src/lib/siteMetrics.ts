import { Pool } from 'pg';

export interface SiteMetrics {
  noticesIndexedTotal: number;
  desksLive: number;
  buyersTracked: number;
  valueIndexedTotal: number;       // £
  valueAwarded12m: number;         // £, excl >£2bn outliers
  valueOpenPipeline: number;       // £
  marketContextAnnual: number;     // £, external constant — cite source inline
  openNow: number;                 // deadline >= today
  closing7d: number;
  closing14d: number;
  closing30d: number;
  closing60d: number;
  awardedCountTotal: number;
  monthlyAwardedSeries: { month: string; value: number }[];
  momentum3m: number;              // signed %
  peakMonth: { month: string; value: number; vsAvgPct: number };
}

export interface DeskMetrics {
  valueAwarded12m: number;
  awardedCount12m: number;
  openNow: number;
  closing7d: number;
  closing30d: number;
  momentum3m: number;
  monthlyAwardedSeries: { month: string; value: number }[];
  avgContractValue: number | null;
}

type CachedNotice = {
  awardedDate?: string | null;
  deadlineDate?: string | null;
  publishedDate?: string | null;
  awardedValue?: number | null;
};

type CachedDeskData = {
  contractsFinder: {
    open: CachedNotice[];
    awarded: CachedNotice[];
  };
};

const OUTLIER_CAP = 2_000_000_000;
const MARKET_CONTEXT_ANNUAL = 400_000_000_000;
const METRICS_TTL_MS = 60 * 60 * 1000;

let _cache: { data: SiteMetrics; ts: number } | null = null;

const EMPTY_METRICS: SiteMetrics = {
  noticesIndexedTotal: 0,
  desksLive: 0,
  buyersTracked: 0,
  valueIndexedTotal: 0,
  valueAwarded12m: 0,
  valueOpenPipeline: 0,
  marketContextAnnual: MARKET_CONTEXT_ANNUAL,
  openNow: 0,
  closing7d: 0,
  closing14d: 0,
  closing30d: 0,
  closing60d: 0,
  awardedCountTotal: 0,
  monthlyAwardedSeries: [],
  momentum3m: 0,
  peakMonth: { month: '—', value: 0, vsAvgPct: 0 },
};

export function invalidateSiteMetrics(): void {
  _cache = null;
}

export async function getSiteMetrics(pool: Pool | null, desksLive: number): Promise<SiteMetrics> {
  if (_cache && Date.now() - _cache.ts < METRICS_TTL_MS) {
    return { ..._cache.data, desksLive };
  }
  if (!pool) return { ...EMPTY_METRICS, desksLive };

  const [coverageR, seriesR] = await Promise.all([
    pool.query<{
      notices_total: string;
      buyers_tracked: string;
      value_indexed: string;
      value_awarded_12m: string;
      value_open_pipeline: string;
      open_now: string;
      closing_7d: string;
      closing_14d: string;
      closing_30d: string;
      closing_60d: string;
      awarded_total: string;
    }>(`
      SELECT
        COUNT(*)::text AS notices_total,
        COUNT(DISTINCT buyer) FILTER (WHERE buyer IS NOT NULL AND buyer <> '')::text AS buyers_tracked,
        COALESCE(SUM(value_amount) FILTER (WHERE value_amount > 0), 0)::text AS value_indexed,
        COALESCE(SUM(value_amount) FILTER (WHERE
          (LOWER(status) LIKE '%award%')
          AND notice_date > NOW() - INTERVAL '12 months'
          AND value_amount > 0
          AND value_amount <= ${OUTLIER_CAP}
        ), 0)::text AS value_awarded_12m,
        COALESCE(SUM(value_amount) FILTER (WHERE
          (LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')
          AND deadline_date >= NOW()
          AND value_amount > 0
        ), 0)::text AS value_open_pipeline,
        COUNT(*) FILTER (WHERE
          (LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')
          AND deadline_date >= NOW()
        )::text AS open_now,
        COUNT(*) FILTER (WHERE
          (LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')
          AND deadline_date BETWEEN NOW() AND NOW() + INTERVAL '7 days'
        )::text AS closing_7d,
        COUNT(*) FILTER (WHERE
          (LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')
          AND deadline_date BETWEEN NOW() AND NOW() + INTERVAL '14 days'
        )::text AS closing_14d,
        COUNT(*) FILTER (WHERE
          (LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')
          AND deadline_date BETWEEN NOW() AND NOW() + INTERVAL '30 days'
        )::text AS closing_30d,
        COUNT(*) FILTER (WHERE
          (LOWER(status) LIKE '%open%' OR LOWER(status) LIKE '%active%')
          AND deadline_date BETWEEN NOW() AND NOW() + INTERVAL '60 days'
        )::text AS closing_60d,
        COUNT(*) FILTER (WHERE LOWER(status) LIKE '%award%')::text AS awarded_total
      FROM homepage_signals
    `),
    pool.query<{ month: string; value_m: number }>(`
      SELECT
        to_char(date_trunc('month', notice_date), 'Mon ''YY') AS month,
        ROUND(SUM(value_amount) / 1e6::numeric, 2)::float AS value_m
      FROM homepage_signals
      WHERE
        (LOWER(status) LIKE '%award%')
        AND notice_date > NOW() - INTERVAL '13 months'
        AND notice_date <= NOW()
        AND notice_date IS NOT NULL
        AND value_amount > 0
        AND value_amount <= ${OUTLIER_CAP}
      GROUP BY date_trunc('month', notice_date)
      ORDER BY date_trunc('month', notice_date)
    `),
  ]);

  const c = coverageR.rows[0] || {};
  const series: { month: string; value: number }[] = seriesR.rows.map(r => ({
    month: r.month,
    value: r.value_m || 0,
  }));

  const avg12m = series.length > 0 ? series.reduce((s, p) => s + p.value, 0) / series.length : 0;
  const first3Avg = series.length >= 3 ? series.slice(0, 3).reduce((s, p) => s + p.value, 0) / 3 : avg12m;
  const last3Avg = series.length >= 3 ? series.slice(-3).reduce((s, p) => s + p.value, 0) / 3 : avg12m;
  const momentum3m = first3Avg > 0 ? Math.round(((last3Avg - first3Avg) / first3Avg) * 100) : 0;

  const peakPoint = series.reduce(
    (best, p) => (p.value > best.value ? p : best),
    series[0] || { month: '—', value: 0 }
  );
  const vsAvgPct = avg12m > 0 ? Math.round(((peakPoint.value - avg12m) / avg12m) * 100) : 0;

  const data: SiteMetrics = {
    noticesIndexedTotal: parseInt(c.notices_total ?? '0'),
    desksLive,
    buyersTracked: parseInt(c.buyers_tracked ?? '0'),
    valueIndexedTotal: parseFloat(c.value_indexed ?? '0'),
    valueAwarded12m: parseFloat(c.value_awarded_12m ?? '0'),
    valueOpenPipeline: parseFloat(c.value_open_pipeline ?? '0'),
    marketContextAnnual: MARKET_CONTEXT_ANNUAL,
    openNow: parseInt(c.open_now ?? '0'),
    closing7d: parseInt(c.closing_7d ?? '0'),
    closing14d: parseInt(c.closing_14d ?? '0'),
    closing30d: parseInt(c.closing_30d ?? '0'),
    closing60d: parseInt(c.closing_60d ?? '0'),
    awardedCountTotal: parseInt(c.awarded_total ?? '0'),
    monthlyAwardedSeries: series,
    momentum3m,
    peakMonth: { month: peakPoint.month, value: peakPoint.value, vsAvgPct },
  };

  _cache = { data, ts: Date.now() };
  return { ...data, desksLive };
}

export function getDeskMetrics(cached: { data: CachedDeskData } | null): DeskMetrics {
  const EMPTY: DeskMetrics = {
    valueAwarded12m: 0,
    awardedCount12m: 0,
    openNow: 0,
    closing7d: 0,
    closing30d: 0,
    momentum3m: 0,
    monthlyAwardedSeries: [],
    avgContractValue: null,
  };

  if (!cached) return EMPTY;

  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const twelveMonthsAgo = new Date(today);
  twelveMonthsAgo.setFullYear(twelveMonthsAgo.getFullYear() - 1);
  const sevenDaysOut = new Date(today.getTime() + 7 * 24 * 60 * 60 * 1000);
  const thirtyDaysOut = new Date(today.getTime() + 30 * 24 * 60 * 60 * 1000);

  const openNowNotices = cached.data.contractsFinder.open.filter(n =>
    !n.deadlineDate || new Date(n.deadlineDate) >= today
  );

  const awarded12m = cached.data.contractsFinder.awarded.filter(n => {
    const dateStr = n.awardedDate || n.publishedDate;
    if (!dateStr) return false;
    return new Date(dateStr) >= twelveMonthsAgo;
  });

  const valueAwarded12m = awarded12m.reduce((s, n) => {
    const v = n.awardedValue ?? 0;
    return s + (v > 0 && v <= OUTLIER_CAP ? v : 0);
  }, 0);

  const awardedWithValue = awarded12m.filter(n => (n.awardedValue ?? 0) > 0);
  const avgContractValue = awardedWithValue.length > 0 ? valueAwarded12m / awardedWithValue.length : null;

  const closing7d = openNowNotices.filter(n =>
    n.deadlineDate && new Date(n.deadlineDate) <= sevenDaysOut
  ).length;
  const closing30d = openNowNotices.filter(n =>
    n.deadlineDate && new Date(n.deadlineDate) <= thirtyDaysOut
  ).length;

  const monthMap = new Map<string, number>();
  for (const n of awarded12m) {
    const dateStr = n.awardedDate || n.publishedDate;
    if (!dateStr) continue;
    const d = new Date(dateStr);
    const key = d.toLocaleDateString('en-GB', { month: 'short', year: '2-digit' });
    const v = n.awardedValue ?? 0;
    if (v > 0 && v <= OUTLIER_CAP) {
      monthMap.set(key, (monthMap.get(key) ?? 0) + v);
    }
  }
  const monthlyAwardedSeries = Array.from(monthMap.entries())
    .map(([month, value]) => ({ month, value: value / 1e6 }))
    .sort((a, b) => {
      const parse = (s: string) => {
        const [m, y] = s.split(' ');
        return new Date(`${m} 20${y}`).getTime();
      };
      return parse(a.month) - parse(b.month);
    });

  const avg = monthlyAwardedSeries.length > 0
    ? monthlyAwardedSeries.reduce((s, p) => s + p.value, 0) / monthlyAwardedSeries.length
    : 0;
  const first3Avg = monthlyAwardedSeries.length >= 3
    ? monthlyAwardedSeries.slice(0, 3).reduce((s, p) => s + p.value, 0) / 3
    : avg;
  const last3Avg = monthlyAwardedSeries.length >= 3
    ? monthlyAwardedSeries.slice(-3).reduce((s, p) => s + p.value, 0) / 3
    : avg;
  const momentum3m = first3Avg > 0 ? Math.round(((last3Avg - first3Avg) / first3Avg) * 100) : 0;

  return {
    valueAwarded12m,
    awardedCount12m: awarded12m.length,
    openNow: openNowNotices.length,
    closing7d,
    closing30d,
    momentum3m,
    monthlyAwardedSeries,
    avgContractValue,
  };
}
