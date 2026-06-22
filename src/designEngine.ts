import * as echarts from "echarts";

type TrustRecord = {
  title?: string;
  buyer?: string;
  awardedSupplier?: string;
  trustStatus?: string;
  confidence?: string;
  addressableValue?: number;
  recordValue?: number;
  recordId?: string;
  sourceUrl?: string;
  url?: string;
};

type TrustLayer = {
  sectorLens?: string;
  pulledCount?: number;
  relevantCount?: number;
  noisyCount?: number;
  verifiedCount?: number;
  inferredCount?: number;
  strategicCount?: number;
  totalPulledRecordValue?: number;
  totalRelevantRecordValue?: number;
  addressableOpportunityValue?: number;
  clientCapacityCap?: number;
  distinctRelevantBuyers?: number;
  distinctRelevantSuppliers?: number;
  keywords?: string[];
  regions?: string;
  topRelevantRecords?: TrustRecord[];
  relevantRecords?: TrustRecord[];
};

function safe(value: unknown) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function numberFmt(value: number | undefined) {
  return new Intl.NumberFormat("en-GB").format(value || 0);
}

function compactFmt(value: number | undefined) {
  return new Intl.NumberFormat("en-GB", {
    notation: "compact",
    maximumFractionDigits: 1
  }).format(value || 0);
}

function moneyFmt(value: number | undefined) {
  if (!value || Number.isNaN(value)) return "Not stated";
  return new Intl.NumberFormat("en-GB", {
    style: "currency",
    currency: "GBP",
    maximumFractionDigits: 0
  }).format(value);
}

function chartSvg(option: any, width = 760, height = 330) {
  const chart = echarts.init(null, null, {
    renderer: "svg",
    ssr: true,
    width,
    height
  });

  chart.setOption({
    backgroundColor: "transparent",
    animation: false,
    textStyle: {
      fontFamily: '"Inter","Helvetica Neue",Arial,sans-serif',
      color: "#0B0F14"
    },
    ...option
  });

  const svg = chart.renderToSVGString();
  chart.dispose();
  return svg;
}

function trustFunnelChart(trust: TrustLayer) {
  return chartSvg({
    title: {
      text: "Trust filter funnel",
      subtext: "Raw records reduced into evidence-backed opportunity signals",
      left: 10,
      top: 4,
      textStyle: { fontSize: 18, fontWeight: 700 },
      subtextStyle: { color: "#6F5B50", fontSize: 12 }
    },
    grid: { left: 160, right: 34, top: 70, bottom: 30 },
    xAxis: {
      type: "value",
      axisLine: { show: false },
      axisTick: { show: false },
      splitLine: { lineStyle: { color: "#EADCC7" } }
    },
    yAxis: {
      type: "category",
      inverse: true,
      data: ["Pulled", "Relevant", "Verified", "Inferred", "Strategic", "Excluded noise"],
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: "#0B0F14", fontWeight: 700 }
    },
    series: [
      {
        type: "bar",
        data: [
          trust.pulledCount || 0,
          trust.relevantCount || 0,
          trust.verifiedCount || 0,
          trust.inferredCount || 0,
          trust.strategicCount || 0,
          trust.noisyCount || 0
        ],
        label: {
          show: true,
          position: "right",
          formatter: "{c}",
          color: "#0B0F14",
          fontWeight: 700
        },
        itemStyle: {
          borderRadius: [0, 8, 8, 0],
          color: "#B8842D"
        },
        barWidth: 20
      }
    ]
  });
}

function valueLensChart(trust: TrustLayer) {
  return chartSvg({
    title: {
      text: "Value lens",
      subtext: "Separates noisy market value from relevant and addressable opportunity value",
      left: 10,
      top: 4,
      textStyle: { fontSize: 18, fontWeight: 700 },
      subtextStyle: { color: "#6F5B50", fontSize: 12 }
    },
    grid: { left: 170, right: 45, top: 75, bottom: 34 },
    xAxis: {
      type: "value",
      axisLabel: {
        formatter: (value: number) => compactFmt(value)
      },
      axisLine: { show: false },
      axisTick: { show: false },
      splitLine: { lineStyle: { color: "#EADCC7" } }
    },
    yAxis: {
      type: "category",
      inverse: true,
      data: ["Pulled value", "Relevant value", "Addressable signal", "Capacity cap"],
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: "#0B0F14", fontWeight: 700 }
    },
    series: [
      {
        type: "bar",
        data: [
          trust.totalPulledRecordValue || 0,
          trust.totalRelevantRecordValue || 0,
          trust.addressableOpportunityValue || 0,
          trust.clientCapacityCap || 0
        ],
        label: {
          show: true,
          position: "right",
          formatter: (params: any) => moneyFmt(params.value),
          color: "#0B0F14",
          fontWeight: 700
        },
        itemStyle: {
          borderRadius: [0, 8, 8, 0],
          color: "#0B0F14"
        },
        barWidth: 22
      }
    ]
  });
}

function trustCompositionChart(trust: TrustLayer) {
  return chartSvg({
    title: {
      text: "Evidence status mix",
      subtext: "How recommendations are classified before human verification",
      left: "center",
      top: 4,
      textStyle: { fontSize: 18, fontWeight: 700 },
      subtextStyle: { color: "#6F5B50", fontSize: 12 }
    },
    series: [
      {
        type: "pie",
        radius: ["44%", "68%"],
        center: ["50%", "58%"],
        avoidLabelOverlap: true,
        label: {
          formatter: "{b}: {c}",
          fontSize: 12,
          color: "#0B0F14"
        },
        itemStyle: {
          borderColor: "#FFF9EF",
          borderWidth: 3
        },
        data: [
          { value: trust.verifiedCount || 0, name: "Verified" },
          { value: trust.inferredCount || 0, name: "Inferred" },
          { value: trust.strategicCount || 0, name: "Strategic" },
          { value: trust.noisyCount || 0, name: "Excluded" }
        ]
      }
    ]
  }, 520, 330);
}

function metric(label: string, value: string, note: string) {
  return `
    <div class="apple-metric">
      <small>${safe(label)}</small>
      <strong>${safe(value)}</strong>
      <span>${safe(note)}</span>
    </div>
  `;
}

function sourceTable(records: TrustRecord[]) {
  const rows = (records || []).slice(0, 7).map(record => {
    const value = moneyFmt(record.addressableValue || record.recordValue || 0);
    const url = record.sourceUrl || record.url || "#";

    return `
      <tr>
        <td>${safe(String(record.title || "Untitled").slice(0, 80))}</td>
        <td>${safe(record.buyer || "Not stated")}</td>
        <td><span class="apple-pill">${safe(record.trustStatus || "Not confirmed")}</span></td>
        <td>${safe(record.confidence || "—")}</td>
        <td>${safe(value)}</td>
        <td><a href="${safe(url)}" target="_blank" rel="noopener noreferrer">${safe(record.recordId || "source")}</a></td>
      </tr>
    `;
  }).join("");

  return `
    <div class="apple-table-card">
      <div class="apple-section-head">
        <p>Source-backed evidence</p>
        <h3>Top records behind the recommendation layer</h3>
      </div>
      <table class="apple-source-table">
        <tr>
          <th>Record</th>
          <th>Buyer</th>
          <th>Status</th>
          <th>Confidence</th>
          <th>Value</th>
          <th>Source</th>
        </tr>
        ${rows || `<tr><td colspan="6">No relevant records passed the trust filter.</td></tr>`}
      </table>
    </div>
  `;
}

function premiumCss() {
  return `
    <style>
      .apple-report-system {
        margin: 30px 0;
        color: #0B0F14;
      }

      .apple-hero {
        position: relative;
        overflow: hidden;
        border: 1px solid #D8BE8C;
        background:
          radial-gradient(circle at 85% 5%, rgba(184,132,45,.25), transparent 32%),
          linear-gradient(135deg, #FFF9EF 0%, #FFFFFF 48%, #F2E3C9 100%);
        padding: 34px;
        min-height: 360px;
      }

      .apple-hero::after {
        content: "";
        position: absolute;
        width: 420px;
        height: 420px;
        right: -160px;
        top: -140px;
        border-radius: 50%;
        background: radial-gradient(circle, rgba(184,132,45,.28), rgba(36,20,15,.04), transparent 70%);
      }

      .apple-kicker {
        margin: 0 0 10px !important;
        color: #A97932;
        font-size: 11px !important;
        font-weight: 900;
        text-transform: uppercase;
        letter-spacing: .18em;
        text-align: left !important;
      }

      .apple-hero h2 {
        max-width: 720px;
        font-family: "Spectral","Iowan Old Style",Georgia,serif;
        font-size: 46px;
        line-height: .98;
        margin: 0 0 16px;
        letter-spacing: -.04em;
      }

      .apple-hero .apple-lede {
        max-width: 710px;
        color: #6F5B50;
        font-size: 15px;
        line-height: 1.65;
        text-align: justify;
        margin: 0;
      }

      .apple-metrics {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 12px;
        margin-top: 26px;
        position: relative;
        z-index: 2;
      }

      .apple-metric {
        background: rgba(255,255,255,.82);
        backdrop-filter: blur(8px);
        border: 1px solid #DFC99F;
        padding: 15px;
        min-height: 112px;
      }

      .apple-metric small {
        display: block;
        color: #6F5B50;
        font-size: 10.5px;
        font-weight: 900;
        text-transform: uppercase;
        letter-spacing: .08em;
        margin-bottom: 8px;
      }

      .apple-metric strong {
        display: block;
        font-family: "Spectral","Iowan Old Style",Georgia,serif;
        font-size: 25px;
        line-height: 1.05;
        margin-bottom: 8px;
        word-break: break-word;
      }

      .apple-metric span {
        display: block;
        color: #6F5B50;
        font-size: 12px;
        line-height: 1.38;
      }

      .apple-chart-grid {
        display: grid;
        grid-template-columns: 1fr;
        gap: 16px;
        margin-top: 18px;
      }

      .apple-chart-card {
        background: #FFFFFF;
        border: 1px solid #D8BE8C;
        padding: 18px;
        break-inside: avoid;
        page-break-inside: avoid;
      }

      .apple-chart-card svg {
        width: 100%;
        height: auto;
        display: block;
      }

      .apple-two-col {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 16px;
        margin-top: 16px;
      }

      .apple-context-card {
        background: #0B0F14;
        color: #FFF9EF;
        padding: 20px;
        border: 1px solid #0B0F14;
      }

      .apple-context-card p {
        color: #FFF9EF;
        font-size: 13px;
        line-height: 1.6;
        text-align: justify;
      }

      .apple-section-head p {
        margin: 0 0 8px !important;
        color: #A97932;
        font-size: 11px !important;
        font-weight: 900;
        text-transform: uppercase;
        letter-spacing: .16em;
        text-align: left !important;
      }

      .apple-section-head h3 {
        font-family: "Spectral","Iowan Old Style",Georgia,serif;
        font-size: 22px;
        line-height: 1.12;
        margin: 0 0 14px;
      }

      .apple-table-card {
        margin-top: 18px;
        background: #FFFFFF;
        border: 1px solid #D8BE8C;
        padding: 18px;
        break-inside: avoid;
        page-break-inside: avoid;
      }

      .apple-source-table {
        width: 100%;
        border-collapse: collapse;
        table-layout: fixed;
        font-size: 11.5px;
      }

      .apple-source-table th,
      .apple-source-table td {
        border: 1px solid #E4D4B9;
        padding: 8px;
        vertical-align: top;
        overflow-wrap: break-word;
      }

      .apple-source-table th {
        background: #0B0F14;
        color: #FFF9EF;
        text-align: left;
      }

      .apple-pill {
        display: inline-block;
        padding: 4px 8px;
        border-radius: 999px;
        background: #F0E2C7;
        color: #0B0F14;
        font-size: 10px;
        font-weight: 900;
        white-space: nowrap;
      }

      @media print {
        .apple-report-system {
          break-inside: avoid;
          page-break-inside: avoid;
        }

        .apple-hero {
          min-height: auto;
        }
      }
    </style>
  `;
}

export type ArticleChartSpec =
  | { type: "bar"; title: string; labels: string[]; values: number[]; unit?: string; color?: string }
  | { type: "line"; title: string; labels: string[]; values: number[]; unit?: string; color?: string }
  | { type: "pie"; title: string; labels: string[]; values: number[] }
  | { type: "horizontal-bar"; title: string; labels: string[]; values: number[]; unit?: string; color?: string };

export function renderArticleChart(spec: ArticleChartSpec, width = 700, height = 300): string {
  const fmtVal = (v: number, unit?: string) => {
    if (unit === "£bn") return `£${(v / 1e9).toFixed(1)}bn`;
    if (unit === "£m") return `£${(v / 1e6).toFixed(1)}m`;
    if (unit === "£k") return `£${Math.round(v / 1000)}k`;
    if (unit === "£") return `£${new Intl.NumberFormat("en-GB").format(v)}`;
    return new Intl.NumberFormat("en-GB").format(v);
  };
  const color = (spec as any).color || "#0E2318";
  const gridColor = "#E5DED4";
  const textColor = "#1A1208";
  const mutedColor = "#7D6B50";

  let option: any;

  if (spec.type === "bar") {
    option = {
      grid: { left: 50, right: 20, top: 50, bottom: 40 },
      xAxis: { type: "category", data: spec.labels, axisLabel: { color: mutedColor, fontSize: 11, interval: 0, rotate: spec.labels.length > 6 ? 30 : 0 }, axisLine: { lineStyle: { color: gridColor } }, axisTick: { show: false } },
      yAxis: { type: "value", axisLabel: { color: mutedColor, fontSize: 10, formatter: (v: number) => fmtVal(v, spec.unit) }, splitLine: { lineStyle: { color: gridColor } }, axisLine: { show: false }, axisTick: { show: false } },
      series: [{ type: "bar", data: spec.values, itemStyle: { color, borderRadius: [3, 3, 0, 0] }, label: { show: spec.values.length <= 8, position: "top", formatter: (p: any) => fmtVal(p.value, spec.unit), color: textColor, fontSize: 10, fontWeight: 600 } }]
    };
  } else if (spec.type === "horizontal-bar") {
    height = Math.max(height, spec.labels.length * 38 + 60);
    option = {
      grid: { left: 160, right: 80, top: 30, bottom: 20 },
      xAxis: { type: "value", axisLabel: { color: mutedColor, fontSize: 10, formatter: (v: number) => fmtVal(v, spec.unit) }, splitLine: { lineStyle: { color: gridColor } }, axisLine: { show: false }, axisTick: { show: false } },
      yAxis: { type: "category", data: [...spec.labels].reverse(), inverse: false, axisLabel: { color: textColor, fontSize: 11, fontWeight: 600 }, axisLine: { show: false }, axisTick: { show: false } },
      series: [{ type: "bar", data: [...spec.values].reverse(), itemStyle: { color, borderRadius: [0, 3, 3, 0] }, barMaxWidth: 28, label: { show: true, position: "right", formatter: (p: any) => fmtVal(p.value, spec.unit), color: mutedColor, fontSize: 10 } }]
    };
  } else if (spec.type === "line") {
    option = {
      grid: { left: 55, right: 20, top: 50, bottom: 40 },
      xAxis: { type: "category", data: spec.labels, axisLabel: { color: mutedColor, fontSize: 11 }, axisLine: { lineStyle: { color: gridColor } }, axisTick: { show: false } },
      yAxis: { type: "value", axisLabel: { color: mutedColor, fontSize: 10, formatter: (v: number) => fmtVal(v, spec.unit) }, splitLine: { lineStyle: { color: gridColor } }, axisLine: { show: false }, axisTick: { show: false } },
      series: [{ type: "line", data: spec.values, smooth: true, lineStyle: { color, width: 2.5 }, areaStyle: { color: color + "18" }, itemStyle: { color }, symbol: "circle", symbolSize: 5 }]
    };
  } else {
    option = {
      series: [{
        type: "pie", radius: ["38%", "62%"], center: ["50%", "58%"],
        data: spec.labels.map((l, i) => ({ name: l, value: spec.values[i] })),
        label: { formatter: "{b}: {d}%", fontSize: 11, color: textColor },
        itemStyle: { borderColor: "#FAF8F4", borderWidth: 2 }
      }]
    };
    height = 280;
  }

  if (spec.type !== "pie") {
    option.title = { text: spec.title, left: "left", top: 4, textStyle: { fontSize: 13, fontWeight: 700, color: textColor }, padding: [0, 0, 0, 6] };
  } else {
    option.title = { text: spec.title, left: "center", top: 8, textStyle: { fontSize: 13, fontWeight: 700, color: textColor } };
  }

  return chartSvg(option, width, height);
}

export function renderWorldClassDashboard(trust: TrustLayer) {
  return `
    ${premiumCss()}

    <section class="apple-report-system">
      <div class="apple-hero">
        <p class="apple-kicker">GovRevenue intelligence layer</p>
        <h2>Evidence, value and route-to-revenue in one commercial signal map.</h2>
        <p class="apple-lede">
          This dashboard is generated from structured procurement records and the GovRevenue trust filter.
          It separates raw pull volume from relevant opportunities, source-backed evidence and addressable value.
          The goal is not decoration; the goal is to make the commercial path obvious in seconds.
        </p>

        <div class="apple-metrics">
          ${metric("Total records pulled", numberFmt(trust.pulledCount), "Raw records returned before filtering")}
          ${metric("Relevant records", numberFmt(trust.relevantCount), "Records passing service and buyer relevance")}
          ${metric("Verified evidence", numberFmt(trust.verifiedCount), "Source-backed records with stronger confidence")}
          ${metric("Addressable value", moneyFmt(trust.addressableOpportunityValue), "Capped value signal, not forecast revenue")}
          ${metric("Relevant record value", moneyFmt(trust.totalRelevantRecordValue), "Gross value before capacity cap")}
          ${metric("Sector lens", trust.sectorLens || "Not stated", "Used to route charts, evidence and recommendations")}
        </div>
      </div>

      <div class="apple-chart-grid">
        <div class="apple-chart-card">${trustFunnelChart(trust)}</div>
        <div class="apple-chart-card">${valueLensChart(trust)}</div>
      </div>

      <div class="apple-two-col">
        <div class="apple-chart-card">${trustCompositionChart(trust)}</div>

        <div class="apple-context-card">
          <div class="apple-section-head">
            <p>Commercial interpretation</p>
            <h3>What the visuals mean</h3>
          </div>
          <p>
            A high pull count is not the same as a market opportunity. GovRevenue filters noisy procurement data
            into relevant records, then separates verified source-backed evidence from inferred signals and strategic targets.
          </p>
          <p>
            The addressable value is deliberately capped so the report does not pretend that the client can capture every
            pound in the pulled data. This is the difference between a generic AI report and a credible commercial scan.
          </p>
        </div>
      </div>

      ${sourceTable(trust.topRelevantRecords || trust.relevantRecords || [])}
    </section>
  `;
}
