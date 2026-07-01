import { Resend } from "resend";

type ScanEmailInput = {
  scanId: string;
  companyName: string;
  status: "completed" | "failed";
  reportUrl?: string;
  pdfUrl?: string;
  clientEmail?: string | null;
  errorSummary?: string | null;
};

let resend: Resend | null | undefined;

function env(name: string) {
  return process.env[name]?.trim() || "";
}

function getResend() {
  if (resend !== undefined) return resend;

  const apiKey = env("RESEND_API_KEY");
  resend = apiKey ? new Resend(apiKey) : null;
  return resend;
}

export function isEmailConfigured() {
  return Boolean(getResend() && env("FROM_EMAIL") && env("ADMIN_EMAIL"));
}

function absoluteUrl(path: string) {
  const explicitBase = env("PUBLIC_APP_URL") || env("APP_BASE_URL") || env("PUBLIC_BASE_URL");
  const railwayDomain = env("RAILWAY_PUBLIC_DOMAIN");
  const base = explicitBase || (railwayDomain ? `https://${railwayDomain}` : "");

  if (!base) return path;
  return `${base.replace(/\/+$/g, "")}${path.startsWith("/") ? path : `/${path}`}`;
}

export function buildScanLinks(scanId: string) {
  return {
    reportUrl: absoluteUrl(`/scan/${encodeURIComponent(scanId)}`),
    pdfUrl: absoluteUrl(`/api/scans/${encodeURIComponent(scanId)}/report.pdf`)
  };
}

async function sendEmail(params: { to: string; subject: string; text: string }) {
  const client = getResend();
  const from = env("FROM_EMAIL");

  if (!client || !from || !params.to) return;

  try {
    await client.emails.send({
      from,
      to: params.to,
      subject: params.subject,
      text: params.text
    });
  } catch (error) {
    console.error("[email] failed", error);
  }
}

export async function notifyScanCompleted(input: ScanEmailInput) {
  if (!isEmailConfigured()) return;

  const adminEmail = env("ADMIN_EMAIL");
  const reportUrl = input.reportUrl || buildScanLinks(input.scanId).reportUrl;
  const pdfUrl = input.pdfUrl || buildScanLinks(input.scanId).pdfUrl;

  await sendEmail({
    to: adminEmail,
    subject: `AtlasRevenue scan completed: ${input.companyName}`,
    text: [
      `Scan completed for ${input.companyName}.`,
      `Scan ID: ${input.scanId}`,
      `Status: ${input.status}`,
      `Report: ${reportUrl}`,
      `PDF: ${pdfUrl}`
    ].join("\n")
  });

  if (input.clientEmail) {
    await sendEmail({
      to: input.clientEmail,
      subject: `Your AtlasRevenue scan is ready`,
      text: [
        `Your AtlasRevenue scan for ${input.companyName} is ready.`,
        "",
        `Report: ${reportUrl}`,
        `PDF: ${pdfUrl}`,
        "",
        "Please review the source notes and human verification checks before making bid decisions."
      ].join("\n")
    });
  }
}

export async function notifyScanFailed(input: ScanEmailInput) {
  if (!isEmailConfigured()) return;

  await sendEmail({
    to: env("ADMIN_EMAIL"),
    subject: `AtlasRevenue scan failed: ${input.companyName}`,
    text: [
      `Scan failed for ${input.companyName}.`,
      `Scan ID: ${input.scanId}`,
      `Status: ${input.status}`,
      `Error: ${input.errorSummary || "No error summary available."}`
    ].join("\n")
  });
}

export type WatchlistBuyer = {
  buyer: string;
  orgType: string;
  intentScore: number;
  prevScore: number | null;
  whyNow: string;
  likelyNeed: string;
  isNew: boolean;
};

export async function sendWatchlistDigest(input: {
  email: string;
  niche: string;
  buyers: WatchlistBuyer[];
  totalTracked: number;
  highIntentCount: number;
  avgScore: number;
  marketPageUrl: string;
  unsubscribeUrl: string;
}) {
  const client = getResend();
  const from = env("FROM_EMAIL");
  if (!client || !from || !input.email) return;

  const buyerLines = input.buyers.slice(0, 12).map((b, i) => {
    const delta = b.prevScore !== null ? (b.intentScore - b.prevScore) : null;
    const deltaStr = delta !== null && delta !== 0
      ? delta > 0 ? ` (+${delta})` : ` (${delta})`
      : "";
    const newTag = b.isNew ? " [NEW]" : "";
    return [
      `${i + 1}. ${b.buyer}${newTag}`,
      `   Intent Score: ${b.intentScore}/100${deltaStr}`,
      `   Type: ${b.orgType}`,
      `   Likely need: ${b.likelyNeed}`,
      `   Why now: ${b.whyNow.slice(0, 120)}`,
    ].join("\n");
  }).join("\n\n");

  const newCount = input.buyers.filter(b => b.isNew).length;
  const movers = input.buyers.filter(b => b.prevScore !== null && b.intentScore - b.prevScore >= 10);

  const summaryParts = [
    `${input.highIntentCount} high-intent buyers (60+)`,
    `${input.totalTracked} total tracked`,
    `Avg score: ${input.avgScore}/100`,
  ];
  if (newCount > 0) summaryParts.push(`${newCount} new this week`);
  if (movers.length > 0) summaryParts.push(`${movers.length} rising (score up 10+)`);

  await sendEmail({
    to: input.email,
    subject: `Buyer Watchlist — ${input.niche} — ${input.highIntentCount} high-intent buyers`,
    text: [
      `Weekly Buyer Watchlist — ${input.niche}`,
      ``,
      summaryParts.join(" · "),
      ``,
      `─────────────────────────────────`,
      ``,
      buyerLines,
      ``,
      input.buyers.length > 12 ? `...and ${input.buyers.length - 12} more buyers tracked.\n` : "",
      `View full buyer map: ${input.marketPageUrl}`,
      ``,
      `─────────────────────────────────`,
      ``,
      `This is your weekly Buyer Watchlist digest from AtlasRevenue.`,
      `Scores update when new procurement data is published.`,
      ``,
      `Unsubscribe: ${input.unsubscribeUrl}`,
    ].filter(Boolean).join("\n"),
  });
}

export type WeeklyAlertNotice = {
  title: string;
  buyer: string;
  value: string;
  deadline: string | null;
  url: string;
  source: string;
};

export type BriefingSignal = {
  title: string;
  buyer: string;
  value: string;
  category: string;
  deadline: string | null;
  url: string;
};

export async function sendBriefingEmail(input: {
  email: string;
  signals: BriefingSignal[];
  unsubscribeUrl: string;
}) {
  const client = getResend();
  const from = env("FROM_EMAIL");
  if (!client || !from || !input.email) return;

  const lines = input.signals.map((s, i) =>
    [
      `${i + 1}. ${s.title}`,
      `   Sector: ${s.category}`,
      `   Buyer: ${s.buyer}`,
      `   Value: ${s.value}`,
      s.deadline ? `   Deadline: ${s.deadline}` : null,
      `   ${s.url}`
    ].filter(Boolean).join("\n")
  ).join("\n\n");

  await sendEmail({
    to: input.email,
    subject: `AtlasRevenue weekly briefing — ${input.signals.length} open opportunities`,
    text: [
      `Weekly government procurement briefing`,
      ``,
      `${input.signals.length} open opportunities across UK public sector desks:`,
      ``,
      lines,
      ``,
      `Browse all desks: ${absoluteUrl("/")}`,
      ``,
      `Unsubscribe: ${input.unsubscribeUrl}`
    ].join("\n")
  });
}

export async function sendWelcomeEmail(email: string, plan: string, setupUrl: string) {
  const planLabel = plan === "payg" ? "Pay as you go" : plan === "agency" ? "Agency" : "Pro";
  await sendEmail({
    to: email,
    subject: `You're in — set your AtlasRevenue password`,
    text: [
      `Welcome to AtlasRevenue.`,
      ``,
      `Your ${planLabel} account has been created. Set your password to access your reports and account:`,
      ``,
      `${setupUrl}`,
      ``,
      `This link expires in 7 days. If you have any questions, reply to this email.`,
      ``,
      `— AtlasRevenue`
    ].join("\n")
  });
}

export async function sendWeeklyAlert(input: {
  subscriptionId: string;
  companyName: string;
  email: string;
  newNotices: WeeklyAlertNotice[];
  totalNewCount: number;
  reportUrl: string;
  unsubscribeUrl: string;
}) {
  const client = getResend();
  const from = env("FROM_EMAIL");
  if (!client || !from || !input.email) return;

  const noticeLines = input.newNotices.slice(0, 15).map((n, i) =>
    [
      `${i + 1}. ${n.title}`,
      `   Buyer: ${n.buyer}`,
      `   Value: ${n.value}`,
      n.deadline ? `   Deadline: ${n.deadline}` : null,
      `   ${n.source}: ${n.url}`
    ].filter(Boolean).join("\n")
  ).join("\n\n");

  const overflow = input.totalNewCount > 15
    ? `\n\n...and ${input.totalNewCount - 15} more opportunities.`
    : "";

  await sendEmail({
    to: input.email,
    subject: `${input.totalNewCount} new ${input.totalNewCount === 1 ? "opportunity" : "opportunities"} for ${input.companyName}`,
    text: [
      `Weekly procurement alert — ${input.companyName}`,
      ``,
      `${input.totalNewCount} new ${input.totalNewCount === 1 ? "opportunity" : "opportunities"} found since your last alert:`,
      ``,
      noticeLines + overflow,
      ``,
      `View your report: ${input.reportUrl}`,
      ``,
      `Unsubscribe: ${input.unsubscribeUrl}`
    ].join("\n")
  });
}
