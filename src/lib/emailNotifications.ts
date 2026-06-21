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
    subject: `GovRevenue scan completed: ${input.companyName}`,
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
      subject: `Your GovRevenue scan is ready`,
      text: [
        `Your GovRevenue scan for ${input.companyName} is ready.`,
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
    subject: `GovRevenue scan failed: ${input.companyName}`,
    text: [
      `Scan failed for ${input.companyName}.`,
      `Scan ID: ${input.scanId}`,
      `Status: ${input.status}`,
      `Error: ${input.errorSummary || "No error summary available."}`
    ].join("\n")
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
    subject: `GovRevenue weekly briefing — ${input.signals.length} open opportunities`,
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
    subject: `You're in — set your GovRevenue password`,
    text: [
      `Welcome to GovRevenue.`,
      ``,
      `Your ${planLabel} account has been created. Set your password to access your reports and account:`,
      ``,
      `${setupUrl}`,
      ``,
      `This link expires in 7 days. If you have any questions, reply to this email.`,
      ``,
      `— GovRevenue`
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
