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
