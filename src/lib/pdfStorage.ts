import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";

type PdfStorageConfig = {
  bucket: string;
  endpoint: string;
  region: string;
  accessKeyId: string;
  secretAccessKey: string;
  publicBaseUrl: string | null;
  keyPrefix: string;
};

export type StoredPdf = {
  key: string;
  publicUrl: string | null;
  etag: string | null;
};

let client: S3Client | null = null;
let config: PdfStorageConfig | null | undefined;

function env(...names: string[]) {
  for (const name of names) {
    const value = process.env[name]?.trim();
    if (value) return value;
  }

  return null;
}

function getConfig(): PdfStorageConfig | null {
  if (config !== undefined) return config;

  const bucket = env("PDF_STORAGE_BUCKET", "R2_BUCKET_NAME", "S3_BUCKET", "AWS_S3_BUCKET");
  const accessKeyId = env("PDF_STORAGE_ACCESS_KEY_ID", "R2_ACCESS_KEY_ID", "S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID");
  const secretAccessKey = env("PDF_STORAGE_SECRET_ACCESS_KEY", "R2_SECRET_ACCESS_KEY", "S3_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY");
  const accountId = env("R2_ACCOUNT_ID", "CLOUDFLARE_ACCOUNT_ID");
  const endpoint = env("PDF_STORAGE_ENDPOINT", "R2_ENDPOINT", "S3_ENDPOINT", "AWS_S3_ENDPOINT") ??
    (accountId ? `https://${accountId}.r2.cloudflarestorage.com` : null);

  if (!bucket || !accessKeyId || !secretAccessKey || !endpoint) {
    config = null;
    return config;
  }

  config = {
    bucket,
    endpoint,
    accessKeyId,
    secretAccessKey,
    region: env("PDF_STORAGE_REGION", "R2_REGION", "S3_REGION", "AWS_REGION") ?? "auto",
    publicBaseUrl: env("PDF_STORAGE_PUBLIC_BASE_URL", "R2_PUBLIC_BASE_URL", "S3_PUBLIC_BASE_URL"),
    keyPrefix: env("PDF_STORAGE_PREFIX") ?? "reports"
  };

  return config;
}

function getClient() {
  const currentConfig = getConfig();
  if (!currentConfig) return null;
  if (client) return client;

  client = new S3Client({
    region: currentConfig.region,
    endpoint: currentConfig.endpoint,
    forcePathStyle: true,
    credentials: {
      accessKeyId: currentConfig.accessKeyId,
      secretAccessKey: currentConfig.secretAccessKey
    }
  });

  return client;
}

export function isPdfStorageConfigured() {
  return Boolean(getConfig());
}

export function buildPdfStorageKey(scanId: string, filename: string) {
  const currentConfig = getConfig();
  const prefix = currentConfig?.keyPrefix.replace(/^\/+|\/+$/g, "") || "reports";
  const safeFilename = filename.replace(/[^a-z0-9._-]+/gi, "_").toLowerCase();
  return `${prefix}/${scanId}/${safeFilename}`;
}

export async function storePdfObject(params: {
  key: string;
  filename: string;
  body: Buffer;
}): Promise<StoredPdf | null> {
  const currentConfig = getConfig();
  const currentClient = getClient();

  if (!currentConfig || !currentClient) return null;

  const result = await currentClient.send(
    new PutObjectCommand({
      Bucket: currentConfig.bucket,
      Key: params.key,
      Body: params.body,
      ContentType: "application/pdf",
      ContentDisposition: `attachment; filename="${params.filename.replace(/"/g, "")}"`,
      CacheControl: "private, max-age=31536000, immutable"
    })
  );

  const publicBaseUrl = currentConfig.publicBaseUrl?.replace(/\/+$/g, "");

  return {
    key: params.key,
    publicUrl: publicBaseUrl ? `${publicBaseUrl}/${params.key}` : null,
    etag: result.ETag?.replace(/"/g, "") ?? null
  };
}
