import type { BuyerEntity } from "../buyer-graph/types.js";
import type { DomainDiscoveryResult } from "./types.js";

const GOV_DOMAIN_MAP: Record<string, (slug: string) => string> = {
  local_authority: (s) => `${s}.gov.uk`,
  nhs: (s) => `${s}.nhs.uk`,
  police_fire: (s) => `${s}.police.uk`,
  education: (s) => `${s}.ac.uk`,
  central_gov: (s) => `${s}.gov.uk`,
  housing: (s) => `${s}.org.uk`,
};

function slugify(name: string): string {
  return name.toLowerCase()
    .replace(/\b(council|borough of|city of|county|district|london borough of|royal borough of|metropolitan|university of|the)\b/g, "")
    .replace(/[^a-z0-9]+/g, "")
    .trim();
}

export function discoverDomain(entity: BuyerEntity): DomainDiscoveryResult {
  if (entity.website) {
    const domain = entity.website.replace(/^https?:\/\//, "").replace(/\/.*$/, "").toLowerCase();
    return { domain, source: "company_website", confidence: 90 };
  }

  const patternFn = GOV_DOMAIN_MAP[entity.buyer_type];
  if (patternFn) {
    const slug = slugify(entity.name);
    if (slug.length >= 3) {
      return { domain: patternFn(slug), source: "buyer_type_pattern", confidence: 70 };
    }
  }

  return { domain: null, source: "buyer_type_pattern", confidence: 0 };
}

export async function verifyDomainMx(domain: string): Promise<boolean> {
  try {
    const dns = await import("dns");
    return new Promise((resolve) => {
      dns.resolveMx(domain, (err, addresses) => {
        resolve(!err && Array.isArray(addresses) && addresses.length > 0);
      });
    });
  } catch {
    return false;
  }
}
