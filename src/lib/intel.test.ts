import { describe, it, expect } from "vitest";
import {
  escapeHtml,
  formatMoney,
  fmtMoney,
  slugify,
  computeOutlierThreshold,
  parseEdpFromMarkdown,
  validateReportConsistency,
  isAggregatorBuyer,
} from "./intel.js";

describe("escapeHtml", () => {
  it("escapes the five HTML-significant characters", () => {
    expect(escapeHtml('<a href="x">&\'')).toBe("&lt;a href=&quot;x&quot;&gt;&amp;&#039;");
  });
  it("escapes ampersand without double-escaping later entities", () => {
    // & must be replaced first, otherwise '<' would become '&amp;lt;'
    expect(escapeHtml("<")).toBe("&lt;");
    expect(escapeHtml("&")).toBe("&amp;");
    expect(escapeHtml("a < b & c")).toBe("a &lt; b &amp; c");
  });
  it("handles null/undefined safely", () => {
    expect(escapeHtml(null as unknown as string)).toBe("");
    expect(escapeHtml(undefined as unknown as string)).toBe("");
  });
});

describe("formatMoney", () => {
  it("formats GBP with no decimals", () => {
    expect(formatMoney(1_500_000)).toBe("£1,500,000");
  });
  it("returns dash for null/NaN", () => {
    expect(formatMoney(null)).toBe("-");
    expect(formatMoney(undefined)).toBe("-");
    expect(formatMoney(NaN)).toBe("-");
  });
});

describe("fmtMoney", () => {
  it("uses bn/m/k suffixes by magnitude", () => {
    expect(fmtMoney(2_400_000_000)).toBe("£2.40bn");
    expect(fmtMoney(3_200_000)).toBe("£3.2m");
    expect(fmtMoney(45_000)).toBe("£45k");
    expect(fmtMoney(800)).toBe("£800");
  });
});

describe("slugify", () => {
  it("lowercases and dashes non-alphanumerics", () => {
    expect(slugify("Networks & Infrastructure")).toBe("networks-infrastructure");
    expect(slugify("Health & Social Care")).toBe("health-social-care");
  });
  it("trims leading/trailing dashes", () => {
    expect(slugify("  Foo_Bar!  ")).toBe("foo-bar");
  });
});

describe("computeOutlierThreshold", () => {
  it("excludes an £18bn academy notice (desk median ~£250k)", () => {
    const values = [150_000, 200_000, 250_000, 18_000_000_000];
    const threshold = computeOutlierThreshold(values);
    expect(18_000_000_000).toBeGreaterThan(threshold);
  });
  it("keeps a legitimate £4bn contract (desk median ~£500k)", () => {
    const values = [400_000, 500_000, 4_000_000_000];
    const threshold = computeOutlierThreshold(values);
    expect(4_000_000_000).toBeLessThanOrEqual(threshold);
  });
  it("clamps to the £50m floor for tiny-median desks", () => {
    expect(computeOutlierThreshold([1_000, 2_000, 3_000])).toBe(50_000_000);
  });
  it("clamps to the £10bn ceiling for huge-median desks", () => {
    expect(computeOutlierThreshold([2_000_000, 3_000_000])).toBe(10_000_000_000);
  });
  it("returns the £10bn ceiling when there are no positive values", () => {
    expect(computeOutlierThreshold([])).toBe(10_000_000_000);
    expect(computeOutlierThreshold([0, 0])).toBe(10_000_000_000);
  });
});

const SAMPLE_EDP = `# GovRevenue Scan: Acme Ltd

## 1. Executive Decision Panel

| Field | Value |
|---|---|
| Verdict | Pursue selectively |
| Can they win now? | Yes, on niche lots |
| Best first money route | DPS for managed services |
| Fastest action this week | Register on the buyer portal |
| Main blocker | No public-sector case study |
| Evidence Grade | B |
| Recommended route | Framework plus direct award |

## 2. Evidence Grade and Scan Basis

Some prose here.
`;

describe("parseEdpFromMarkdown", () => {
  it("extracts every EDP field from the panel table", () => {
    const edp = parseEdpFromMarkdown(SAMPLE_EDP);
    expect(edp).not.toBeNull();
    expect(edp!.verdict).toBe("Pursue selectively");
    expect(edp!.evidenceGrade).toBe("B");
    expect(edp!.canTheyWinNow).toBe("Yes, on niche lots");
    expect(edp!.recommendedRoute).toBe("Framework plus direct award");
  });
  it("returns null when the panel is absent", () => {
    expect(parseEdpFromMarkdown("## 2. Something else\n\nNo panel.")).toBeNull();
  });
});

describe("validateReportConsistency", () => {
  it("passes a well-formed report", () => {
    const edp = parseEdpFromMarkdown(SAMPLE_EDP);
    const result = validateReportConsistency(edp, SAMPLE_EDP);
    expect(result.valid).toBe(true);
    expect(result.errors).toHaveLength(0);
    expect(result.conflicts).toHaveLength(0);
  });
  it("flags conflicting evidence grades", () => {
    const edp = parseEdpFromMarkdown(SAMPLE_EDP);
    const conflicting = SAMPLE_EDP + "\n| Evidence Grade | D |\n";
    const result = validateReportConsistency(edp, conflicting);
    expect(result.valid).toBe(false);
    expect(result.conflicts.join(" ")).toMatch(/Conflicting evidence grades/);
  });
  it("fails when the EDP could not be parsed", () => {
    const result = validateReportConsistency(null, "no panel");
    expect(result.valid).toBe(false);
    expect(result.errors.length).toBeGreaterThan(0);
  });
});

describe("isAggregatorBuyer", () => {
  it("catches known framework aggregators", () => {
    expect(isAggregatorBuyer("YPO")).toBe(true);
    expect(isAggregatorBuyer("Yorkshire Purchasing Organisation")).toBe(true);
    expect(isAggregatorBuyer("Crown Commercial Service")).toBe(true);
    expect(isAggregatorBuyer("ESPO")).toBe(true);
  });
  it("does not flag genuine direct buyers", () => {
    expect(isAggregatorBuyer("Birmingham City Council")).toBe(false);
    expect(isAggregatorBuyer("NHS Greater Manchester ICB")).toBe(false);
  });
  it("does not catch The Crescent Academy (a real buyer, handled by outlier exclusion)", () => {
    expect(isAggregatorBuyer("The Crescent Academy")).toBe(false);
  });
});
