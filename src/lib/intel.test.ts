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
  computeRenewalRadar,
  renewalDaysLeft,
  keywordMatchesText,
  anyKeywordMatches,
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

const SAMPLE_EDP = `# AtlasRevenue Scan: Acme Ltd

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

describe("computeRenewalRadar", () => {
  const now = new Date("2026-07-02T00:00:00Z");
  const mk = (over: Partial<import("./intel.js").RenewalNotice>) => ({
    buyer: "Kent County Council",
    title: "Cleaning services",
    awardedValue: 500_000,
    awardedSupplier: "Incumbent Ltd",
    contractEnd: "2026-12-01T00:00:00Z",
    url: "https://example.test/notice/1",
    ...over,
  });

  it("keeps contracts ending within the 12-month horizon, sorted soonest-first", () => {
    const out = computeRenewalRadar(
      [mk({ title: "B", contractEnd: "2027-05-01T00:00:00Z" }), mk({ title: "A", contractEnd: "2026-09-01T00:00:00Z" })],
      now
    );
    expect(out.map(n => n.title)).toEqual(["A", "B"]);
  });

  it("includes recently-lapsed contracts (open retender window) but not older ones", () => {
    const lapsed30d = mk({ title: "lapsed", contractEnd: "2026-06-02T00:00:00Z" });
    const lapsed200d = mk({ title: "too old", contractEnd: "2025-12-14T00:00:00Z" });
    const out = computeRenewalRadar([lapsed30d, lapsed200d], now);
    expect(out.map(n => n.title)).toEqual(["lapsed"]);
  });

  it("drops notices with no contract end, beyond-horizon ends, unnamed buyers and aggregators", () => {
    const out = computeRenewalRadar(
      [
        mk({ title: "no end", contractEnd: null }),
        mk({ title: "far future", contractEnd: "2029-01-01T00:00:00Z" }),
        mk({ title: "anon", buyer: "Not stated" }),
        mk({ title: "aggregator", buyer: "ESPO" }),
        mk({ title: "keeper" }),
      ],
      now
    );
    expect(out.map(n => n.title)).toEqual(["keeper"]);
  });

  it("dedupes identical buyer+title pairs and respects the limit", () => {
    const out = computeRenewalRadar([mk({}), mk({}), mk({ title: "second" })], now, { limit: 1 });
    expect(out).toHaveLength(1);
  });
});

describe("renewalDaysLeft", () => {
  it("is positive before expiry and negative after", () => {
    const now = new Date("2026-07-02T00:00:00Z");
    expect(renewalDaysLeft("2026-07-12T00:00:00Z", now)).toBe(10);
    expect(renewalDaysLeft("2026-06-22T00:00:00Z", now)).toBe(-10);
  });
});

describe("keywordMatchesText", () => {
  it("matches short keywords only as whole words", () => {
    expect(keywordMatchesText("enterprise software procurement", "erp")).toBe(false);
    expect(keywordMatchesText("erp implementation for council", "erp")).toBe(true);
    expect(keywordMatchesText("social care framework", "soc")).toBe(false);
    expect(keywordMatchesText("soc 2 compliance audit", "soc")).toBe(true);
    expect(keywordMatchesText("commissioning services", "mis")).toBe(false);
    expect(keywordMatchesText("mis replacement project", "mis")).toBe(true);
  });
  it("treats punctuation and string edges as word boundaries", () => {
    expect(keywordMatchesText("upgrade (erp)", "erp")).toBe(true);
    expect(keywordMatchesText("erp", "erp")).toBe(true);
    expect(keywordMatchesText("cctv/security upgrade", "cctv")).toBe(true);
  });
  it("does not treat digits as boundaries", () => {
    expect(keywordMatchesText("iso27001erp cert", "erp")).toBe(false);
  });
  it("handles regex-special characters in short keywords", () => {
    expect(keywordMatchesText("m&e services contract", "m&e")).toBe(true);
    expect(keywordMatchesText("time services contract", "m&e")).toBe(false);
  });
  it("keeps substring matching for keywords longer than 4 chars", () => {
    expect(keywordMatchesText("recleaning works", "cleaning")).toBe(true);
    expect(keywordMatchesText("ux design sprint retainer", "ux design")).toBe(true);
  });
});

describe("anyKeywordMatches", () => {
  it("returns true when any keyword matches", () => {
    expect(anyKeywordMatches("hvac maintenance", ["erp", "hvac"])).toBe(true);
  });
  it("returns false when only substring hits exist for short keywords", () => {
    expect(anyKeywordMatches("enterprise misommunication society", ["erp", "mis", "soc"])).toBe(false);
  });
});
