import { z } from "zod";
import { intakeSchema } from "../data/intake.js";
import { openai, OPENAI_MODEL } from "../config.js";

function withTimeout<T>(ms: number, fn: (signal: AbortSignal) => Promise<T>): Promise<T> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), ms);
  return fn(controller.signal).finally(() => clearTimeout(timer));
}

function withOpenAiTimeout<T>(fn: (signal: AbortSignal) => Promise<T>): Promise<T> {
  return withTimeout(90_000, fn);
}

export async function generateSearchKeywords(input: z.infer<typeof intakeSchema>): Promise<string[]> {
  const prompt = `You are a UK public-sector procurement specialist helping to find relevant contracts on Contracts Finder.

Given this company intake, return exactly 6–8 keyword search phrases to use as Contracts Finder search terms.

Rules:
- Use 2–4 word phrases that Contracts Finder would return genuine contract matches for
- Match the company's actual core services precisely — do not invent or assume sectors
- Do not use company names, buyer names, or location names
- Focus on contract and tender terminology (what councils and housing associations would call the service in a tender)
- If the company does social housing repairs, use housing maintenance terms — not property surveying terms
- If the company does cleaning, use cleaning terms — not facilities management terms
- Return ONLY valid JSON: { "keywords": ["phrase 1", "phrase 2", ...] }

Company intake:
Company: ${input.companyName}
Main services: ${input.mainServices}
Secondary services: ${input.secondaryServices || "none"}
Ideal buyers: ${input.idealBuyers || "public sector"}
Main goal: ${input.mainGoal || "win public sector contracts"}
Framework access: ${input.frameworkStatus || "none stated"}
Last public contract: ${input.lastPublicContract || "none stated"}`;

  const response = await withOpenAiTimeout(signal =>
    openai.chat.completions.create({
      model: OPENAI_MODEL,
      messages: [{ role: "user", content: prompt }],
      response_format: { type: "json_object" },
      temperature: 0.1,
      max_tokens: 300
    }, { signal })
  );

  const raw = response.choices[0]?.message?.content ?? "{}";
  const parsed = JSON.parse(raw);
  const keywords: unknown = parsed.keywords ?? parsed.terms ?? Object.values(parsed)[0];

  if (!Array.isArray(keywords) || keywords.length === 0) {
    throw new Error("LLM returned no keywords");
  }

  return (keywords as unknown[])
    .filter((k): k is string => typeof k === "string" && k.length >= 3 && k.length <= 80)
    .slice(0, 8);
}
