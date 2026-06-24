const BASE = "https://questions-statements-api.parliament.uk/api/writtenquestions/questions";
const TIMEOUT_MS = 15_000;

function makeAbort(): AbortController {
  const ac = new AbortController();
  setTimeout(() => ac.abort(), TIMEOUT_MS);
  return ac;
}

export type ParliamentaryQuestion = {
  id: string;
  askingMember: string;
  questionText: string;
  answerText: string | null;
  department: string;
  dateTabled: string;
  dateAnswered: string | null;
  url: string;
};

function mapQuestion(raw: Record<string, unknown>): ParliamentaryQuestion | null {
  const value = (raw.value ?? raw) as Record<string, unknown>;
  if (!value) return null;

  const id = String(value.id ?? value.uin ?? "");
  const questionText = String(value.questionText ?? value.heading ?? "");
  if (!questionText) return null;

  const member = value.askingMember as Record<string, unknown> | undefined;
  const answer = value.answer as Record<string, unknown> | undefined;

  return {
    id,
    askingMember: member
      ? String(member.name ?? member.listAs ?? "")
      : String(value.askingMemberName ?? ""),
    questionText,
    answerText: answer?.answerText ? String(answer.answerText) : null,
    department: String(
      value.answeringBodyName ?? value.department ?? ""
    ),
    dateTabled: String(value.dateTabled ?? value.dateOfQuestion ?? ""),
    dateAnswered: answer?.dateOfAnswer
      ? String(answer.dateOfAnswer)
      : value.dateAnswered
        ? String(value.dateAnswered)
        : null,
    url: id
      ? `https://questions-statements.parliament.uk/written-questions/detail/${encodeURIComponent(id)}`
      : "https://questions-statements.parliament.uk/",
  };
}

/**
 * Search UK Parliament written questions.
 * Free API — no key required.
 */
export async function searchParliamentaryQuestions(
  query: string,
  limit = 20
): Promise<ParliamentaryQuestion[]> {
  try {
    const ac = makeAbort();
    const url =
      `${BASE}` +
      `?searchTerm=${encodeURIComponent(query)}` +
      `&take=${limit}` +
      `&skip=0`;
    const res = await fetch(url, {
      headers: { Accept: "application/json" },
      signal: ac.signal,
    });
    if (!res.ok) return [];
    const data: unknown = await res.json();
    if (!data || typeof data !== "object") return [];

    const results = (data as Record<string, unknown>).results;
    const list = Array.isArray(results) ? results : [];
    return list
      .map((r: Record<string, unknown>) => mapQuestion(r))
      .filter((q): q is ParliamentaryQuestion => q !== null);
  } catch {
    return [];
  }
}
