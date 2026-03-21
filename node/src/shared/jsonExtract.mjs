/**
 * Extract a JSON array from LLM output (may include markdown fences or extra text).
 */

export function extractJsonArray(text) {
  if (!text || typeof text !== "string") {
    throw new Error("Empty model response");
  }
  let s = text.trim();

  const fence = s.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fence) {
    s = fence[1].trim();
  }

  try {
    const parsed = JSON.parse(s);
    if (Array.isArray(parsed)) return parsed;
    if (parsed && typeof parsed === "object") {
      for (const k of ["results", "items", "data", "produtos"]) {
        if (Array.isArray(parsed[k])) return parsed[k];
      }
    }
  } catch {
    // continue
  }

  const bracket = s.match(/\[[\s\S]*\]/);
  if (bracket) {
    return JSON.parse(bracket[0]);
  }

  throw new Error("Could not extract a JSON array from model output");
}
