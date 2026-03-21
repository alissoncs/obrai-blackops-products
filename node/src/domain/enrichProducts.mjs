/**
 * Domain: one enrichment batch — builds prompts and parses model JSON.
 */

import { chatCompletion } from "../infrastructure/lmStudioClient.mjs";
import { extractJsonArray } from "../shared/jsonExtract.mjs";
import { buildEnrichmentSystemPrompt } from "./enrichmentSchema.mjs";

/**
 * @param {Record<string, string>[]} records
 * @param {{
 *   baseUrl: string,
 *   apiKey: string,
 *   model: string,
 *   schemaText: string,
 *   temperature: number,
 *   maxTokens: number,
 *   signal?: AbortSignal
 * }} options
 */
export async function enrichProductRecords(records, options) {
  const {
    baseUrl,
    apiKey,
    model,
    schemaText,
    temperature,
    maxTokens,
    signal,
  } = options;

  const system = buildEnrichmentSystemPrompt(schemaText ?? "");
  const user = `Transform this list (JSON). Return ONLY the output JSON array:\n${JSON.stringify(records)}`;

  const raw = await chatCompletion({
    baseUrl,
    apiKey,
    model,
    messages: [
      { role: "system", content: system },
      { role: "user", content: user },
    ],
    temperature,
    maxTokens,
    signal,
  });

  return extractJsonArray(raw);
}
