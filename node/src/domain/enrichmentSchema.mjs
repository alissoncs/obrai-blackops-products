/**
 * Domain: LLM instructions — output shape and mapping rules from Flank CSV columns.
 */

export const DEFAULT_ENRICHMENT_SCHEMA = `
Each input record has CSV columns:
- "nome do produto" (text)
- "estoque" (integer or numeric string)
- "preço" (number; Brazilian format with comma allowed, e.g. "32,90")

Output one object per record (use null or "" if unknown):
{
  "sku": "string (derive from name if missing)",
  "ean": "string or null",
  "name": "string (from nome do produto)",
  "slug": "URL-friendly string without accents",
  "description": "short Portuguese e-commerce text",
  "tags": "comma-separated keywords",
  "primary_category_id": "string or null (category suggestion)",
  "retail_price": "number (parse preço; null if invalid)",
  "stock_quantity": "integer (parse estoque; null if invalid)"
}
`.trim();

export function buildEnrichmentSystemPrompt(schemaText) {
  const schema = schemaText?.trim() || DEFAULT_ENRICHMENT_SCHEMA;
  return `You enrich product rows for a catalog.
You receive a JSON array of records (CSV rows). For EACH record, output one enriched object.
Reply with ONLY a valid JSON array. No markdown, no \`\`\`json, no text before or after.
Keep the same order and length as the input array.

Output shape per item:
${schema}

Rules:
- Input columns are: nome do produto, estoque, preço → map to name, stock_quantity, retail_price.
- Parse Brazilian price (comma as decimal) to a number.
- Generate coherent sku and slug from the product name.
- Ignore extra columns if present; still follow the output schema above.
`.trim();
}
