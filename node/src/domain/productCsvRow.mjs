/**
 * Domain: expected CSV columns — nome do produto, estoque, preço.
 * Normalizes header key variants (BOM, spacing, preço/preco).
 */

function stripDiacritics(s) {
  return s.normalize("NFD").replace(/\p{M}/gu, "");
}

function normKey(k) {
  return stripDiacritics(String(k).trim())
    .toLowerCase()
    .replace(/\s+/g, " ");
}

/** @param {Record<string, string>} record */
export function normalizeProductCsvRow(record) {
  const byNorm = new Map();
  for (const [k, v] of Object.entries(record)) {
    byNorm.set(normKey(k), v);
  }

  const get = (...candidates) => {
    for (const c of candidates) {
      const key = normKey(c);
      if (byNorm.has(key)) return byNorm.get(key);
    }
    return "";
  };

  return {
    "nome do produto": String(get("nome do produto", "nome_do_produto") ?? "").trim(),
    estoque: String(get("estoque") ?? "").trim(),
    preço: String(get("preço", "preco") ?? "").trim(),
  };
}
