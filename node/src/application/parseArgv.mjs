/**
 * Application boundary: parse process argv into a plain config object (no business logic).
 */

import path from "node:path";
import { Command } from "commander";

/**
 * @param {string[]} [argv=process.argv]
 */
export function parseCli(argv = process.argv) {
  const program = new Command();

  program
    .name("enrich-csv")
    .description("Read CSV, enrich products with LM Studio (Qwen), write JSON/JSONL")
    .requiredOption("-i, --input <path>", "Input CSV path")
    .requiredOption("-o, --output <path>", "Output file (.json or .jsonl)")
    .option(
      "--base-url <url>",
      "LM Studio OpenAI-compatible base URL",
      process.env.LMSTUDIO_BASE_URL || "http://localhost:1234/v1"
    )
    .option(
      "--model <name>",
      "Loaded model name in LM Studio (e.g. Qwen variant)",
      process.env.LMSTUDIO_MODEL || "qwen2.5-7b-instruct"
    )
    .option(
      "--api-key <key>",
      "Dummy API key (LM Studio usually ignores)",
      process.env.LMSTUDIO_API_KEY || "lm-studio"
    )
    .option("--chunk <n>", "Rows per LLM request", "40")
    .option("--delimiter <char>", "CSV delimiter", ",")
    .option("--encoding <enc>", "File encoding", "utf8")
    .option(
      "--format <type>",
      "json (single array at end) or jsonl (one object per line)",
      "jsonl"
    )
    .option("--temperature <n>", "Model temperature", "0.1")
    .option("--max-tokens <n>", "Max response tokens", "8192")
    .option("--schema-file <path>", "Optional file with custom output schema text");

  program.parse(argv);

  const opts = program.opts();
  const chunkSize = Math.max(1, parseInt(String(opts.chunk), 10) || 40);
  const format = String(opts.format).toLowerCase() === "json" ? "json" : "jsonl";

  return {
    inputPath: path.resolve(process.cwd(), opts.input),
    outputPath: path.resolve(process.cwd(), opts.output),
    format,
    chunkSize,
    delimiter: opts.delimiter,
    encoding: opts.encoding,
    baseUrl: opts.baseUrl,
    model: opts.model,
    apiKey: opts.apiKey,
    temperature: Number(opts.temperature),
    maxTokens: parseInt(String(opts.maxTokens), 10) || 8192,
    schemaFile: opts.schemaFile || null,
  };
}
