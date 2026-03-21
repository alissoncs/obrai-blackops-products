/**
 * Application: orchestrates CSV stream → batches → LLM enrichment → output file.
 */

import fs from "node:fs";
import path from "node:path";
import { parse } from "csv-parse";

import { enrichProductRecords } from "../domain/enrichProducts.mjs";
import { normalizeProductCsvRow } from "../domain/productCsvRow.mjs";
import { createRunLogger, logStepAsync, logStepSync } from "../infrastructure/logging/winstonLogger.mjs";

/**
 * @typedef {object} EnrichmentRunConfig
 * @property {string} inputPath
 * @property {string} outputPath
 * @property {'json'|'jsonl'} format
 * @property {number} chunkSize
 * @property {string} delimiter
 * @property {string} encoding
 * @property {string} baseUrl
 * @property {string} model
 * @property {string} apiKey
 * @property {number} temperature
 * @property {number} maxTokens
 * @property {string|null} schemaFile
 */

/**
 * @param {import('winston').Logger} logger
 * @param {EnrichmentRunConfig} config
 * @param {string} schemaText
 * @param {{ batch: Record<string, string>[], chunkIndex: number, totalOut: number }} state
 * @param {Record<string, unknown>[]} jsonAccumulator
 * @param {boolean} takeAll
 */
async function flushBatch(logger, config, schemaText, state, jsonAccumulator, takeAll) {
  if (state.batch.length === 0) return;

  state.chunkIndex += 1;
  const take = takeAll ? state.batch.length : Math.min(config.chunkSize, state.batch.length);
  const slice = state.batch.splice(0, take);

  logger.info(`Starting chunk ${state.chunkIndex}`, { recordCount: slice.length });

  const enriched = await logStepAsync(
    logger,
    `Chunk ${state.chunkIndex}: LM Studio enrich`,
    async () => {
      const result = await enrichProductRecords(slice, {
        baseUrl: config.baseUrl,
        apiKey: config.apiKey,
        model: config.model,
        schemaText,
        temperature: config.temperature,
        maxTokens: config.maxTokens,
        signal: undefined,
      });
      if (!Array.isArray(result)) {
        throw new Error("Model did not return a JSON array");
      }
      return result;
    }
  );

  if (enriched.length !== slice.length) {
    logger.warn(
      `Chunk ${state.chunkIndex}: expected ${slice.length} items, got ${enriched.length}; merging partial`
    );
  }

  logStepSync(logger, `Chunk ${state.chunkIndex}: merge and write output`, () => {
    const mergeCount = Math.min(slice.length, enriched.length);
    for (let i = 0; i < mergeCount; i++) {
      const merged = { ...slice[i], ...enriched[i] };
      if (config.format === "jsonl") {
        fs.appendFileSync(config.outputPath, JSON.stringify(merged) + "\n", "utf8");
      } else {
        jsonAccumulator.push(merged);
      }
      state.totalOut += 1;
    }
  });

  logger.info(`Chunk ${state.chunkIndex} finished`, { totalOutSoFar: state.totalOut });
}

/**
 * @param {EnrichmentRunConfig} config
 */
export async function runEnrichmentPipeline(config) {
  const runStart = performance.now();
  /** @type {import('winston').Logger | undefined} */
  let logger;

  const state = {
    batch: /** @type {Record<string, string>[]} */ ([]),
    chunkIndex: 0,
    totalRead: 0,
    totalOut: 0,
  };

  const jsonAccumulator = /** @type {Record<string, unknown>[]} */ ([]);

  try {
    const { logger: lg, logFilePath } = createRunLogger();
    logger = lg;

    logger.info("Run started", {
      logFile: logFilePath,
      input: config.inputPath,
      output: config.outputPath,
      format: config.format,
      chunkSize: config.chunkSize,
      delimiter: config.delimiter,
      encoding: config.encoding,
      baseUrl: config.baseUrl,
      model: config.model,
      schemaFile: config.schemaFile,
    });

    logStepSync(logger, "Validate input file exists", () => {
      if (!fs.existsSync(config.inputPath)) {
        throw new Error(`Input file not found: ${config.inputPath}`);
      }
    });

    const schemaText = logStepSync(logger, "Resolve optional schema file", () => {
      if (!config.schemaFile) return "";
      return fs.readFileSync(config.schemaFile, "utf8").trim();
    });

    logStepSync(logger, "Prepare output file", () => {
      fs.mkdirSync(path.dirname(config.outputPath), { recursive: true });
      if (config.format === "jsonl") {
        fs.writeFileSync(config.outputPath, "", "utf8");
      }
    });

    await logStepAsync(logger, "Stream CSV, normalize rows, enrich and write", async () => {
      const parser = fs
        .createReadStream(config.inputPath, { encoding: config.encoding })
        .pipe(
          parse({
            columns: true,
            skip_empty_lines: true,
            delimiter: config.delimiter,
            relax_column_count: true,
            trim: true,
            bom: true,
          })
        );

      for await (const record of parser) {
        state.batch.push(normalizeProductCsvRow(record));
        state.totalRead += 1;
        if (state.batch.length >= config.chunkSize) {
          await flushBatch(logger, config, schemaText, state, jsonAccumulator, false);
        }
      }
      await flushBatch(logger, config, schemaText, state, jsonAccumulator, true);
    });

    if (config.format === "json") {
      logStepSync(logger, "Write final JSON array to disk", () => {
        fs.writeFileSync(config.outputPath, JSON.stringify(jsonAccumulator, null, 2), "utf8");
      });
    }

    const totalRunMs = Number((performance.now() - runStart).toFixed(2));
    logger.info("Run finished successfully", {
      durationMs: totalRunMs,
      csvRowsRead: state.totalRead,
      recordsWritten: state.totalOut,
      outputPath: config.outputPath,
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (logger) {
      logger.error("Run failed", {
        error: msg,
        durationMs: Number((performance.now() - runStart).toFixed(2)),
      });
    }
    throw err;
  }
}
