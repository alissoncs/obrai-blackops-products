/**
 * Infrastructure: Winston — one log file per run under node/logs/.
 */

import fs from "node:fs";
import path from "node:path";
import winston from "winston";

import { PACKAGE_ROOT } from "../../config/packageRoot.mjs";

const lineFormat = winston.format.printf(({ level, message, timestamp, ...meta }) => {
  const rest = { ...meta };
  delete rest.splat;
  const extra = Object.keys(rest).length ? ` ${JSON.stringify(rest)}` : "";
  return `${timestamp} [${level}] ${message}${extra}`;
});

/**
 * @returns {{ logger: import('winston').Logger, logFilePath: string }}
 */
export function createRunLogger() {
  const logsDir = path.join(PACKAGE_ROOT, "logs");
  fs.mkdirSync(logsDir, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const logFilePath = path.join(logsDir, `enrich-csv-${stamp}.log`);

  const logger = winston.createLogger({
    level: "info",
    format: winston.format.combine(winston.format.timestamp({ format: "YYYY-MM-DD HH:mm:ss.SSS" }), lineFormat),
    transports: [
      new winston.transports.File({ filename: logFilePath }),
      new winston.transports.Console({
        format: winston.format.combine(
          winston.format.colorize(),
          winston.format.timestamp({ format: "HH:mm:ss.SSS" }),
          lineFormat
        ),
      }),
    ],
  });

  return { logger, logFilePath };
}

/**
 * @param {import('winston').Logger} logger
 * @param {string} label
 * @param {() => void} fn
 */
export function logStepSync(logger, label, fn) {
  const start = performance.now();
  try {
    const result = fn();
    const durationMs = Number((performance.now() - start).toFixed(2));
    logger.info(`${label} completed`, { durationMs });
    return result;
  } catch (err) {
    const durationMs = Number((performance.now() - start).toFixed(2));
    logger.error(`${label} failed`, {
      durationMs,
      error: err instanceof Error ? err.message : String(err),
    });
    throw err;
  }
}

/**
 * @param {import('winston').Logger} logger
 * @param {string} label
 * @param {() => Promise<unknown>} fn
 */
export async function logStepAsync(logger, label, fn) {
  const start = performance.now();
  try {
    const result = await fn();
    const durationMs = Number((performance.now() - start).toFixed(2));
    logger.info(`${label} completed`, { durationMs });
    return result;
  } catch (err) {
    const durationMs = Number((performance.now() - start).toFixed(2));
    logger.error(`${label} failed`, {
      durationMs,
      error: err instanceof Error ? err.message : String(err),
    });
    throw err;
  }
}
