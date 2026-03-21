/**
 * Root directory of the npm package (the `node/` folder).
 */

import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/** Resolves to .../node/ */
export const PACKAGE_ROOT = path.resolve(__dirname, "..", "..");
