#!/usr/bin/env node
/**
 * Entry point: parse CLI args and run the enrichment pipeline.
 *
 *   npm install
 *   node cli.mjs -i produtos.csv -o saida.jsonl
 */

import { runEnrichmentPipeline } from "./src/application/enrichPipeline.mjs";
import { parseCli } from "./src/application/parseArgv.mjs";

const config = parseCli();
runEnrichmentPipeline(config).catch((e) => {
  console.error(e);
  process.exit(1);
});
